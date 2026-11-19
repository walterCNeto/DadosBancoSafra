"""RI do Banco Safra (www.safra.com.br): catalogo e download de demonstracoes contabeis
(BRGAAP e IFRS), Pilar 3/GIR e relatorios anuais. Sem parsing de PDF/xlsx: so arquivo, hash
e metadados (tipo, data-base, URL, titulo).

Descoberta (so HTML, sem JavaScript): parte de `pagina_inicial`, segue os links internos cujo
caminho comeca com `prefixo_paginas` e le, em cada pagina, os blocos
`<div class="doc AAAA" data-month="MM">` com `.titulo` (texto + `span.badge`) e o link do arquivo
(`../../data/files/...`). A secao (acordeao `a.acc-toggle`) ajuda a classificar o tipo.

robots.txt: respeitado ANTES de cada requisicao, com limite de taxa proprio
(`intervalo_min_segundos`). O robots.txt do site tem `Disallow: /data/` e `Disallow:
/external-files/`, e todos os arquivos de RI ficam em `/data/files/...`; portanto os
documentos ficam so no catalogo (`status = bloqueado_robots`, sem sha256) e nada e baixado de
`/data/`. As paginas de listagem (permitidas) vao para a bronze, com hash.

Bronze: `bronze/safra_ri/<data_base>/` com `pagina__*.html` (listagens), `doc__*` (arquivos
liberados pelo robots.txt), `catalogo.jsonl` (metadados de TODOS os documentos vistos) e
`manifest.jsonl`. Silver: `silver/safra_ri/<data_base>.parquet` (um registro por documento).
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit

import polars as pl

from ..config import HttpSettings, Settings
from ..entidades import MapaEntidades, carregar_mapa_entidades, marcar_grupos
from ..manifest import now_iso, read_entries, ultimas_por_url
from ..robots import RegrasRobots, carregar_robots
from .base import HttpConnector, Recurso, decode_bytes, write_silver_parquet

FONTE = "safra_ri"
TIPOS_BAIXAR = {"demonstracoes_brgaap", "demonstracoes_brgaap_resumo", "demonstracoes_ifrs", "pilar3_gir", "relatorio_anual"}

_MESES = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6, "jul": 7, "ago": 8, "set": 9, "out": 10,
    "nov": 11, "dez": 12, "feb": 2, "apr": 4, "may": 5, "aug": 8, "sep": 9, "oct": 10, "dec": 12,
}


@dataclass
class DocumentoRi:
    titulo: str
    url: str
    formato: str  # pdf | excel | ...
    secao: str
    ano_publicacao: int | None
    mes_publicacao: int | None
    pagina_origem: str


# ---------------------------------------------------------------- parsing da listagem

class _ListagemParser(HTMLParser):
    """Extrai documentos e links internos de uma pagina de RI da Safra."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.docs: list[dict[str, Any]] = []
        self.links: list[str] = []
        self.secao = ""
        self._ano: int | None = None
        self._mes: int | None = None
        self._em_toggle = False
        self._titulo: dict[str, Any] | None = None
        self._tag_titulo: str | None = None
        self._parou_texto = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k: (v or "") for k, v in attrs}
        classes = a.get("class", "").split()
        if tag == "a":
            if a.get("href"):
                self.links.append(a["href"])
            if "acc-toggle" in classes:
                self._em_toggle = True
        if tag == "div" and "doc" in classes:
            ano = next((c for c in classes if re.fullmatch(r"\d{4}", c)), None)
            self._ano = int(ano) if ano else None
            self._mes = int(a["data-month"]) if a.get("data-month", "").isdigit() else None
        if "titulo" in classes and tag in ("h3", "p", "h4"):
            self._titulo = {"texto": [], "formato": "", "url": None, "toggle": self._em_toggle}
            self._tag_titulo, self._parou_texto = tag, False
        elif self._titulo is not None:
            if tag == "span" and "badge" in classes:
                self._parou_texto = True
                self._titulo["formato"] = next((c for c in classes if c != "badge"), "")
            elif tag == "a" and a.get("href") and self._titulo["url"] is None:
                self._parou_texto = True
                self._titulo["url"] = a["href"]

    def handle_data(self, data: str) -> None:
        if self._titulo is not None and not self._parou_texto:
            self._titulo["texto"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._em_toggle and self._titulo is None:
            self._em_toggle = False
        if self._titulo is not None and tag == self._tag_titulo:
            t, self._titulo = self._titulo, None
            texto = re.sub(r"\s+", " ", "".join(t["texto"])).strip()
            if t["toggle"]:
                self.secao = texto
                self._em_toggle = False
            elif t["url"] and texto:
                self.docs.append({
                    "titulo": texto, "url": t["url"], "formato": t["formato"], "secao": self.secao,
                    "ano": self._ano, "mes": self._mes,
                })


def parse_listagem(html: str, url_pagina: str) -> tuple[list[DocumentoRi], list[str]]:
    """(documentos, links da pagina) de uma listagem de RI; URLs ja absolutas."""
    p = _ListagemParser()
    p.feed(html)
    docs, vistos = [], set()
    for d in p.docs:
        url = urljoin(url_pagina, d["url"])
        if url in vistos:
            continue
        vistos.add(url)
        docs.append(DocumentoRi(d["titulo"], url, d["formato"], d["secao"], d["ano"], d["mes"], url_pagina))
    return docs, [urljoin(url_pagina, href) for href in p.links]


# ---------------------------------------------------------------- classificacao

def _sem_acento(txt: str) -> str:
    import unicodedata

    return "".join(c for c in unicodedata.normalize("NFKD", txt.lower()) if not unicodedata.combining(c))


def classificar(titulo: str, secao: str, url: str) -> str:
    """Tipo do documento (heuristica por titulo, secao e nome do arquivo)."""
    nome = _sem_acento(unquote(urlsplit(url).path.rsplit("/", 1)[-1]))
    t, s = _sem_acento(titulo), _sem_acento(secao)
    tudo = f"{t} {s} {nome}"
    if "pilar 3" in tudo or "pilar3" in tudo or "gerenciamento de riscos" in tudo or re.search(r"\bgir\b", tudo):
        return "pilar3_gir"
    if re.search(r"relatorio anual|annual report", t):
        return "relatorio_anual"
    if re.search(r"\bifrs\b", t) or re.search(r"\bifrs\b", s) or "ifrs" in nome:
        if re.search(r"demonstra|financial statement|statements|extrato", tudo):
            return "demonstracoes_ifrs"
    if re.search(r"demonstracoes contabeis|demonstracoes financeiras|informacoes contabeis|financial statements", t + " " + s):
        # "Resumo Consolidado e Principais Indicadores" / "Summary of Consolidated Financial Statements"
        # (balanco promocional) nao e o conjunto completo de demonstracoes
        if re.search(r"^resumo|^summary|principais indicadores", t):
            return "demonstracoes_brgaap_resumo"
        return "demonstracoes_brgaap"
    return "outros"


def extrair_data_base(titulo: str, url: str, ano_pub: int | None, mes_pub: int | None) -> tuple[str | None, str]:
    """Data-base de referencia (AAAAMM) a partir do titulo (depois do nome do arquivo); sem pista,
    cai no ano/mes de publicacao. Devolve (AAAAMM, origem)."""
    nome = _sem_acento(unquote(urlsplit(url).path.rsplit("/", 1)[-1]))
    for origem, txt in (("titulo", _sem_acento(titulo)), ("arquivo", nome)):
        m = re.search(r"\b([1-4])t\s?(\d{2}|\d{4})\b", txt)  # 1T23, 4T 2025
        if m:
            ano = int(m.group(2)) + (2000 if len(m.group(2)) == 2 else 0)
            return f"{ano}{int(m.group(1)) * 3:02d}", origem
        m = re.search(r"\b([12])s\s?(\d{2})\b", txt)  # 1S20
        if m:
            return f"{2000 + int(m.group(2))}{int(m.group(1)) * 6:02d}", origem
        m = re.search(r"\b(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|feb|apr|may|aug|sep|oct|dec)[a-z]*[\s._-]*((?:19|20)\d{2}|\d{2})\b", txt)
        if m:
            ano = int(m.group(2)) + (2000 if len(m.group(2)) == 2 else 0)
            return f"{ano}{_MESES[m.group(1)]:02d}", origem
        m = re.search(r"\b(0[1-9]|1[0-2])[\s._/-]+((?:19|20)\d{2})\b", txt)  # 06_2019, 12.2020
        if m:
            return f"{m.group(2)}{m.group(1)}", origem
        m = re.search(r"\b(20\d{2})\b", txt)  # "Relatorio Anual 2025"
        if m and re.search(r"anual|annual|sustentabilidade", txt):
            return f"{m.group(1)}12", origem
    if ano_pub:
        return f"{ano_pub}{(mes_pub or 12):02d}", "publicacao"
    return None, "desconhecida"


# ---------------------------------------------------------------- conector

class SafraRiConnector(HttpConnector):
    nome = FONTE

    def __init__(self, settings: Settings, client: Any | None = None, hoje: date | None = None) -> None:
        super().__init__(settings, client)
        self.cfg = settings.fonte_config(self.nome)
        self.hoje = hoje or date.today()
        self.falhas_discover: list[str] = []
        self.robots: RegrasRobots | None = None
        self._mapa: MapaEntidades | None = None
        self.paginas_descobertas: list[str] = []
        self._html_pendente: dict[str, str] = {}

    @property
    def mapa(self) -> MapaEntidades:
        if self._mapa is None:
            self._mapa = carregar_mapa_entidades(self.settings)
        return self._mapa

    def _get_client(self):
        if self._client is None:
            from dataclasses import replace

            from ..http import SafraDadosClient

            intervalo = float(self.cfg.get("intervalo_min_segundos", 2.0))
            http: HttpSettings = replace(
                self.settings.http, min_interval_seconds=max(self.settings.http.min_interval_seconds, intervalo)
            )
            self._client = SafraDadosClient(http)
        return self._client

    def mes_coleta(self) -> str:
        return f"{self.hoje.year}{self.hoje.month:02d}"

    # ---- descoberta
    def _pagina(self, url: str) -> str | None:
        if not self.robots.permitido(url):
            self.falhas_discover.append(f"robots.txt proibe a pagina {url}")
            return None
        try:
            resposta = self._get_client().get(url)
        except RuntimeError as exc:
            self.falhas_discover.append(f"{url}: {exc}")
            return None
        if resposta.status_code != 200:
            self.falhas_discover.append(f"{url}: HTTP {resposta.status_code}")
            return None
        return decode_bytes(resposta.content, ("utf-8-sig", "cp1252", "latin-1"))

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        base = self.cfg["base_url"].rstrip("/")
        prefixo = self.cfg["prefixo_paginas"]
        inicial = urljoin(base + "/", self.cfg["pagina_inicial"].lstrip("/"))
        self.robots = carregar_robots(self._get_client(), base)
        if self.robots.tudo_proibido:
            self.falhas_discover.append(f"robots.txt indisponivel ({self.robots.origem}); nada foi coletado")
            return []

        desde_aaaamm = desde.replace("-", "")[:6]
        fila, vistas = [inicial], set()
        paginas: list[tuple[str, list[DocumentoRi]]] = []
        while fila and len(vistas) < int(self.cfg.get("max_paginas", 12)):
            url = fila.pop(0)
            if url in vistas:
                continue
            vistas.add(url)
            html = self._pagina(url)
            if html is None:
                continue
            docs, links = parse_listagem(html, url)
            paginas.append((url, docs))
            self.paginas_descobertas.append(url)
            for link in links:
                partes = urlsplit(link)
                if (
                    partes.netloc == urlsplit(base).netloc and partes.path.startswith(prefixo)
                    and partes.path.endswith(".htm") and link.split("#")[0] not in vistas
                ):
                    fila.append(link.split("#")[0])
            self._html_pendente[url] = html

        coleta = self.mes_coleta()
        recursos: list[Recurso] = []
        for url, docs in paginas:
            nome = re.sub(r"[^A-Za-z0-9._-]+", "_", urlsplit(url).path.strip("/").rsplit("/", 1)[-1]) or "index.htm"
            recursos.append(Recurso(self.nome, coleta, url, f"pagina__{nome}", extra={"kind": "pagina", "n_docs": len(docs)}))
        vistos_docs: set[str] = set()
        for url, docs in paginas:
            for d in docs:
                if d.url in vistos_docs:
                    continue
                vistos_docs.add(d.url)
                tipo = classificar(d.titulo, d.secao, d.url)
                data_base, origem = extrair_data_base(d.titulo, d.url, d.ano_publicacao, d.mes_publicacao)
                if data_base is None or data_base < desde_aaaamm or (ate and data_base > ate.replace("-", "")[:6]):
                    continue
                permitido = self.robots.permitido(d.url)
                nome_arq = unquote(urlsplit(d.url).path.rsplit("/", 1)[-1])
                recursos.append(Recurso(
                    self.nome, data_base, d.url, f"doc__{tipo}__{nome_arq}",
                    extra={
                        "kind": "documento", "tipo": tipo, "titulo": d.titulo, "formato": d.formato or Path(nome_arq).suffix.lstrip("."),
                        "secao": d.secao, "pagina_origem": d.pagina_origem, "data_base_origem": origem,
                        "ano_publicacao": d.ano_publicacao, "mes_publicacao": d.mes_publicacao,
                        "nome_arquivo_original": nome_arq, "robots_permitido": permitido,
                        "baixar": tipo in TIPOS_BAIXAR,
                    },
                ))
        return recursos

    # ---- fetch
    def _catalogar(self, recurso: Recurso, **campos: Any) -> None:
        pasta = self.settings.bronze_fonte_dir(self.nome, recurso.data_base)
        pasta.mkdir(parents=True, exist_ok=True)
        caminho = pasta / "catalogo.jsonl"
        atuais = {}
        if caminho.is_file():
            for linha in caminho.read_text(encoding="utf-8").splitlines():
                if linha.strip():
                    item = json.loads(linha)
                    atuais[item["url"]] = item
        atuais[recurso.url] = {"url": recurso.url, "nome_arquivo": recurso.nome_arquivo, **(recurso.extra or {}), **campos}
        caminho.write_text("\n".join(json.dumps(v, ensure_ascii=False) for v in atuais.values()) + "\n", encoding="utf-8")

    def _registrar(self, recurso: Recurso, status: str, erro: str | None = None) -> None:
        from ..manifest import ManifestEntry, append_entry

        dest = self.settings.bronze_fonte_dir(self.nome, recurso.data_base) / recurso.nome_arquivo
        append_entry(
            self.settings.manifest_path(self.nome, recurso.data_base),
            ManifestEntry(url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(), status=status,
                          arquivo=str(dest), erro=erro),
        )

    def fetch(self, recurso: Recurso) -> None:
        extra = recurso.extra or {}
        if extra.get("kind") == "pagina":
            # reaproveita o HTML que a descoberta ja leu e evita uma segunda requisicao
            html = self._html_pendente.get(recurso.url)
            if html is not None:
                self._salvar_pagina(recurso, html.encode("utf-8"))
                return
            return super().fetch(recurso)
        # documento
        if self._importado_manualmente(recurso):
            return  # arquivo colocado pelo usuario em data/manual: nao sobrescrever com o catalogo bloqueado
        if not extra.get("robots_permitido", False):
            self._catalogar(recurso, status="bloqueado_robots", motivo=f"robots.txt ({self.robots.origem if self.robots else '?'}) proibe este caminho")
            self._registrar(recurso, "bloqueado_robots", "robots.txt proibe este caminho")
            return
        if not extra.get("baixar", False):
            self._catalogar(recurso, status="nao_baixado", motivo="tipo fora do escopo (outros)")
            return
        antes = len(read_entries(self.settings.manifest_path(self.nome, recurso.data_base)))
        super().fetch(recurso)
        entradas = read_entries(self.settings.manifest_path(self.nome, recurso.data_base))
        ultima = ultimas_por_url([e for e in entradas[antes:] if e.url == recurso.url])
        e = ultima[0] if ultima else None
        self._catalogar(
            recurso, status=(e.status if e else "falha"), sha256=(e.sha256 if e else ""), bytes=(e.bytes if e else 0),
            motivo=(e.erro if e else "sem entrada no manifest"),
        )

    def _importado_manualmente(self, recurso: Recurso) -> bool:
        entradas = [e for e in read_entries(self.settings.manifest_path(self.nome, recurso.data_base)) if e.url == recurso.url]
        return bool(entradas) and entradas[-1].status == "importado_manual" and Path(entradas[-1].arquivo).is_file()

    def _salvar_pagina(self, recurso: Recurso, conteudo: bytes) -> None:
        from ..http import sha256_bytes
        from ..manifest import ManifestEntry, already_collected, append_entry

        manifest = self.settings.manifest_path(self.nome, recurso.data_base)
        dest = self.settings.bronze_fonte_dir(self.nome, recurso.data_base) / recurso.nome_arquivo
        sha = sha256_bytes(conteudo)
        if already_collected(manifest, sha) and dest.is_file():
            status = "ja_existe"
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(conteudo)
            status = "ok"
        append_entry(manifest, ManifestEntry(url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                                             status=status, sha256=sha, bytes=len(conteudo), arquivo=str(dest)))

    # ---- silver
    def silver_path(self, data_base: str) -> Path:
        return self.settings.silver_fonte_dir(self.nome) / f"{data_base}.parquet"

    def normalize(self, data_base: str) -> Path | None:
        pasta = self.settings.bronze_fonte_dir(self.nome, data_base)
        catalogo = pasta / "catalogo.jsonl"
        if not catalogo.is_file():
            return None
        itens = [json.loads(l) for l in catalogo.read_text(encoding="utf-8").splitlines() if l.strip()]
        if not itens:
            return None
        manifest = {e.url: e for e in ultimas_por_url(read_entries(self.settings.manifest_path(self.nome, data_base)))}
        linhas = []
        for it in itens:
            e = manifest.get(it["url"])
            baixado = it.get("status") in ("ok", "ja_existe", "importado_manual")
            linhas.append({
                "tipo": it["tipo"], "data_base": data_base, "titulo": it["titulo"], "url": it["url"],
                "formato": it.get("formato"), "secao": it.get("secao"), "pagina_origem": it.get("pagina_origem"),
                "nome_arquivo_original": it.get("nome_arquivo_original"),
                "data_base_origem": it.get("data_base_origem"),
                "ano_publicacao": it.get("ano_publicacao"), "mes_publicacao": it.get("mes_publicacao"),
                "status": it.get("status"), "motivo": it.get("motivo"),
                "baixado": baixado,
                "sha256": (e.sha256 if e and baixado else None) or None,
                "bytes": (e.bytes if e and baixado else None),
                "arquivo_local": (e.arquivo if e and baixado else None),
                "coletado_em": e.coletado_em if e else None,
                "cnpj_emissor": self.cfg.get("cnpj_emissor"),
            })
        df = pl.DataFrame(linhas, infer_schema_length=None).sort("tipo", "titulo")
        df = marcar_grupos(df, self.mapa, ["cnpj_emissor"])
        return write_silver_parquet(df, self.silver_path(data_base))
