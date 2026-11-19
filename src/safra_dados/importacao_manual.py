"""Importacao manual de arquivos para fontes bloqueadas por robots.txt (hoje: `safra_ri`).

O usuario baixa os PDFs/xlsx pelo navegador e os coloca em `data/manual/<fonte>/`. Este modulo
NAO acessa a rede nem o site: calcula o sha256, casa cada arquivo com um item do catalogo
(`bronze/safra_ri/<AAAAMM>/catalogo.jsonl`) pelo nome do arquivo/titulo, copia para a bronze e
registra no manifest com `status = importado_manual`.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import re
import shutil
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from .config import Settings
from .manifest import ManifestEntry, append_entry, now_iso, read_entries

FONTE_RI = "safra_ri"
STATUS_IMPORTADO = "importado_manual"
TIPOS_VALIDOS = (
    "demonstracoes_ifrs", "demonstracoes_brgaap", "demonstracoes_brgaap_resumo", "pilar3_gir",
    "relatorio_anual", "outros",
)
EXTENSOES = {".pdf", ".xlsx", ".xls", ".xlsm", ".csv", ".zip", ".docx"}
LIMIAR_NOME = 0.92


def normalizar_nome(texto: str) -> str:
    """Minusculas, sem acento, sem extensao nem sufixo de download duplicado `(1)`; so letras/digitos."""
    texto = re.sub(r"\.[A-Za-z0-9]{2,5}$", "", texto.strip())
    texto = re.sub(r"\s*\(\d+\)\s*$", "", texto)
    texto = "".join(c for c in unicodedata.normalize("NFKD", texto.lower()) if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", texto).strip()


def sha256_arquivo(caminho: Path) -> str:
    h = hashlib.sha256()
    with caminho.open("rb") as fh:
        for bloco in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest()


def normalizar_data_base(valor: str) -> str:
    m = re.fullmatch(r"(\d{4})-?(0[1-9]|1[0-2])", valor.strip())
    if not m:
        raise ValueError(f"data-base invalida: {valor!r} (use AAAA-MM ou AAAAMM)")
    return f"{m.group(1)}{m.group(2)}"


@dataclass
class ItemCatalogo:
    data_base: str
    url: str
    nome_arquivo: str  # nome no bronze (doc__<tipo>__<original>)
    nome_original: str
    titulo: str
    tipo: str


def carregar_catalogo(settings: Settings, fonte: str = FONTE_RI) -> list[ItemCatalogo]:
    itens: list[ItemCatalogo] = []
    base = settings.bronze_fonte_dir(fonte)
    if not base.is_dir():
        return itens
    for pasta in sorted(p for p in base.iterdir() if p.is_dir()):
        caminho = pasta / "catalogo.jsonl"
        if not caminho.is_file():
            continue
        for linha in caminho.read_text(encoding="utf-8").splitlines():
            if not linha.strip():
                continue
            d = json.loads(linha)
            if d.get("kind", "documento") != "documento":
                continue
            itens.append(ItemCatalogo(
                data_base=pasta.name, url=d["url"], nome_arquivo=d["nome_arquivo"],
                nome_original=d.get("nome_arquivo_original") or d["nome_arquivo"], titulo=d.get("titulo", ""),
                tipo=d.get("tipo", "outros"),
            ))
    return itens


def casar(nome_arquivo: str, catalogo: list[ItemCatalogo]) -> tuple[ItemCatalogo | None, str]:
    """Casa o arquivo com um item do catalogo: (1) nome igual ao original, (2) titulo igual,
    (3) similaridade de nome >= LIMIAR_NOME. Empate entre itens distintos = sem casamento."""
    alvo = normalizar_nome(nome_arquivo)
    if not alvo:
        return None, "nome vazio"
    for criterio, chave in (
        ("nome", lambda i: normalizar_nome(i.nome_original)),
        ("titulo", lambda i: normalizar_nome(i.titulo)),
    ):
        achados = [i for i in catalogo if chave(i) == alvo]
        if len(achados) == 1:
            return achados[0], criterio
        if len(achados) > 1:
            return None, f"ambiguo ({len(achados)} itens com o mesmo {criterio})"
    pontuados = sorted(
        ((difflib.SequenceMatcher(None, alvo, normalizar_nome(i.nome_original)).ratio(), i) for i in catalogo),
        key=lambda t: -t[0],
    )
    if pontuados and pontuados[0][0] >= LIMIAR_NOME:
        if len(pontuados) > 1 and pontuados[1][0] >= pontuados[0][0] - 0.02:
            return None, "ambiguo (nomes parecidos)"
        return pontuados[0][1], f"similaridade {pontuados[0][0]:.2f}"
    return None, "sem correspondencia no catalogo"


@dataclass
class ResultadoImportacao:
    arquivo: str
    estado: str  # importado | ja_importado | pendente
    data_base: str | None = None
    tipo: str | None = None
    destino: str | None = None
    detalhe: str = ""


def _atualizar_catalogo(settings: Settings, data_base: str, url: str, campos: dict, novo: dict | None) -> None:
    pasta = settings.bronze_fonte_dir(FONTE_RI, data_base)
    pasta.mkdir(parents=True, exist_ok=True)
    caminho = pasta / "catalogo.jsonl"
    atuais: dict[str, dict] = {}
    if caminho.is_file():
        for linha in caminho.read_text(encoding="utf-8").splitlines():
            if linha.strip():
                d = json.loads(linha)
                atuais[d["url"]] = d
    base = atuais.get(url) or novo or {"url": url}
    base.update(campos)
    atuais[url] = base
    caminho.write_text("\n".join(json.dumps(v, ensure_ascii=False) for v in atuais.values()) + "\n", encoding="utf-8")


def importar_manual(
    settings: Settings, fonte: str = FONTE_RI, tipo: str | None = None, data_base: str | None = None,
    pasta: Path | None = None,
) -> list[ResultadoImportacao]:
    """Importa os arquivos de `data/manual/<fonte>/`. Sem rede. Arquivos sem casamento no catalogo
    ficam `pendente` a menos que `tipo` e `data_base` sejam informados."""
    if fonte != FONTE_RI:
        raise ValueError(f"importacao manual so disponivel para {FONTE_RI!r}, nao {fonte!r}")
    if tipo is not None and tipo not in TIPOS_VALIDOS:
        raise ValueError(f"tipo invalido: {tipo!r}. Use um de: {', '.join(TIPOS_VALIDOS)}")
    db_opcao = normalizar_data_base(data_base) if data_base else None
    pasta = pasta or settings.manual_dir / fonte
    pasta.mkdir(parents=True, exist_ok=True)
    arquivos = sorted(
        p for p in pasta.iterdir() if p.is_file() and not p.name.startswith(".") and p.suffix.lower() in EXTENSOES
    )
    catalogo = carregar_catalogo(settings, fonte)
    resultados: list[ResultadoImportacao] = []

    for arq in arquivos:
        sha = sha256_arquivo(arq)
        item, motivo = casar(arq.name, catalogo)
        if item is not None:
            db, url, nome_dest, tipo_item, extra = item.data_base, item.url, item.nome_arquivo, item.tipo, None
        elif tipo and db_opcao:
            db, tipo_item = db_opcao, tipo
            url = f"manual://{fonte}/{db}/{sha[:16]}"
            nome_dest = f"doc__{tipo}__{arq.name}"
            extra = {
                "url": url, "nome_arquivo": nome_dest, "kind": "documento", "tipo": tipo, "titulo": arq.stem,
                "formato": arq.suffix.lstrip(".").lower(), "secao": "importacao manual",
                "nome_arquivo_original": arq.name, "data_base_origem": "manual",
                "robots_permitido": False, "baixar": True,
            }
            motivo = "tipo/data-base informados"
        else:
            resultados.append(ResultadoImportacao(
                arq.name, "pendente",
                detalhe=f"{motivo}; informe --tipo ({'|'.join(TIPOS_VALIDOS)}) e --data-base AAAA-MM",
            ))
            continue

        manifest_path = settings.manifest_path(fonte, db)
        ja = [e for e in read_entries(manifest_path) if e.sha256 == sha and e.status == STATUS_IMPORTADO and e.url == url]
        destino = settings.bronze_fonte_dir(fonte, db) / nome_dest
        if ja and destino.is_file():
            resultados.append(ResultadoImportacao(arq.name, "ja_importado", db, tipo_item, str(destino), "mesmo sha256 ja importado"))
            continue
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(arq, destino)
        append_entry(manifest_path, ManifestEntry(
            url=url, data_base=db, coletado_em=now_iso(), status=STATUS_IMPORTADO, sha256=sha,
            bytes=destino.stat().st_size, arquivo=str(destino)))
        _atualizar_catalogo(settings, db, url, {
            "status": STATUS_IMPORTADO, "sha256": sha, "bytes": destino.stat().st_size,
            "motivo": f"importado manualmente de {arq.name} ({motivo})",
        }, extra)
        resultados.append(ResultadoImportacao(arq.name, "importado", db, tipo_item, str(destino), motivo))
    return resultados
