"""SUSEP - SES (Sistema de Estatisticas da SUSEP): base completa.

Descoberta: a unica URL configurada e a da pagina do SES (`pagina_url`). Os links da base
(`BaseCompleta.zip`, via redirecionamento `redarq.asp`) e do cadastro (`LISTAEMPRESAS.csv`) e
o mes de carga ("atualizada ate AAAAMM", usado como data-base) saem do HTML da pagina.
robots.txt do host e consultado (ausente = liberado).

Formato: zip (~580 MB; ~2,9 GB descompactado) com CSVs `;` em cp1252/latin-1 e decimal com
virgula. Os nomes de arquivo variam de caixa (`Ses_seguros.csv`, `SES_Balanco.csv`), por isso
a busca de membros ignora maiusculas. Chaves: `coenti` = codigo FIP (5 digitos, as vezes com
espacos), `damesano` = AAAAMM.

Silver (`silver/susep_ses/<tabela>/<data_base>.parquet`):
- universo completo (pequenas): `cadastro` (cias + LISTAEMPRESAS + ultimo grupo economico),
  `ramos`, `grupos_ramos`, `campos`, `grupos_economicos` (desde `desde`);
- tabelas grandes, so empresas do universo (abaixo) e periodos >= `desde`: `seguros`
  (premios/sinistros por ramo), `provramos` (provisoes tecnicas por ramo), `prov`,
  `balanco` (com o nome do item de `campos`), `pl_margem` (PL e margem de solvencia),
  `valores_mov_ramos`, `uf`.

Universo das tabelas grandes = FIPs de `grupo` safra/pares marcados por identificador (CNPJ do
cadastro no mapa, ou FIP de sugestao SUSEP aprovada) + FIPs *candidatos* (sugestoes ainda
`aprovado: false`). Candidatos ficam com `grupo` nulo e `grupo_sugerido` preenchido, para que
aprovar uma sugestao so exija reprocessar a silver (`normalize`), sem baixar o zip de novo.
"""

from __future__ import annotations

import re
import shutil
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import polars as pl

from ..config import Settings, load_entidades_resolvidas
from ..entidades import MapaEntidades, carregar_mapa_entidades, fips_susep
from ..manifest import ManifestEntry, already_collected, append_entry, now_iso
from ..robots import RegrasRobots, carregar_robots
from .base import HttpConnector, Recurso, decode_bytes, write_silver_parquet
from .cvm_comum import combinar_grupos, expr_grupo, ler_csv_cvm, silver_tabela_path

FONTE = "susep_ses"

# tabela -> (arquivo no zip, colunas-chave que ficam como texto; as demais viram Float64)
TABELAS_GRANDES: dict[str, tuple[str, tuple[str, ...]]] = {
    "seguros": ("ses_seguros.csv", ("damesano", "coenti", "cogrupo", "coramo")),
    "provramos": ("ses_provramos.csv", ("damesano", "coenti", "coramo")),
    "prov": ("ses_prov.csv", ("coenti", "damesano")),
    "balanco": ("ses_balanco.csv", ("coenti", "damesano", "cmpid", "seq", "quadro")),
    "pl_margem": ("ses_pl_margem.csv", ("coenti", "damesano")),
    "valores_mov_ramos": (
        "ses_valoresmovramos.csv", ("coenti", "damesano", "cmpid", "ramcodigo", "gracodigo", "seq", "quadro"),
    ),
    "uf": ("ses_uf2.csv", ("coenti", "damesano", "ramos", "uf", "gracodigo")),
}
ARQ_CIAS = "ses_cias.csv"
ARQ_RAMOS = "ses_ramos.csv"
ARQ_GRUPOS_RAMOS = "ses_gruposramos.csv"
ARQ_CAMPOS = "ses_campos.csv"
ARQ_GRUPOS_ECON = "ses_grupos_economicos.csv"


# ---------------------------------------------------------------- pagina / descoberta

def parse_pagina_ses(html: str, url_pagina: str) -> dict[str, str | None]:
    """Extrai da pagina do SES: URL do zip da base, URL do cadastro e mes de carga (AAAAMM)."""
    zip_url = lista_url = None
    for href, _texto in re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.I | re.S):
        baixo = href.lower()
        if "basecompleta" in baixo and zip_url is None:
            zip_url = urljoin(url_pagina, href)
        elif "listaempresas" in baixo and lista_url is None:
            lista_url = urljoin(url_pagina, href)
    mes = re.search(r'id="[^"]*lblMesAnoCarga"[^>]*>\s*(\d{6})\s*<', html)
    gerada = re.search(r'id="[^"]*lblDataCarga"[^>]*>\s*([^<]+?)\s*<', html)
    return {
        "base_url": zip_url,
        "lista_url": lista_url,
        "mes_carga": mes.group(1) if mes else None,
        "gerada_em": gerada.group(1) if gerada else None,
    }


def ler_csv_ses(raw: bytes) -> pl.DataFrame:
    """CSV pequeno do SES (cp1252/latin-1, `;`) com colunas em minusculas e valores sem espacos
    nas pontas; colunas sem nome (separador sobrando no fim) sao descartadas."""
    df = ler_csv_cvm(decode_bytes(raw))
    df = df.select([c for c in df.columns if c.strip()])
    return df.with_columns(pl.col(c).str.strip_chars() for c in df.columns)


def _fip(coluna: str) -> pl.Expr:
    return pl.col(coluna).cast(pl.String).str.strip_chars().str.zfill(5)


def _numero(coluna: str) -> pl.Expr:
    return pl.col(coluna).str.strip_chars().str.replace(",", ".", literal=True).cast(pl.Float64, strict=False)


def parse_lista_empresas(raw: bytes) -> pl.DataFrame:
    """LISTAEMPRESAS.csv (CodigoFIP;NomeEntidade;CNPJ) -> codigo_fip, nome, cnpj (so digitos)."""
    df = ler_csv_ses(raw)
    return df.select(
        _fip("codigofip").alias("codigo_fip"),
        pl.col("nomeentidade").alias("nome"),
        pl.col("cnpj").str.replace_all(r"\D", "").replace("", None).alias("cnpj"),
    )


# ---------------------------------------------------------------- universo / marcacao

def montar_universo(
    cadastro: pl.DataFrame, mapa: MapaEntidades, fips_aprovados: dict[str, str], fips_sugeridos: dict[str, str]
) -> pl.DataFrame:
    """Uma linha por FIP do universo: `grupo`/`is_safra` (so por identificador: CNPJ no mapa ou
    FIP de sugestao aprovada) e `grupo_sugerido` (qualquer sugestao, aprovada ou nao).
    Entra no universo quem tem `grupo` ou `grupo_sugerido`."""
    aprov = pl.DataFrame(
        {"codigo_fip": list(fips_aprovados), "g_fip": list(fips_aprovados.values())},
        schema={"codigo_fip": pl.String, "g_fip": pl.String},
    )
    sug = pl.DataFrame(
        {"codigo_fip": list(fips_sugeridos), "grupo_sugerido": list(fips_sugeridos.values())},
        schema={"codigo_fip": pl.String, "grupo_sugerido": pl.String},
    )
    base = cadastro.select("codigo_fip", "cnpj").unique("codigo_fip")
    fips_extra = pl.DataFrame({"codigo_fip": sorted(set(fips_aprovados) | set(fips_sugeridos))}, schema={"codigo_fip": pl.String})
    base = pl.concat([base, fips_extra.join(base, on="codigo_fip", how="anti").with_columns(pl.lit(None, dtype=pl.String).alias("cnpj"))])
    base = base.join(aprov, on="codigo_fip", how="left").join(sug, on="codigo_fip", how="left")
    base = base.with_columns(combinar_grupos(expr_grupo(mapa, "cnpj"), pl.col("g_fip")).alias("grupo"))
    base = base.with_columns(
        (pl.col("grupo") == "safra").fill_null(False).alias("is_safra"),
        pl.coalesce("grupo_sugerido", "grupo").alias("grupo_sugerido"),
    )
    return base.filter(pl.col("grupo").is_not_null() | pl.col("grupo_sugerido").is_not_null()).select(
        "codigo_fip", "grupo", "is_safra", "grupo_sugerido"
    )


def montar_cadastro(
    cias: pl.DataFrame | None, lista: pl.DataFrame | None, grupos_econ: pl.DataFrame | None,
    mapa: MapaEntidades, fips_aprovados: dict[str, str],
) -> pl.DataFrame:
    """Cadastro de empresas: FIP, nome, CNPJ, ultimo grupo economico da SUSEP e marcacao."""
    partes = []
    if cias is not None:
        partes.append(cias.select(
            _fip("coenti").alias("codigo_fip"), pl.col("noenti").alias("nome"),
            pl.lit(None, dtype=pl.String).alias("cnpj"), pl.lit(False).alias("na_lista_empresas"),
        ))
    if lista is not None:
        partes.append(lista.select("codigo_fip", "nome", "cnpj", pl.lit(True).alias("na_lista_empresas")))
    if not partes:
        raise ValueError("SES sem Ses_cias.csv e sem LISTAEMPRESAS.csv")
    junto = pl.concat(partes)
    cad = junto.sort("na_lista_empresas", descending=True).group_by("codigo_fip", maintain_order=True).agg(
        pl.col("nome").drop_nulls().first(),
        pl.col("cnpj").drop_nulls().first(),
        pl.col("na_lista_empresas").any(),
    )
    if grupos_econ is not None and not grupos_econ.is_empty():
        ultimo = (
            grupos_econ.with_columns(_fip("coenti").alias("codigo_fip"))
            .sort("damesano").group_by("codigo_fip").last()
            .select("codigo_fip", pl.col("cogrupo").alias("cogrupo_susep"), pl.col("nogrupo").alias("nogrupo_susep"),
                    pl.col("damesano").alias("grupo_susep_ate"))
        )
        cad = cad.join(ultimo, on="codigo_fip", how="left")
    else:
        cad = cad.with_columns(*(pl.lit(None, dtype=pl.String).alias(c) for c in ("cogrupo_susep", "nogrupo_susep", "grupo_susep_ate")))
    cad = cad.with_columns(
        pl.col("cnpj").str.slice(0, 8).alias("cnpj_raiz"),
        pl.col("codigo_fip").replace_strict(fips_aprovados, default=None, return_dtype=pl.String).alias("_g_fip"),
    )
    cad = cad.with_columns(combinar_grupos(expr_grupo(mapa, "cnpj"), pl.col("_g_fip")).alias("grupo")).drop("_g_fip")
    return cad.with_columns((pl.col("grupo") == "safra").fill_null(False).alias("is_safra")).sort("codigo_fip")


# ---------------------------------------------------------------- tabelas grandes

def ler_tabela_grande(
    caminho: Path, chaves: tuple[str, ...], fips: set[str], desde_aaaamm: str
) -> pl.DataFrame:
    """Le um CSV grande do SES em streaming, filtrando por FIP e periodo ANTES de materializar.
    Colunas fora de `chaves` viram Float64 (virgula decimal); se o cast anular algo, a coluna
    fica como texto. So ha dados ASCII (codigos e numeros): `utf8-lossy` e seguro."""
    lf = pl.scan_csv(
        caminho, separator=";", quote_char=None, infer_schema_length=0, encoding="utf8-lossy",
        truncate_ragged_lines=True,
    )
    nomes = [n for n in lf.collect_schema().names() if n.strip()]
    lf = lf.select(nomes).rename({n: n.strip().lower() for n in nomes})
    lf = lf.with_columns(_fip("coenti").alias("coenti"), pl.col("damesano").str.strip_chars().alias("damesano"))
    lf = lf.filter(pl.col("coenti").is_in(sorted(fips)) & (pl.col("damesano") >= desde_aaaamm))
    try:
        df = lf.collect(engine="streaming")
    except TypeError:  # polars antigo
        df = lf.collect(streaming=True)

    texto = set(chaves)
    saida = df
    for c in df.columns:
        if c in texto:
            saida = saida.with_columns(pl.col(c).str.strip_chars())
            continue
        novo = df.select(_numero(c).alias(c))[c]
        original = df[c].str.strip_chars().replace("", None)
        if novo.null_count() <= original.null_count():
            saida = saida.with_columns(novo)
    return saida.with_columns(
        (pl.col("damesano") + "01").str.to_date("%Y%m%d", strict=False).alias("periodo")
    ).rename({"coenti": "codigo_fip"})


class SusepSesConnector(HttpConnector):
    nome = FONTE

    def __init__(self, settings: Settings, client: Any | None = None, hoje: date | None = None) -> None:
        super().__init__(settings, client)
        self.cfg = settings.fonte_config(self.nome)
        self.hoje = hoje or date.today()
        self.falhas_discover: list[str] = []
        self.info_pagina: dict[str, str | None] = {}
        self._desde: str | None = None
        self.resumo: dict[str, int] = {}
        self.avisos: list[str] = []
        self._mapa: MapaEntidades | None = None

    @property
    def mapa(self) -> MapaEntidades:
        if self._mapa is None:
            self._mapa = carregar_mapa_entidades(self.settings)
        return self._mapa

    # ---- descoberta
    def ler_pagina(self) -> dict[str, str | None]:
        url = self.cfg["pagina_url"]
        resposta = self._get_client().get(url)
        if resposta.status_code != 200:
            raise RuntimeError(f"pagina do SES: HTTP {resposta.status_code} em {url}")
        info = parse_pagina_ses(decode_bytes(resposta.content), url)
        faltando = [k for k in ("base_url", "lista_url", "mes_carga") if not info.get(k)]
        if faltando:
            raise RuntimeError(f"pagina do SES mudou: nao encontrei {faltando} em {url}")
        return info

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        self._desde = desde
        try:
            info = self.ler_pagina()
        except RuntimeError as exc:
            self.falhas_discover.append(str(exc))
            return []
        self.info_pagina = info
        robots = carregar_robots(self._get_client(), self.cfg["pagina_url"])
        recursos = []
        for chave, url, nome in (
            ("lista", info["lista_url"], "lista__LISTAEMPRESAS.csv"),
            ("base", info["base_url"], "base__BaseCompleta.zip"),
        ):
            if not robots.permitido(url):
                self.falhas_discover.append(f"robots.txt ({robots.origem}) proibe {url}")
                continue
            recursos.append(Recurso(
                self.nome, info["mes_carga"], url, nome,
                extra={"chave": chave, "gerada_em": info.get("gerada_em")},
            ))
        return recursos

    # ---- fetch: o zip (~580 MB) vai para um .novo e so substitui se o sha256 for novo
    def fetch(self, recurso: Recurso) -> None:
        if (recurso.extra or {}).get("chave") != "base":
            return super().fetch(recurso)
        from ..http import sha256_file

        manifest_path = self.settings.manifest_path(self.nome, recurso.data_base)
        dest = self.settings.bronze_fonte_dir(self.nome, recurso.data_base) / recurso.nome_arquivo
        novo = dest.with_suffix(dest.suffix + ".novo")

        def _registrar(**campos: Any) -> None:
            append_entry(manifest_path, ManifestEntry(
                url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(), arquivo=str(dest), **campos))

        resultado = self._get_client().download_to_file(recurso.url, novo)
        if resultado.status != "ok":
            novo.unlink(missing_ok=True)
            _registrar(status="falha", erro=resultado.erro)
            return
        sha = resultado.sha256 or sha256_file(novo)
        if dest.is_file() and already_collected(manifest_path, sha):
            novo.unlink(missing_ok=True)
            _registrar(status="ja_existe", sha256=sha, bytes=resultado.bytes_written)
            return
        novo.replace(dest)
        _registrar(status="ok", sha256=sha, bytes=resultado.bytes_written)

    # ---- silver
    def _arquivo(self, data_base: str, prefixo: str) -> Path | None:
        pasta = self.settings.bronze_fonte_dir(self.nome, data_base)
        achados = sorted(pasta.glob(f"{prefixo}__*")) if pasta.is_dir() else []
        achados = [a for a in achados if a.suffix != ".novo"]
        return achados[0] if achados else None

    def silver_path(self, tabela: str, data_base: str) -> Path:
        return silver_tabela_path(self.settings, self.nome, tabela, data_base)

    def gravar(self, df: pl.DataFrame, tabela: str, data_base: str) -> Path:
        self.resumo[tabela] = df.height
        return write_silver_parquet(df, self.silver_path(tabela, data_base))

    def normalize(self, data_base: str) -> Path | None:
        zip_path = self._arquivo(data_base, "base")
        if zip_path is None:
            return None
        desde = (self._desde or self.cfg.get("desde", "2020-01")).replace("-", "")[:6]
        lista_path = self._arquivo(data_base, "lista")
        lista = parse_lista_empresas(lista_path.read_bytes()) if lista_path else None
        resolvidas = load_entidades_resolvidas(self.settings)
        aprovados, sugeridos = fips_susep(resolvidas, True), fips_susep(resolvidas, False)
        self.resumo, self.avisos = {}, []

        with zipfile.ZipFile(zip_path) as zf:
            membros = {i.filename.rsplit("/", 1)[-1].lower(): i for i in zf.infolist() if not i.is_dir()}
            for nome in membros:
                if nome.startswith("/") or ".." in Path(nome).parts:
                    raise ValueError(f"entrada de zip suspeita (zip-slip): {nome!r}")

            def pequena(arq: str) -> pl.DataFrame | None:
                info = membros.get(arq)
                if info is None:
                    self.avisos.append(f"membro ausente no zip: {arq}")
                    return None
                return ler_csv_ses(zf.read(info))

            cias, ge = pequena(ARQ_CIAS), pequena(ARQ_GRUPOS_ECON)
            ramos, gramos, campos = pequena(ARQ_RAMOS), pequena(ARQ_GRUPOS_RAMOS), pequena(ARQ_CAMPOS)
            cadastro = montar_cadastro(cias, lista, ge, self.mapa, aprovados)
            universo = montar_universo(cadastro, self.mapa, aprovados, sugeridos)
            self.gravar(cadastro, "cadastro", data_base)
            if ramos is not None:
                self.gravar(ramos.with_columns(pl.col("coramo").str.strip_chars()), "ramos", data_base)
            if gramos is not None:
                self.gravar(gramos, "grupos_ramos", data_base)
            if campos is not None:
                self.gravar(campos, "campos", data_base)
            if ge is not None:
                ge = ge.with_columns(_fip("coenti").alias("coenti")).filter(pl.col("damesano") >= desde)
                self.gravar(
                    ge.join(cadastro.select(pl.col("codigo_fip").alias("coenti"), "grupo", "is_safra"), on="coenti", how="left")
                    .rename({"coenti": "codigo_fip"}),
                    "grupos_economicos", data_base,
                )

            fips = set(universo["codigo_fip"])
            ultimo = None
            tabelas = self.cfg.get("tabelas_grandes") or list(TABELAS_GRANDES)
            with tempfile.TemporaryDirectory(dir=self.settings.data_root, prefix="tmp_ses_") as tmp:
                for tabela in tabelas:
                    arq, chaves = TABELAS_GRANDES[tabela]
                    info = membros.get(arq)
                    if info is None:
                        self.avisos.append(f"membro ausente no zip: {arq}")
                        continue
                    destino = Path(tmp) / f"{tabela}.csv"
                    with zf.open(info) as origem, destino.open("wb") as saida:
                        shutil.copyfileobj(origem, saida, 1024 * 1024)
                    df = ler_tabela_grande(destino, chaves, fips, desde)
                    destino.unlink()
                    df = df.join(universo, on="codigo_fip", how="left")
                    if tabela == "balanco" and campos is not None:
                        itens = campos.select(
                            pl.col("nuitem").alias("cmpid"), pl.col("nuquad").alias("quadro"),
                            pl.col("noitem").alias("noitem"),
                        ).unique(["cmpid", "quadro"])
                        df = df.join(itens, on=["cmpid", "quadro"], how="left")
                    ultimo = self.gravar(df, tabela, data_base)
        return ultimo or self.silver_path("cadastro", data_base)
