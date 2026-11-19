"""Base e utilitarios dos conectores da CVM (dados.cvm.gov.br, CKAN).

Descoberta: os recursos sao SEMPRE listados por `package_show` da API CKAN
(`{ckan_base_url}/package_show?id=<pacote>`), nunca por URL fixa. Unica excecao
documentada: o CKAN da CVM so lista os ultimos ~12 meses dos arquivos mensais
(informe diario, CDA); os meses anteriores continuam no mesmo diretorio, e o
conector os acha aplicando ao nome de um recurso do CKAN o template
`<prefixo>AAAAMM.zip`, confirmando por HEAD que o arquivo existe.

Arquivos CVM: latin-1 (as vezes utf-8), separador ";", sem aspas confiaveis,
zips mensais/anuais. O silver tem colunas em minusculas, CNPJs so com digitos
(o original fica na bronze), datas como Date e numericos como Float64 (so onde
o layout e conhecido; cast que perderia valores e abandonado e a coluna fica texto).
"""

from __future__ import annotations

import io
import re
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable

import polars as pl

from ..config import Settings
from ..entidades import MapaEntidades, carregar_mapa_entidades
from ..periodos import meses_entre
from .base import HttpConnector, Recurso, decode_bytes, write_silver_parquet

# CNPJ "placeholder" (filial 0000, ex.: 00.000.000/0000-00 de companhias estrangeiras
# no IPE) nunca identifica ninguem; sem isso viraria o Banco do Brasil (raiz 00000000).
_RE_CNPJ_PLACEHOLDER = r"^\d{8}0000\d{2}$"
_RE_COLUNA_CNPJ = re.compile(r"(^|_)cnpj($|_)|^cpf_cnpj")
_RE_COLUNA_DATA = re.compile(r"^(dt_|data_)")
_FORMATOS_DATA = ("%Y-%m-%d", "%d/%m/%Y")


# ---------------------------------------------------------------- leitura / tipos

def ler_csv_cvm(raw: bytes | str) -> pl.DataFrame:
    """CSV da CVM (todas as colunas como texto) -> DataFrame com colunas em minusculas."""
    texto = raw if isinstance(raw, str) else decode_bytes(raw)
    df = pl.read_csv(
        io.StringIO(texto), separator=";", infer_schema_length=0, quote_char=None,
        truncate_ragged_lines=True,
    )
    return df.rename({c: c.strip().lower() for c in df.columns})


def normalizar_cnpjs(df: pl.DataFrame) -> pl.DataFrame:
    """Colunas de CNPJ/CPF (nome contem `cnpj`) ficam so com digitos."""
    exprs = [
        pl.col(c).str.replace_all(r"\D", "").replace("", None).alias(c)
        for c in df.columns if _RE_COLUNA_CNPJ.search(c) and df[c].dtype == pl.String
    ]
    return df.with_columns(exprs) if exprs else df


def _cast_seguro(serie: pl.Series, converter: Callable[[pl.Series], pl.Series]) -> pl.Series | None:
    limpa = serie.str.strip_chars().replace("", None)
    nova = converter(limpa)
    return nova if nova.null_count() <= limpa.null_count() else None


def tipar(df: pl.DataFrame, numericas: Iterable[str] = (), inteiras: Iterable[str] = ()) -> pl.DataFrame:
    """Datas (`dt_*`/`data_*`) -> Date; `numericas` -> Float64; `inteiras` -> Int64.
    Nunca perde valor: se o cast anularia algo que existia, a coluna fica como esta."""
    saida = df
    for coluna in df.columns:
        if df[coluna].dtype != pl.String:
            continue
        novo = None
        if _RE_COLUNA_DATA.search(coluna):
            for fmt in _FORMATOS_DATA:
                novo = _cast_seguro(df[coluna], lambda s, f=fmt: s.str.to_date(f, strict=False))
                if novo is not None:
                    break
        elif coluna in set(numericas):
            novo = _cast_seguro(df[coluna], lambda s: s.cast(pl.Float64, strict=False))
        elif coluna in set(inteiras):
            novo = _cast_seguro(df[coluna], lambda s: s.cast(pl.Int64, strict=False))
        if novo is not None:
            saida = saida.with_columns(novo.alias(coluna))
    return saida


def renomear_legado_fundo(df: pl.DataFrame) -> pl.DataFrame:
    """Layout pre-RCVM 175 (`CNPJ_FUNDO`/`TP_FUNDO`) -> layout atual (`*_classe`)."""
    mapa = {"cnpj_fundo": "cnpj_fundo_classe", "tp_fundo": "tp_fundo_classe"}
    return df.rename({a: b for a, b in mapa.items() if a in df.columns and b not in df.columns})


# ---------------------------------------------------------------- grupos (por identificador)

def expr_grupo(mapa: MapaEntidades, coluna: str) -> pl.Expr:
    """Grupo ("safra"/"pares"/nulo) de uma coluna de CNPJ, so por identificador.
    CNPJ de 14 digitos vira raiz de 8; placeholders (filial 0000) nao casam."""
    limpo = pl.col(coluna).cast(pl.String).str.replace_all(r"[^0-9A-Za-z]", "").str.to_uppercase()
    valido = pl.when(limpo.str.contains(_RE_CNPJ_PLACEHOLDER)).then(None).otherwise(limpo)
    raiz = pl.when(valido.str.contains(r"^\d{14}$")).then(valido.str.slice(0, 8)).otherwise(valido)
    return raiz.replace_strict(mapa.por_identificador, default=None, return_dtype=pl.String)


def combinar_grupos(*exprs: pl.Expr) -> pl.Expr:
    """"safra" tem prioridade sobre os demais, como em MapaEntidades.grupo_de."""
    return (
        pl.when(pl.any_horizontal(*[e == "safra" for e in exprs]))
        .then(pl.lit("safra"))
        .otherwise(pl.coalesce(*exprs))
    )


def marcar_grupos_cvm(df: pl.DataFrame, mapa: MapaEntidades, colunas: list[str]) -> pl.DataFrame:
    """Adiciona `grupo` e `is_safra` olhando so as colunas de identificador."""
    existentes = [c for c in colunas if c in df.columns]
    if not existentes:
        return df.with_columns(pl.lit(None, dtype=pl.String).alias("grupo"), pl.lit(False).alias("is_safra"))
    df = df.with_columns(combinar_grupos(*[expr_grupo(mapa, c) for c in existentes]).alias("grupo"))
    return df.with_columns((pl.col("grupo") == "safra").fill_null(False).alias("is_safra"))


def filtrar_por_mapa(df: pl.DataFrame, mapa: MapaEntidades, colunas: list[str]) -> pl.DataFrame:
    """Mantem so as linhas cujo(s) CNPJ(s) pertencem ao mapa, ja com `grupo`/`is_safra`."""
    marcado = marcar_grupos_cvm(df, mapa, colunas)
    return marcado.filter(pl.col("grupo").is_not_null())


# ---------------------------------------------------------------- conector base

def nome_url(url: str) -> str:
    return url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]


def silver_tabela_path(settings: Settings, fonte: str, tabela: str, data_base: str) -> Path:
    return settings.silver_fonte_dir(fonte) / tabela / f"{data_base}.parquet"


class CvmConnector(HttpConnector):
    """Base dos conectores CVM: descoberta via CKAN e gravacao de silver por tabela
    (`silver/<fonte>/<tabela>/<data_base>.parquet`)."""

    def __init__(self, settings: Settings, client: Any | None = None, hoje: date | None = None) -> None:
        super().__init__(settings, client)
        self.cfg = settings.fonte_config(self.nome)
        self._ckan = self.cfg["ckan_base_url"].rstrip("/")
        self.hoje = hoje or date.today()
        self.falhas_discover: list[str] = []
        self._mapa: MapaEntidades | None = None

    @property
    def mapa(self) -> MapaEntidades:
        if self._mapa is None:
            self._mapa = carregar_mapa_entidades(self.settings)
        return self._mapa

    def mes_coleta(self) -> str:
        return f"{self.hoje.year}{self.hoje.month:02d}"

    def recursos_ckan(self, package_id: str) -> list[dict]:
        """Recursos de um pacote CKAN (`package_show`); levanta RuntimeError em falha."""
        url = f"{self._ckan}/package_show?id={package_id}"
        resposta = self._get_client().get(url)
        if resposta.status_code != 200:
            raise RuntimeError(f"CKAN HTTP {resposta.status_code} em {url}: {resposta.text[:200]}")
        corpo = resposta.json()
        if not corpo.get("success"):
            raise RuntimeError(f"CKAN respondeu success=false em {url}: {corpo.get('error')}")
        return corpo["result"]["resources"]

    def _recursos_ckan_ou_falha(self, package_id: str) -> list[dict] | None:
        try:
            return self.recursos_ckan(package_id)
        except RuntimeError as exc:
            self.falhas_discover.append(f"{package_id}: {exc}")
            return None

    @staticmethod
    def dados(recursos: list[dict], padrao: str | None = None) -> list[dict]:
        """Recursos de dados (diretorio /DADOS/, nao o dicionario /META/), opcionalmente
        filtrados por regex sobre o nome do arquivo."""
        rx = re.compile(padrao) if padrao else None
        return [
            r for r in recursos
            if "/DADOS/" in r.get("url", "") and (rx is None or rx.search(nome_url(r["url"])))
        ]

    def descobrir_snapshot(self, pacotes: list[tuple[str, str, str | None]]) -> list[Recurso]:
        """Fonte sem historico (arquivo atualizado todo dia): data-base = mes da coleta.
        `pacotes` = [(chave, pacote CKAN, regex do nome do arquivo)]; o arquivo baixado
        fica na bronze como `<chave>__<nome original>`."""
        data_base = self.mes_coleta()
        saida: list[Recurso] = []
        for chave, pacote, padrao in pacotes:
            recursos = self._recursos_ckan_ou_falha(pacote)
            if recursos is None:
                continue
            escolhidos = self.dados(recursos, padrao)
            if not escolhidos:
                self.falhas_discover.append(f"{pacote}: nenhum recurso de dados casou com {padrao!r}")
            for r in escolhidos:
                saida.append(Recurso(
                    self.nome, data_base, r["url"], f"{chave}__{nome_url(r['url'])}", extra={"chave": chave},
                ))
        return saida

    def silver_path(self, tabela: str, data_base: str) -> Path:
        return silver_tabela_path(self.settings, self.nome, tabela, data_base)

    def gravar(self, df: pl.DataFrame, tabela: str, data_base: str) -> Path:
        return write_silver_parquet(df, self.silver_path(tabela, data_base))

    def bronze_arquivos(self, data_base: str) -> list[Path]:
        pasta = self.settings.bronze_fonte_dir(self.nome, data_base)
        if not pasta.is_dir():
            return []
        return sorted(p for p in pasta.iterdir() if p.is_file() and p.name != "manifest.jsonl")


class CvmMensalConnector(CvmConnector):
    """Arquivos mensais `<prefixo>AAAAMM.zip` (informe diario, CDA).

    O CKAN lista so os ultimos meses; os anteriores sao deduzidos pelo template do nome
    de um recurso listado e confirmados por HEAD."""

    package_id: str
    padrao_arquivo: str  # regex com 1 grupo: AAAAMM

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        recursos = self._recursos_ckan_ou_falha(self.package_id)
        if recursos is None:
            return []
        rx = re.compile(self.padrao_arquivo)
        por_mes = {rx.search(nome_url(r["url"])).group(1): r["url"] for r in self.dados(recursos) if rx.search(nome_url(r["url"]))}
        if not por_mes:
            self.falhas_discover.append(f"{self.package_id}: nenhum arquivo mensal reconhecido")
            return []
        modelo_mes, modelo_url = next(iter(sorted(por_mes.items())))
        client = self._get_client()
        saida = []
        for anomes in meses_entre(desde, ate, self.hoje, incluir_mes_corrente=True):
            url = por_mes.get(anomes)
            if url is None:
                candidata = modelo_url[: modelo_url.rindex(modelo_mes)] + anomes + modelo_url[modelo_url.rindex(modelo_mes) + len(modelo_mes):]
                url = candidata if client.exists(candidata) else None
            if url:
                saida.append(Recurso(self.nome, anomes, url, nome_url(url)))
        return saida
