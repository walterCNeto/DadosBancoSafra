"""Conector do Estban (Estatistica Bancaria Mensal por Municipio/Agencia, Documento 4500).

ATENCAO: o Estban NAO esta no CKAN do BCB nem e Olinda. Os arquivos mensais ficam em
    {base}/{nivel}/{AAAAMM}_ESTBAN.csv.zip        (nivel = municipio)
    {base}/{nivel}/{AAAAMM}_ESTBAN_AG.csv.zip     (nivel = agencia)
com base https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban
(padrao conferido no pacote R `estbanr` e por HEAD; ha variantes de nome .ZIP/.csv
que o conector tenta em seguida). CSV latin-1, ';', 2 linhas de metadados e
cabecalho iniciado em '#'; valores inteiros em reais; CNPJ de 8 digitos.

Silver em formato longo: uma linha por (instituicao, municipio[, agencia], verbete).
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, marcar_grupos
from ..periodos import meses_entre
from .base import HttpConnector, Recurso, decode_bytes, extract_single_csv, write_silver_parquet

NOME_FONTE = "bcb_estban"

_ID_COLS = {
    "DATA_BASE": "data_base", "UF": "uf", "CODMUN": "cod_municipio", "MUNICIPIO": "municipio",
    "CNPJ": "cnpj", "NOME_INSTITUICAO": "nome_instituicao", "AGENCIA": "agencia",
    "AGEN_ESPERADAS": "agencias_esperadas", "AGEN_PROCESSADAS": "agencias_processadas",
}


def nomes_arquivo(anomes: str, nivel: str) -> list[str]:
    sufixo = "_ESTBAN_AG" if nivel == "agencia" else "_ESTBAN"
    return [f"{anomes}{sufixo}{ext}" for ext in (".csv.zip", ".ZIP", ".csv")]


def parse_estban(texto: str, nivel: str) -> pl.DataFrame:
    """Parser puro: CSV Estban ja decodificado -> formato longo."""
    linhas = texto.splitlines()
    i = next(n for n, l in enumerate(linhas) if l.startswith("#"))
    cabecalho = linhas[i].lstrip("#")
    corpo = "\n".join([cabecalho, *[l for l in linhas[i + 1:] if l.strip()]])
    df = pl.read_csv(io.StringIO(corpo), separator=";", infer_schema_length=0, quote_char=None, truncate_ragged_lines=True)

    ids = [c for c in df.columns if c in _ID_COLS]
    verbetes = {}
    for c in df.columns:
        m = re.match(r"VERBETE_(\d+)_?(.*)", c)
        if m:
            verbetes[c] = (m.group(1), m.group(2).strip(" _+"))
    longo = (
        df.select(ids + list(verbetes))
        .unpivot(index=ids, on=list(verbetes), variable_name="_col", value_name="saldo")
        .with_columns(
            pl.col("_col").replace_strict({c: v[0] for c, v in verbetes.items()}, return_dtype=pl.String).alias("verbete"),
            pl.col("_col").replace_strict({c: v[1] for c, v in verbetes.items()}, return_dtype=pl.String).alias("nome_verbete"),
            pl.col("saldo").str.strip_chars().replace("", None).cast(pl.Float64, strict=False),
        )
        .drop("_col")
        .rename({c: _ID_COLS[c] for c in ids})
    )
    if "agencia" in longo.columns:
        longo = longo.with_columns(pl.col("agencia").str.replace("^'", ""))
    return longo.with_columns(pl.lit(nivel).alias("nivel"))


class BcbEstbanConnector(HttpConnector):
    nome = NOME_FONTE

    def __init__(self, settings: Settings, client=None) -> None:
        super().__init__(settings, client)
        cfg = settings.fonte_config(NOME_FONTE)
        self._base_url = cfg["base_url"].rstrip("/")
        self._niveis: list[str] = list(cfg.get("niveis", ["municipio", "agencia"]))

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        client = self._get_client()
        recursos = []
        for anomes in meses_entre(desde, ate):
            for nivel in self._niveis:
                for nome in nomes_arquivo(anomes, nivel):
                    url = f"{self._base_url}/{nivel}/{nome}"
                    if client.exists(url):
                        recursos.append(
                            Recurso(NOME_FONTE, anomes, url, f"{nivel}_{nome}", extra={"nivel": nivel})
                        )
                        break
        return recursos

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None
        frames = []
        for path in sorted(bronze_dir.iterdir()):
            nivel = path.name.split("_", 1)[0]
            if nivel not in ("municipio", "agencia"):
                continue
            raw = path.read_bytes()
            try:  # alguns meses vem como CSV puro com nome .zip
                _, raw = extract_single_csv(raw)
            except (zipfile.BadZipFile, ValueError):
                pass
            frames.append(parse_estban(decode_bytes(raw, ("cp1252", "latin-1")), nivel))
        if not frames:
            return None
        df = marcar_grupos(
            pl.concat(frames, how="diagonal_relaxed"), carregar_mapa_entidades(self.settings), ["cnpj"]
        )
        return write_silver_parquet(df, self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet")
