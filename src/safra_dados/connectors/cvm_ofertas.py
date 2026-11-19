"""Ofertas publicas de distribuicao (CVM, pacote `oferta-distrib`).

Um zip com dois CSVs: `oferta_distribuicao.csv` (ICVM 400/476 e anteriores) e
`oferta_resolucao_160.csv` (Resolucao CVM 160). Entram no silver as ofertas em que
uma entidade do mapa (safra/pares) e EMISSORA (`CNPJ_Emissor`) ou COORDENADORA LIDER
(`CNPJ_Lider`), sempre por CNPJ. Ha colunas so com nome (Grupo_Coordenador, Nome_Vendedor,
e emissor sem CNPJ em ofertas antigas) que, pela regra do projeto, nunca marcam nada.

Colunas novas: `grupo_emissor`, `grupo_coordenador`, `grupo` (safra tem prioridade),
`is_safra`. Instantaneo: data-base = mes da coleta.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ..config import Settings
from .base import Recurso, iter_zip_members
from .cvm_comum import (
    CvmConnector, combinar_grupos, expr_grupo, ler_csv_cvm, normalizar_cnpjs, tipar,
)

NOME_FONTE = "cvm_ofertas"

_MEMBROS = {
    "oferta_distribuicao.csv": "ofertas_distribuicao",
    "oferta_resolucao_160.csv": "ofertas_res160",
}
_NUMERICAS = (
    "quantidade_total", "preco_unitario", "valor_total", "qtde_total_registrada", "valor_total_registrado",
)


def filtrar_ofertas(df: pl.DataFrame, mapa) -> pl.DataFrame:
    """Linhas com emissor ou lider no mapa, com colunas de grupo por papel."""
    df = df.with_columns(
        expr_grupo(mapa, "cnpj_emissor").alias("grupo_emissor"),
        expr_grupo(mapa, "cnpj_lider").alias("grupo_coordenador"),
    )
    df = df.filter(pl.col("grupo_emissor").is_not_null() | pl.col("grupo_coordenador").is_not_null())
    df = df.with_columns(
        combinar_grupos(pl.col("grupo_emissor"), pl.col("grupo_coordenador")).alias("grupo")
    )
    return df.with_columns((pl.col("grupo") == "safra").fill_null(False).alias("is_safra"))


class CvmOfertasConnector(CvmConnector):
    nome = NOME_FONTE

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        return self.descobrir_snapshot([("ofertas", self.cfg["pacote"], r"^oferta_distribuicao\.zip$")])

    def normalize(self, data_base: str) -> Path | None:
        ultimo = None
        for arquivo in self.bronze_arquivos(data_base):
            for nome, conteudo in iter_zip_members(arquivo.read_bytes(), lambda n: n in _MEMBROS):
                df = tipar(normalizar_cnpjs(ler_csv_cvm(conteudo)), numericas=_NUMERICAS)
                ultimo = self.gravar(filtrar_ofertas(df, self.mapa), _MEMBROS[nome], data_base)
        return ultimo
