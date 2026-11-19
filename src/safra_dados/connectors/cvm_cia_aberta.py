"""Documentos de companhias abertas (CVM): DFP, ITR, FRE e IPE.

Pacotes CKAN `cia_aberta-doc-{dfp,itr,fre,ipe}`; um zip por ano, data-base = "AAAA".
O zip do ano e re-publicado quando chegam novos documentos, entao o sha256 do arquivo
decide se ha dado novo.

Silver (`silver/<fonte>/<tabela>/<AAAA>.parquet`), so empresas do mapa (safra/pares),
sempre por CNPJ (placeholder 00.000.000/0000-00 do IPE e ignorado):
- `documentos`: indice de documentos/metadados (DFP/ITR/FRE: um por versao entregue;
  IPE: um por documento, com link). Nao ha tabelas de detalhe do FRE no silver.
- DFP/ITR: `demonstracoes` em formato longo (uma linha por empresa, versao, demonstracao,
  consolidacao, exercicio e conta; BPA, BPP, DRE, DRA, DFC_MD, DFC_MI, DVA e DMPL) e
  `composicao_capital`. `vl_conta_brl` = `vl_conta` na unidade (escala MIL x 1000).
  Parecer do auditor nao e carregado.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from ..config import Settings
from .base import Recurso, iter_zip_members
from .cvm_comum import CvmConnector, filtrar_por_mapa, ler_csv_cvm, nome_url, normalizar_cnpjs, tipar

_RE_DEMONSTRACAO = re.compile(r"_(BPA|BPP|DRE|DRA|DFC_MD|DFC_MI|DVA|DMPL)_(con|ind)_\d{4}\.csv$", re.IGNORECASE)
_COLS_CNPJ = ["cnpj_cia", "cnpj_companhia"]
_NUMERICAS_DEMONSTRACAO = ("vl_conta",)


def parse_demonstracao(nome_membro: str, conteudo: bytes, mapa) -> pl.DataFrame:
    """Um membro `*_<DEMONSTRACAO>_<con|ind>_AAAA.csv` -> linhas longas do grupo, ainda como texto."""
    m = _RE_DEMONSTRACAO.search(nome_membro)
    if not m:
        raise ValueError(f"membro de demonstracao nao reconhecido: {nome_membro}")
    df = filtrar_por_mapa(normalizar_cnpjs(ler_csv_cvm(conteudo)), mapa, _COLS_CNPJ)
    return df.with_columns(
        pl.lit(m.group(1).upper()).alias("demonstracao"),
        pl.lit("consolidado" if m.group(2).lower() == "con" else "individual").alias("consolidacao"),
    )


def finalizar_demonstracoes(frames: list[pl.DataFrame]) -> pl.DataFrame:
    df = tipar(pl.concat(frames, how="diagonal_relaxed"), numericas=_NUMERICAS_DEMONSTRACAO)
    if "vl_conta" in df.columns and df["vl_conta"].dtype == pl.Float64:
        fator = pl.when(pl.col("escala_moeda") == "MIL").then(1000.0).otherwise(1.0)
        df = df.with_columns((pl.col("vl_conta") * fator).alias("vl_conta_brl"))
    return df


class CvmDocCiaAbertaConnector(CvmConnector):
    """DFP, ITR, FRE e IPE (subclasses so definem `nome` e `tipo`)."""

    tipo: str  # "dfp" | "itr" | "fre" | "ipe"
    com_demonstracoes = False

    def __init__(self, settings: Settings, client=None, hoje=None) -> None:
        super().__init__(settings, client, hoje)
        self._rx = re.compile(rf"^{self.tipo}_cia_aberta_(\d{{4}})\.zip$")

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        recursos = self._recursos_ckan_ou_falha(self.cfg["pacote"])
        if recursos is None:
            return []
        ano_ini = int(desde[:4])
        ano_fim = int(ate[:4]) if ate else self.hoje.year
        saida = []
        for r in self.dados(recursos):
            m = self._rx.match(nome_url(r["url"]))
            if m and ano_ini <= int(m.group(1)) <= ano_fim:
                saida.append(Recurso(self.nome, m.group(1), r["url"], nome_url(r["url"])))
        if not saida and not self.falhas_discover:
            self.falhas_discover.append(f"{self.cfg['pacote']}: nenhum zip anual em {ano_ini}-{ano_fim}")
        return sorted(saida, key=lambda r: r.data_base)

    def normalize(self, data_base: str) -> Path | None:
        arquivos = [a for a in self.bronze_arquivos(data_base) if self._rx.match(a.name)]
        if not arquivos:
            return None
        ano = data_base
        indice = f"{self.tipo}_cia_aberta_{ano}.csv"
        capital = f"{self.tipo}_cia_aberta_composicao_capital_{ano}.csv"

        def aceitar(n: str) -> bool:
            return n == indice or (self.com_demonstracoes and (n == capital or bool(_RE_DEMONSTRACAO.search(n))))

        documentos = None
        capital_df = None
        demonstracoes: list[pl.DataFrame] = []
        for nome, conteudo in iter_zip_members(arquivos[0].read_bytes(), aceitar):
            if nome == indice:
                documentos = tipar(filtrar_por_mapa(normalizar_cnpjs(ler_csv_cvm(conteudo)), self.mapa, _COLS_CNPJ),
                                   inteiras=("versao",))
            elif nome == capital:
                capital_df = tipar(filtrar_por_mapa(normalizar_cnpjs(ler_csv_cvm(conteudo)), self.mapa, _COLS_CNPJ),
                                   inteiras=("versao",))
            else:
                demonstracoes.append(parse_demonstracao(nome, conteudo, self.mapa))
        if documentos is None:
            raise ValueError(f"{arquivos[0].name}: indice {indice} ausente no zip")
        ultimo = self.gravar(documentos, "documentos", data_base)
        if self.com_demonstracoes:
            if capital_df is not None:
                self.gravar(capital_df, "composicao_capital", data_base)
            if demonstracoes:
                ultimo = self.gravar(finalizar_demonstracoes(demonstracoes), "demonstracoes", data_base)
        return ultimo


class CvmDfpConnector(CvmDocCiaAbertaConnector):
    nome, tipo, com_demonstracoes = "cvm_dfp", "dfp", True


class CvmItrConnector(CvmDocCiaAbertaConnector):
    nome, tipo, com_demonstracoes = "cvm_itr", "itr", True


class CvmFreConnector(CvmDocCiaAbertaConnector):
    nome, tipo = "cvm_fre", "fre"


class CvmIpeConnector(CvmDocCiaAbertaConnector):
    nome, tipo = "cvm_ipe", "ipe"
