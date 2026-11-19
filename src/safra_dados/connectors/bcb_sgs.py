"""Conector de series SGS de contexto (API api.bcb.gov.br; nao e Olinda).

    GET {base}/bcdata.sgs.{codigo}/dados?formato=json&dataInicial=dd/MM/aaaa&dataFinal=dd/MM/aaaa
    -> [{"data": "dd/MM/aaaa", "valor": "x.y"}]

Series (config): 432 Selic meta, 433 IPCA (var. mensal %), 1 dolar venda, 12 CDI (% a.d.).
Uma requisicao por serie e mes; data_base = AAAAMM (mes corrente incluido, parcial).
A janela mensal evita o limite de 10 anos da API para series diarias.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from ..config import Settings
from ..periodos import meses_entre, ultimo_dia
from .base import HttpConnector, Recurso, write_silver_parquet

NOME_FONTE = "bcb_sgs"


class BcbSgsConnector(HttpConnector):
    nome = NOME_FONTE

    def __init__(self, settings: Settings, client=None, hoje: date | None = None) -> None:
        super().__init__(settings, client)
        cfg = settings.fonte_config(NOME_FONTE)
        self._base_url = cfg["base_url"].rstrip("/")
        self._series: dict[int, str] = {int(k): v for k, v in cfg["series"].items()}
        self._hoje = hoje or date.today()

    def url_serie(self, codigo: int, anomes: str) -> str:
        ini = f"01/{anomes[4:]}/{anomes[:4]}"
        fim = ultimo_dia(anomes, self._hoje).strftime("%d/%m/%Y")
        return f"{self._base_url}/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={ini}&dataFinal={fim}"

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        return [
            Recurso(NOME_FONTE, m, self.url_serie(codigo, m), f"sgs_{codigo}.json", extra={"serie": codigo})
            for m in meses_entre(desde, ate, self._hoje, incluir_mes_corrente=True)
            for codigo in self._series
        ]

    def _baixar(self, recurso: Recurso) -> bytes:
        """HTTP 404 "Value(s) not found" = janela sem valores publicados (ex.: IPCA do
        mes corrente): vale como serie vazia, nao como falha."""
        try:
            return super()._baixar(recurso)
        except RuntimeError as exc:
            if "HTTP 404" in str(exc) and "not found" in str(exc):
                return b"[]"
            raise

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None
        rows = []
        for codigo, nome in self._series.items():
            arquivo = bronze_dir / f"sgs_{codigo}.json"
            if not arquivo.is_file():
                continue
            for r in json.loads(arquivo.read_text(encoding="utf-8")):
                rows.append({"data_base": data_base, "serie": codigo, "nome_serie": nome,
                             "data": r["data"], "valor": r["valor"]})
        if not rows:
            return None
        df = pl.DataFrame(rows).with_columns(
            pl.col("data").str.to_date("%d/%m/%Y"), pl.col("valor").cast(pl.Float64)
        )
        return write_silver_parquet(df, self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet")
