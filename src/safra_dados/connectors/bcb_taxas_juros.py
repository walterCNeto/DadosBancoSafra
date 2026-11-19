"""Conector BCB Olinda taxaJuros v2: taxas de juros por instituicao e modalidade.

Recursos descobertos no $metadata do servico:
    TaxasJurosMensalPorMes            (Mes, Modalidade, Posicao, InstituicaoFinanceira,
                                       TaxaJurosAoMes, TaxaJurosAoAno, cnpj8, anoMes)
    TaxasJurosDiariaPorInicioPeriodo  (InicioPeriodo, FimPeriodo, Segmento, Modalidade, ...)
    ConsultaDatas                     (janelas disponiveis: inicioPeriodo, fimPeriodo, tipoModalidade M|D)

Sao entity sets filtraveis por $filter (sem parametros de funcao). Para a serie
diaria (janelas moveis de 1 semana, uma por dia) coleta-se so a ultima janela de
cada mes. Os dados sao o ranking publicado pelo BCB (apenas as instituicoes que
aparecem no ranking de cada modalidade).
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, marcar_grupos
from ..olinda import olinda_url
from ..periodos import meses_entre
from .base import HttpConnector, Recurso, write_silver_parquet

NOME_FONTE = "bcb_taxas_juros"


def ultima_janela_por_mes(datas: list[dict]) -> dict[str, str]:
    """{AAAAMM: inicioPeriodo} da ultima janela diaria (tipoModalidade D) de cada mes."""
    por_mes: dict[str, str] = {}
    for d in datas:
        if d.get("tipoModalidade") != "D":
            continue
        inicio = d["inicioPeriodo"]  # AAAA-MM-DD
        mes = inicio[:4] + inicio[5:7]
        if inicio > por_mes.get(mes, ""):
            por_mes[mes] = inicio
    return por_mes


class BcbTaxasJurosConnector(HttpConnector):
    nome = NOME_FONTE
    paginar_olinda = True

    def __init__(self, settings: Settings, client=None) -> None:
        super().__init__(settings, client)
        self._base_url = settings.fonte_config(NOME_FONTE)["base_url"]

    def url_mensal(self, anomes: str) -> str:
        return olinda_url(self._base_url, "TaxasJurosMensalPorMes", filtro=f"anoMes eq '{anomes[:4]}-{anomes[4:]}'")

    def url_diaria(self, inicio: str) -> str:
        return olinda_url(self._base_url, "TaxasJurosDiariaPorInicioPeriodo", filtro=f"InicioPeriodo eq '{inicio}'")

    def url_datas(self) -> str:
        return olinda_url(self._base_url, "ConsultaDatas")

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        meses = meses_entre(desde, ate)
        recursos = [
            Recurso(NOME_FONTE, m, self.url_mensal(m), "mensal.json", extra={"periodicidade": "mensal"})
            for m in meses
        ]
        try:
            datas = json.loads(self._baixar(Recurso(NOME_FONTE, "", self.url_datas(), "datas.json")))["value"]
        except Exception:  # sem as janelas ainda da para coletar o mensal
            datas = []
        janelas = ultima_janela_por_mes(datas)
        for m in meses:
            if m in janelas:
                recursos.append(
                    Recurso(NOME_FONTE, m, self.url_diaria(janelas[m]), f"diaria_{janelas[m]}.json",
                            extra={"periodicidade": "diaria", "inicio": janelas[m]})
                )
        return recursos

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None
        rows: list[dict] = []
        for path in sorted(bronze_dir.glob("*.json")):
            periodicidade = "mensal" if path.name.startswith("mensal") else "diaria"
            for r in json.loads(path.read_text(encoding="utf-8")).get("value", []):
                rows.append(
                    {
                        "data_base": data_base,
                        "periodicidade": periodicidade,
                        "inicio_periodo": r.get("InicioPeriodo"),
                        "fim_periodo": r.get("FimPeriodo"),
                        "segmento": r.get("Segmento"),
                        "modalidade": r.get("Modalidade"),
                        "posicao": r.get("Posicao"),
                        "instituicao_financeira": r.get("InstituicaoFinanceira"),
                        "cnpj8": r.get("cnpj8"),
                        "taxa_ao_mes": r.get("TaxaJurosAoMes"),
                        "taxa_ao_ano": r.get("TaxaJurosAoAno"),
                    }
                )
        if not rows:
            return None
        df = marcar_grupos(
            pl.DataFrame(rows, infer_schema_length=None), carregar_mapa_entidades(self.settings), ["cnpj8"]
        )
        return write_silver_parquet(df, self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet")
