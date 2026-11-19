"""Conector BCB Olinda Informes_ListaTarifasPorInstituicaoFinanceira v1: tarifas
bancarias por instituicao.

Recursos (ver $metadata):
    ListaInstituicoesDeGrupoConsolidado(CodigoGrupoConsolidado) -> [{Cnpj, Nome}]
    ListaTarifasPorInstituicaoFinanceira(PessoaFisicaOuJuridica, CNPJ) -> tarifas vigentes

O servico so devolve as tarifas VIGENTES (instantaneo, sem historico): a data-base
e o mes da coleta. Coleta-se so para instituicoes dos grupos safra/pares (CNPJs
resolvidos), cruzadas com a lista de instituicoes que publicam tarifas (grupo
consolidado "15" = todas), e para pessoa fisica (F) e juridica (J).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, marcar_grupos, normalizar_identificador
from ..olinda import olinda_url
from ..periodos import meses_entre
from .base import HttpConnector, Recurso, write_silver_parquet

NOME_FONTE = "bcb_tarifas"
_ARQ_INSTITUICOES = "instituicoes.json"


class BcbTarifasConnector(HttpConnector):
    nome = NOME_FONTE
    paginar_olinda = True

    def __init__(self, settings: Settings, client=None, hoje: date | None = None) -> None:
        super().__init__(settings, client)
        cfg = settings.fonte_config(NOME_FONTE)
        self._base_url = cfg["base_url"]
        self._grupo = str(cfg.get("grupo_consolidado", "15"))
        self._hoje = hoje or date.today()

    def url_instituicoes(self) -> str:
        return olinda_url(self._base_url, "ListaInstituicoesDeGrupoConsolidado", {"CodigoGrupoConsolidado": self._grupo})

    def url_tarifas(self, tipo_pessoa: str, cnpj8: str) -> str:
        return olinda_url(
            self._base_url, "ListaTarifasPorInstituicaoFinanceira",
            {"PessoaFisicaOuJuridica": tipo_pessoa, "CNPJ": cnpj8},
        )

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        data_base = f"{self._hoje.year}{self._hoje.month:02d}"
        if data_base not in meses_entre(desde, ate, self._hoje, incluir_mes_corrente=True):
            return []  # so ha instantaneo do mes corrente

        lista = Recurso(NOME_FONTE, data_base, self.url_instituicoes(), _ARQ_INSTITUICOES)
        self.fetch(lista)  # a lista alimenta o discover e fica na bronze
        arquivo = self.settings.bronze_fonte_dir(NOME_FONTE, data_base) / _ARQ_INSTITUICOES
        if not arquivo.is_file():
            return [lista]
        mapa = carregar_mapa_entidades(self.settings)
        cnpjs = sorted(
            {
                normalizar_identificador(i["Cnpj"])
                for i in json.loads(arquivo.read_text(encoding="utf-8"))["value"]
                if mapa.contem(i.get("Cnpj"))
            }
        )
        recursos = [lista]
        for cnpj in cnpjs:
            for tipo in ("F", "J"):
                recursos.append(
                    Recurso(NOME_FONTE, data_base, self.url_tarifas(tipo, cnpj), f"tarifas_{tipo}_{cnpj}.json",
                            extra={"tipo_pessoa": tipo, "cnpj8": cnpj})
                )
        return recursos

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None
        nomes: dict[str, str] = {}
        lista = bronze_dir / _ARQ_INSTITUICOES
        if lista.is_file():
            nomes = {
                normalizar_identificador(i["Cnpj"]): i.get("Nome")
                for i in json.loads(lista.read_text(encoding="utf-8")).get("value", [])
            }
        rows: list[dict] = []
        for path in sorted(bronze_dir.glob("tarifas_*.json")):
            _, tipo, cnpj = path.stem.split("_", 2)
            for r in json.loads(path.read_text(encoding="utf-8")).get("value", []):
                rows.append(
                    {
                        "data_base": data_base,
                        "cnpj8": cnpj,
                        "nome_instituicao": nomes.get(cnpj),
                        "tipo_pessoa": tipo,
                        "codigo_servico": r.get("CodigoServico"),
                        "servico": r.get("Servico"),
                        "unidade": r.get("Unidade"),
                        "data_vigencia": r.get("DataVigencia"),
                        "valor_maximo": r.get("ValorMaximo"),
                        "tipo_valor": r.get("TipoValor"),
                        "periodicidade": r.get("Periodicidade"),
                    }
                )
        if not rows:
            return None
        df = marcar_grupos(
            pl.DataFrame(rows, infer_schema_length=None), carregar_mapa_entidades(self.settings), ["cnpj8"]
        )
        return write_silver_parquet(df, self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet")
