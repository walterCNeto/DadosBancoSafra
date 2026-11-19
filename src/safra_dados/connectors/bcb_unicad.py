"""Conector do cadastro de entidades supervisionadas (Unicad) do BCB.

Olinda BcBase v2 (dataset CKAN "dados-cadastrais-de-entidades-autorizadas"):
    EntidadesSupervisionadas(dataBase=@dataBase)   # dataBase no formato 'MM-DD-AAAA'
Observado: 'AAAA-MM-DD' e 'AAAAMM' devolvem HTTP 500; 'MM-DD-AAAA' funciona e o campo
`database` da resposta ecoa a data pedida (retrato do cadastro naquela data).
data_base = AAAAMM; o retrato e o do ultimo dia do mes (limitado a hoje).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, marcar_grupos
from ..olinda import olinda_url
from ..periodos import meses_entre, ultimo_dia
from .base import HttpConnector, Recurso, write_silver_parquet

NOME_FONTE = "bcb_unicad"

_CAMPOS = {
    "database": "data_cadastro",
    "codigoIdentificadorBacen": "cod_bacen",
    "codigoSisbacen": "cod_sisbacen",
    "codigoCNPJ14": "cnpj14",
    "codigoCNPJ8": "cnpj8",
    "nomeEntidadeInteresse": "nome",
    "nomeReduzido": "nome_reduzido",
    "nomeFantasia": "nome_fantasia",
    "siglaDaPessoaJuridica": "sigla",
    "descricaoTipoEntidadeSupervisionada": "tipo_entidade",
    "descricaoTipoSituacaoPessoaJuridica": "situacao",
    "descricaoNaturezaJuridica": "natureza_juridica",
    "nomeDaUnidadeFederativa": "uf",
    "nomeDoMunicipio": "municipio",
    "codigoDoMunicipioNoIBGE": "cod_municipio_ibge",
}


class BcbUnicadConnector(HttpConnector):
    nome = NOME_FONTE
    paginar_olinda = True

    def __init__(self, settings: Settings, client=None, hoje: date | None = None) -> None:
        super().__init__(settings, client)
        self._base_url = settings.fonte_config(NOME_FONTE)["base_url"]
        self._hoje = hoje or date.today()

    def url_entidades(self, anomes: str) -> str:
        data = ultimo_dia(anomes, self._hoje)
        return olinda_url(self._base_url, "EntidadesSupervisionadas", {"dataBase": data.strftime("%m-%d-%Y")})

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        return [
            Recurso(NOME_FONTE, m, self.url_entidades(m), "entidades.json")
            for m in meses_entre(desde, ate, self._hoje)
        ]

    def normalize(self, data_base: str) -> Path | None:
        arquivo = self.settings.bronze_fonte_dir(NOME_FONTE, data_base) / "entidades.json"
        if not arquivo.is_file():
            return None
        linhas = json.loads(arquivo.read_text(encoding="utf-8")).get("value", [])
        if not linhas:
            return None
        df = pl.DataFrame(
            [{novo: r.get(orig) for orig, novo in _CAMPOS.items()} for r in linhas],
            infer_schema_length=None,
        ).with_columns(pl.lit(data_base).alias("data_base"))
        df = marcar_grupos(df, carregar_mapa_entidades(self.settings), ["cnpj8", "cnpj14"])
        return write_silver_parquet(df, self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet")
