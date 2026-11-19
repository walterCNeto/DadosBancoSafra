"""Conector BCB Olinda IFDATA v1 (IfDataCadastro e IfDataValores).

Descoberto via o metadado OData do servico
(https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/$metadata,
documentado tambem no swagger-ui3):

    IfDataCadastro(AnoMes=@AnoMes)
        -> cadastro das instituicoes/conglomerados em cada data-base
    IfDataValores(AnoMes=@AnoMes, TipoInstituicao=@TipoInstituicao, Relatorio=@Relatorio)
        -> valores de um relatorio especifico
    ListaDeRelatorio()
        -> lista de relatorios disponiveis (numero e nome)

IMPORTANTE: o Olinda responde HTTP 500 se as chaves da query vierem codificadas
(`%40AnoMes`, `%24format`). Todas as URLs sao montadas por
safra_dados.olinda.olinda_url (chaves "@"/"$" literais) e passadas inteiras ao
httpx, sem `params=`.

A coluna de valor do IfDataValores e `Saldo` (Edm.Decimal, ver $metadata) e vai
para `valor` na silver. No tipo de instituicao 1, `CodInst` e o codigo do
conglomerado prudencial (ex.: C0080109), que nao existe como linha do cadastro;
o nome e o CNPJ lider vem do membro lider do conglomerado.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, marcar_grupos
from ..http import DownloadError, SafraDadosClient
from ..olinda import olinda_url
from .base import HttpConnector, Recurso, write_silver_parquet

NOME_FONTE = "bcb_ifdata"

TIPOS_INSTITUICAO = {
    1: "Conglomerados Prudenciais e Instituicoes Independentes",
    2: "Conglomerados Financeiros e Instituicoes Independentes",
    3: "Instituicoes Individuais",
    4: "Instituicoes com Operacoes de Cambio",
}

_QUARTER_MONTHS = (3, 6, 9, 12)


def _quarter_end(year: int, month: int) -> date:
    if month == 3:
        return date(year, 3, 31)
    if month == 6:
        return date(year, 6, 30)
    if month == 9:
        return date(year, 9, 30)
    return date(year, 12, 31)


def publication_date(anomes: str) -> date:
    """Data em que a data-base `anomes` (AAAAMM) e divulgada pelo IFDATA:
    60 dias apos o fechamento de mar/jun/set, 90 dias apos o de dezembro."""
    year, month = int(anomes[:4]), int(anomes[4:6])
    quarter_end = _quarter_end(year, month)
    dias = 90 if month == 12 else 60
    return quarter_end + timedelta(days=dias)


def quarters_between(desde: str, ate: str | None, today: date | None = None) -> list[str]:
    """Gera trimestres "AAAAMM" entre `desde` ("AAAA-MM") e `ate` (ou o ultimo
    trimestre ja publicado, se `ate` for None), restritos aos meses de
    fechamento (03, 06, 09, 12)."""
    today = today or date.today()
    start_year, start_month = (int(p) for p in desde.split("-"))

    if ate:
        end_year, end_month = (int(p) for p in ate.split("-"))
        limite = date(end_year, end_month, 1)
    else:
        limite = today

    anomes_list = []
    year = start_year
    # avanca para o primeiro mes de fechamento >= start_month
    month = next((m for m in _QUARTER_MONTHS if m >= start_month), None)
    if month is None:
        year += 1
        month = _QUARTER_MONTHS[0]

    while date(year, month, 1) <= limite:
        anomes = f"{year}{month:02d}"
        if ate or publication_date(anomes) <= today:
            anomes_list.append(anomes)
        idx = _QUARTER_MONTHS.index(month)
        if idx == len(_QUARTER_MONTHS) - 1:
            year += 1
            month = _QUARTER_MONTHS[0]
        else:
            month = _QUARTER_MONTHS[idx + 1]
    return anomes_list


def latest_available_anomes(today: date | None = None) -> str:
    today = today or date.today()
    anomes_list = quarters_between(f"{today.year - 2}-01", None, today=today)
    if not anomes_list:
        raise RuntimeError("nenhum trimestre do IFDATA disponivel para a data atual")
    return anomes_list[-1]




def url_cadastro(base_url: str, anomes: str | int, **kw) -> str:
    return olinda_url(base_url, "IfDataCadastro", {"AnoMes": int(anomes)}, **kw)


def url_valores(base_url: str, anomes: str | int, tipo_inst: int, relatorio: str | int, **kw) -> str:
    return olinda_url(
        base_url,
        "IfDataValores",
        {"AnoMes": int(anomes), "TipoInstituicao": int(tipo_inst), "Relatorio": str(relatorio)},
        **kw,
    )


def indexar_cadastro(linhas: list[dict]) -> dict[str, dict]:
    """Indice por CodInst (CNPJ raiz) e tambem por codigo de conglomerado
    (prudencial/financeiro), apontando para o membro lider. Chaves de
    conglomerado comecam com 'C' e nao colidem com CNPJ."""
    indice: dict[str, dict] = {row["CodInst"]: row for row in linhas}
    for row in linhas:
        lider = indice.get(row.get("CnpjInstituicaoLider") or "")
        if lider is None:
            continue
        for campo in ("CodConglomeradoPrudencial", "CodConglomeradoFinanceiro"):
            cod = row.get(campo)
            if cod and cod not in indice:
                indice[cod] = lider
    return indice


class BcbIfDataConnector(HttpConnector):
    nome = NOME_FONTE
    paginar_olinda = True

    def __init__(self, settings: Settings, client: SafraDadosClient | None = None) -> None:
        super().__init__(settings, client)
        self._cfg = settings.fonte_config(NOME_FONTE)
        self._base_url = self._cfg["base_url"]
        self._tipos_instituicao: list[int] = list(self._cfg.get("tipos_instituicao", [1]))
        self._relatorios_cache: list[dict] | None = None

    @property
    def base_url(self) -> str:
        return self._base_url

    def lista_relatorios(self) -> list[dict]:
        """GET ListaDeRelatorio() -> [{"NomeRelatorio":..., "NumeroRelatorio":...}, ...]."""
        if self._relatorios_cache is not None:
            return self._relatorios_cache
        response = self._get_client().get(olinda_url(self._base_url, "ListaDeRelatorio()"))
        if response.status_code != 200:
            raise DownloadError(f"ListaDeRelatorio() HTTP {response.status_code}: {response.text[:300]}")
        self._relatorios_cache = response.json()["value"]
        return self._relatorios_cache

    def buscar_cadastro(self, anomes: str) -> list[dict]:
        """Cadastro completo de uma data-base (usado pelo resolvedor)."""
        recurso = Recurso(NOME_FONTE, anomes, url_cadastro(self._base_url, anomes), "cadastro.json")
        return json.loads(self._baixar(recurso))["value"]

    # ---- interface Connector ----

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        recursos: list[Recurso] = []
        relatorios = self._relatorios_cache or self._tentar_listar_relatorios()
        for anomes in quarters_between(desde, ate):
            recursos.append(
                Recurso(
                    fonte=NOME_FONTE, data_base=anomes,
                    url=url_cadastro(self._base_url, anomes),
                    nome_arquivo="cadastro.json", extra={"tipo": "cadastro"},
                )
            )
            for tipo_inst in self._tipos_instituicao:
                for relatorio in relatorios:
                    numero = relatorio["NumeroRelatorio"]
                    recursos.append(
                        Recurso(
                            fonte=NOME_FONTE, data_base=anomes,
                            url=url_valores(self._base_url, anomes, tipo_inst, numero),
                            nome_arquivo=f"valores_tipo{tipo_inst}_rel{numero}.json",
                            extra={"tipo": "valores", "tipo_inst": tipo_inst, "relatorio": numero},
                        )
                    )
        return recursos

    def _tentar_listar_relatorios(self) -> list[dict]:
        try:
            return self.lista_relatorios()
        except DownloadError:
            # sem a lista de relatorios ainda e possivel descobrir o cadastro
            return []

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None

        cadastro_path = bronze_dir / "cadastro.json"
        cadastro: dict[str, dict] = {}
        if cadastro_path.is_file():
            cadastro = indexar_cadastro(json.loads(cadastro_path.read_text(encoding="utf-8")).get("value", []))

        rows: list[dict] = []
        for valores_path in sorted(bronze_dir.glob("valores_*.json")):
            payload = json.loads(valores_path.read_text(encoding="utf-8"))
            for row in payload.get("value", []):
                cad = cadastro.get(row["CodInst"], {})
                rows.append(
                    {
                        "data_base": row.get("AnoMes", data_base),
                        "cod_inst": row["CodInst"],
                        "tipo_inst": row["TipoInstituicao"],
                        "relatorio": row["NumeroRelatorio"],
                        "nome_relatorio": row.get("NomeRelatorio"),
                        "grupo_conta": row.get("Grupo"),
                        "conta": row.get("Conta"),
                        "nome_coluna": row.get("NomeColuna"),
                        "descricao_coluna": row.get("DescricaoColuna"),
                        "valor": row.get("Saldo"),
                        "nome_instituicao": cad.get("NomeInstituicao"),
                        "cod_conglomerado_prudencial": cad.get("CodConglomeradoPrudencial"),
                        "cod_conglomerado_financeiro": cad.get("CodConglomeradoFinanceiro"),
                        "cnpj_instituicao_lider": cad.get("CnpjInstituicaoLider"),
                    }
                )

        if not rows:
            return None

        df = pl.DataFrame(rows, infer_schema_length=None)
        df = marcar_grupos(
            df,
            carregar_mapa_entidades(self.settings),
            ["cod_inst", "cod_conglomerado_prudencial", "cod_conglomerado_financeiro", "cnpj_instituicao_lider"],
        )
        out_path = self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet"
        return write_silver_parquet(df, out_path)
