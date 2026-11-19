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

Nota: durante o desenvolvimento deste conector, os dois endpoints parametrizados
(IfDataCadastro e IfDataValores) estavam retornando HTTP 500 "Erro desconhecido"
para qualquer AnoMes testado, enquanto ListaDeRelatorio() funcionava normalmente
— indicio de instabilidade do lado do servidor do BCB, e nao de erro de uso da
API (a assinatura foi confirmada contra o $metadata/swagger). O conector foi
implementado conforme a especificacao oficial e tenta novamente (retry/backoff)
via safra_dados.http; se o BCB continuar indisponivel, os recursos ficam
registrados como "falha" no manifest para nova tentativa posterior.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_entidades_safra, marcar_is_safra
from ..http import DownloadError, SafraDadosClient, sha256_bytes
from ..manifest import ManifestEntry, append_entry, already_collected, now_iso
from .base import Connector, Recurso, write_silver_parquet

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


class BcbIfDataConnector(Connector):
    nome = NOME_FONTE

    def __init__(self, settings: Settings, client: SafraDadosClient | None = None) -> None:
        super().__init__(settings)
        self._cfg = settings.fonte_config(NOME_FONTE)
        self._base_url = self._cfg["base_url"]
        self._tipos_instituicao: list[int] = list(self._cfg.get("tipos_instituicao", [1]))
        self._client = client
        self._relatorios_cache: list[dict] | None = None

    @property
    def base_url(self) -> str:
        return self._base_url

    def _get_client(self) -> SafraDadosClient:
        if self._client is None:
            self._client = SafraDadosClient(self.settings.http)
        return self._client

    def lista_relatorios(self) -> list[dict]:
        """GET ListaDeRelatorio() -> [{"NomeRelatorio":..., "NumeroRelatorio":...}, ...]."""
        if self._relatorios_cache is not None:
            return self._relatorios_cache
        client = self._get_client()
        url = f"{self._base_url}/ListaDeRelatorio()"
        response = client.get(url, params={"$format": "json"})
        if response.status_code != 200:
            raise DownloadError(f"ListaDeRelatorio() HTTP {response.status_code}: {response.text[:300]}")
        self._relatorios_cache = response.json()["value"]
        return self._relatorios_cache

    # ---- interface Connector ----

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        recursos: list[Recurso] = []
        relatorios = self._relatorios_cache or self._tentar_listar_relatorios()
        for anomes in quarters_between(desde, ate):
            recursos.append(
                Recurso(
                    fonte=NOME_FONTE,
                    data_base=anomes,
                    url=f"{self._base_url}/IfDataCadastro(AnoMes=@AnoMes)",
                    nome_arquivo="cadastro.json",
                    params={"@AnoMes": int(anomes), "$format": "json"},
                    extra={"tipo": "cadastro"},
                )
            )
            for tipo_inst in self._tipos_instituicao:
                for relatorio in relatorios:
                    numero = relatorio["NumeroRelatorio"]
                    recursos.append(
                        Recurso(
                            fonte=NOME_FONTE,
                            data_base=anomes,
                            url=f"{self._base_url}/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)",
                            nome_arquivo=f"valores_tipo{tipo_inst}_rel{numero}.json",
                            params={
                                "@AnoMes": int(anomes),
                                "@TipoInstituicao": tipo_inst,
                                "@Relatorio": f"'{numero}'",
                                "$format": "json",
                            },
                            extra={"tipo": "valores", "tipo_inst": tipo_inst, "relatorio": numero},
                        )
                    )
        return recursos

    def _tentar_listar_relatorios(self) -> list[dict]:
        try:
            return self.lista_relatorios()
        except DownloadError:
            # sem a lista de relatorios ainda e possivel descobrir o cadastro;
            # os relatorios ficam vazios e o chamador deve tratar separadamente.
            return []

    def fetch(self, recurso: Recurso) -> None:
        client = self._get_client()
        manifest_path = self.settings.manifest_path(NOME_FONTE, recurso.data_base)
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, recurso.data_base)
        dest = bronze_dir / recurso.nome_arquivo

        try:
            response = client.get(recurso.url, params=recurso.params)
        except DownloadError as exc:
            append_entry(
                manifest_path,
                ManifestEntry(
                    url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                    status="falha", arquivo=str(dest), erro=str(exc),
                ),
            )
            return

        if response.status_code != 200:
            append_entry(
                manifest_path,
                ManifestEntry(
                    url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                    status="falha", arquivo=str(dest),
                    erro=f"HTTP {response.status_code}: {response.text[:300]}",
                ),
            )
            return

        content = response.content
        sha256 = sha256_bytes(content)
        if already_collected(manifest_path, sha256) and dest.is_file():
            append_entry(
                manifest_path,
                ManifestEntry(
                    url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                    status="ja_existe", sha256=sha256, bytes=len(content), arquivo=str(dest),
                ),
            )
            return

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        append_entry(
            manifest_path,
            ManifestEntry(
                url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                status="ok", sha256=sha256, bytes=len(content), arquivo=str(dest),
            ),
        )

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        cadastro_path = bronze_dir / "cadastro.json"
        if not bronze_dir.is_dir():
            return None

        cadastro_por_cod: dict[str, dict] = {}
        if cadastro_path.is_file():
            cadastro_json = json.loads(cadastro_path.read_text(encoding="utf-8"))
            for row in cadastro_json.get("value", []):
                cadastro_por_cod[row["CodInst"]] = row

        rows: list[dict] = []
        for valores_path in sorted(bronze_dir.glob("valores_*.json")):
            payload = json.loads(valores_path.read_text(encoding="utf-8"))
            for row in payload.get("value", []):
                cod_inst = row["CodInst"]
                cadastro = cadastro_por_cod.get(cod_inst, {})
                rows.append(
                    {
                        "data_base": row.get("AnoMes", data_base),
                        "cod_inst": cod_inst,
                        "tipo_inst": row["TipoInstituicao"],
                        "relatorio": row["NumeroRelatorio"],
                        "nome_relatorio": row.get("NomeRelatorio"),
                        "grupo": row.get("Grupo"),
                        "conta": row.get("Conta"),
                        "nome_coluna": row.get("NomeColuna"),
                        "valor": row.get("Saldo"),
                        "nome_instituicao": cadastro.get("NomeInstituicao"),
                        "cod_conglomerado_prudencial": cadastro.get("CodConglomeradoPrudencial"),
                        "cod_conglomerado_financeiro": cadastro.get("CodConglomeradoFinanceiro"),
                        "cnpj_instituicao_lider": cadastro.get("CnpjInstituicaoLider"),
                    }
                )

        if not rows:
            return None

        df = pl.DataFrame(rows)
        entidades_safra = carregar_entidades_safra(self.settings)
        df = marcar_is_safra(
            df,
            entidades_safra,
            colunas_identificador=[
                "cod_inst",
                "cod_conglomerado_prudencial",
                "cod_conglomerado_financeiro",
                "cnpj_instituicao_lider",
            ],
            coluna_nome="nome_instituicao",
        )
        out_path = self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet"
        return write_silver_parquet(df, out_path)
