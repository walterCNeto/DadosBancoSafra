"""Conector dos balancetes Cosif do BCB (dataset CKAN "ifs-balancetes").

A lista de recursos do pacote CKAN (package_show) descreve dois mecanismos de
acesso, ambos parametrizados por data-base (URLs sao templates, nunca fixas):

1. Arquivo consolidado por segmento (todas as instituicoes de uma vez):
       {segmento_base_url}/{segmento}/{AAAAMM}{SIGLA}.csv.zip
   Confirmado durante o desenvolvimento que, para os segmentos "Bancos" e
   "Conglomerados-prudenciais", esse arquivo so existe a partir da data-base
   2023-03 (periodos anteriores retornam 404) — provavelmente quando o BCB
   padronizou esse formato de distribuicao em lote.

2. Arquivo individualizado por CNPJ/codigo de conglomerado, documentos 4010
   (balancete patrimonial) e 4040 (consolidado financeiro, descontinuado em
   06/2022):
       {individualizado_base_url}/{AAAAMM}/{DOC}/{AAAAMM}-{DOC}-{CODIGO}.ZIP
   Usado como fallback para datas-base anteriores a 2023, apenas para os
   codigos (CNPJ raiz / codigo de conglomerado) configurados em
   config/entidades.yaml e config/entidades_resolvidas.yaml — sem um cadastro
   confiavel do universo completo de instituicoes (o IfDataCadastro do BCB
   esteve indisponivel durante esta sessao, ver bcb_ifdata.py), nao e possivel
   enumerar todas as instituicoes para baixar o individualizado de cada uma.

Tambem identificado empiricamente: a partir da migracao para conglomerado
prudencial, o documento do arquivo de "Conglomerados-prudenciais" passou a ser
4060 (e nao mais 4040) — o conector nao assume um numero fixo de documento no
arquivo consolidado, apenas registra o que vem na coluna DOCUMENTO de cada
linha.
"""

from __future__ import annotations

import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, codigos_busca_individualizado, marcar_grupos
from ..http import DownloadError, SafraDadosClient, sha256_bytes
from ..manifest import ManifestEntry, append_entry, already_collected, now_iso
from .base import Connector, Recurso, decode_bytes, extract_single_csv, write_silver_parquet

NOME_FONTE = "bcb_cosif"

_QUARTER_END_MONTHS = (3, 6, 9, 12)

# cabecalho oficial dos arquivos Cosif (ver desc. do recurso CKAN e inspecao
# dos arquivos): 3 linhas de metadados + 1 linha de cabecalho prefixada com "#"
_COLUNAS_ORIGEM = [
    "DATA_BASE", "DOCUMENTO", "CNPJ", "AGENCIA", "NOME_INSTITUICAO",
    "COD_CONGL", "NOME_CONGL", "TAXONOMIA", "CONTA", "NOME_CONTA", "SALDO",
]


def publication_date(anomes: str) -> date:
    """Calendario de divulgacao (Comunicado 44.132/2025): jan-mar em 31/05,
    abr-jun em 31/08, jul-set em 30/11, out-dez em 31/03 do ano seguinte."""
    year, month = int(anomes[:4]), int(anomes[4:6])
    if month in (1, 2, 3):
        return date(year, 5, 31)
    if month in (4, 5, 6):
        return date(year, 8, 31)
    if month in (7, 8, 9):
        return date(year, 11, 30)
    return date(year + 1, 3, 31)


def quarters_between(desde: str, ate: str | None, today: date | None = None) -> list[str]:
    today = today or date.today()
    start_year, start_month = (int(p) for p in desde.split("-"))
    if ate:
        end_year, end_month = (int(p) for p in ate.split("-"))
        limite = date(end_year, end_month, 1)
    else:
        limite = today

    anomes_list = []
    year = start_year
    month = next((m for m in _QUARTER_END_MONTHS if m >= start_month), None)
    if month is None:
        year += 1
        month = _QUARTER_END_MONTHS[0]

    while date(year, month, 1) <= limite:
        anomes = f"{year}{month:02d}"
        if ate or publication_date(anomes) <= today:
            anomes_list.append(anomes)
        idx = _QUARTER_END_MONTHS.index(month)
        if idx == len(_QUARTER_END_MONTHS) - 1:
            year += 1
            month = _QUARTER_END_MONTHS[0]
        else:
            month = _QUARTER_END_MONTHS[idx + 1]
    return anomes_list


def latest_available_anomes(today: date | None = None) -> str:
    today = today or date.today()
    anomes_list = quarters_between(f"{today.year - 2}-01", None, today=today)
    if not anomes_list:
        raise RuntimeError("nenhuma data-base Cosif disponivel para a data atual")
    return anomes_list[-1]


def parse_cosif_csv(texto: str, *, segmento: str | None = None) -> pl.DataFrame:
    """Parser puro (sem rede/disco): recebe o CSV Cosif ja decodificado e
    devolve o DataFrame no esquema da silver. As 3 primeiras linhas sao
    metadados; a 4a comeca com '#' e e o cabecalho."""
    linhas = texto.splitlines()
    linhas_dados = [linha for linha in linhas if linha and not linha.startswith("#")]
    # descarta as linhas de metadados (titulo, data de geracao, fonte) que
    # nao tem o separador ';' em quantidade compativel com o cabecalho
    linhas_dados = [linha for linha in linhas_dados if linha.count(";") >= len(_COLUNAS_ORIGEM) - 1]

    registros = []
    for linha in linhas_dados:
        campos = linha.split(";")
        if len(campos) < len(_COLUNAS_ORIGEM):
            continue
        valores = dict(zip(_COLUNAS_ORIGEM, campos))
        saldo_str = valores.get("SALDO", "").strip().replace(".", "").replace(",", ".")
        try:
            saldo = float(saldo_str) if saldo_str else None
        except ValueError:
            saldo = None
        registros.append(
            {
                "data_base": valores.get("DATA_BASE"),
                "cnpj": valores.get("CNPJ"),
                "nome": valores.get("NOME_INSTITUICAO"),
                "atributo": valores.get("TAXONOMIA"),
                "documento": valores.get("DOCUMENTO"),
                "conta": valores.get("CONTA"),
                "nome_conta": valores.get("NOME_CONTA"),
                "saldo": saldo,
                "agencia": valores.get("AGENCIA") or None,
                "cod_conglomerado": valores.get("COD_CONGL") or None,
                "nome_conglomerado": valores.get("NOME_CONGL") or None,
                "segmento": segmento,
            }
        )

    return pl.DataFrame(registros)


@dataclass
class _EstrategiaRecurso:
    pass


class BcbCosifBalancetesConnector(Connector):
    nome = NOME_FONTE

    def __init__(self, settings: Settings, client: SafraDadosClient | None = None) -> None:
        super().__init__(settings)
        self._cfg = settings.fonte_config(NOME_FONTE)
        self._segmento_base_url = self._cfg["segmento_base_url"]
        self._individualizado_base_url = self._cfg["individualizado_base_url"]
        self._segmentos: dict[str, str] = dict(self._cfg.get("segmentos", {}))
        self._documentos_individualizado: list[str] = list(
            self._cfg.get("documentos_individualizado", ["4010", "4040"])
        )
        self._client = client

    def _get_client(self) -> SafraDadosClient:
        if self._client is None:
            self._client = SafraDadosClient(self.settings.http)
        return self._client

    # ---- interface Connector ----

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        client = self._get_client()
        codigos = codigos_busca_individualizado(self.settings)
        recursos: list[Recurso] = []

        for anomes in quarters_between(desde, ate):
            algum_segmento_ok = False
            for segmento, sigla in self._segmentos.items():
                url = f"{self._segmento_base_url}/{segmento}/{anomes}{sigla}.csv.zip"
                if client.exists(url):
                    algum_segmento_ok = True
                    recursos.append(
                        Recurso(
                            fonte=NOME_FONTE,
                            data_base=anomes,
                            url=url,
                            nome_arquivo=f"{segmento}_{anomes}.zip",
                            extra={"estrategia": "segmento", "segmento": segmento},
                        )
                    )

            if not algum_segmento_ok:
                for codigo in codigos:
                    for doc in self._documentos_individualizado:
                        url = (
                            f"{self._individualizado_base_url}/{anomes}/{doc}/"
                            f"{anomes}-{doc}-{codigo}.ZIP"
                        )
                        if client.exists(url):
                            recursos.append(
                                Recurso(
                                    fonte=NOME_FONTE,
                                    data_base=anomes,
                                    url=url,
                                    nome_arquivo=f"individualizado_{doc}_{codigo}_{anomes}.zip",
                                    extra={
                                        "estrategia": "individualizado",
                                        "documento": doc,
                                        "codigo": codigo,
                                    },
                                )
                            )
        return recursos

    def fetch(self, recurso: Recurso) -> None:
        client = self._get_client()
        manifest_path = self.settings.manifest_path(NOME_FONTE, recurso.data_base)
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, recurso.data_base)
        dest = bronze_dir / recurso.nome_arquivo

        resultado = client.download_to_file(recurso.url, dest)
        if resultado.status != "ok":
            append_entry(
                manifest_path,
                ManifestEntry(
                    url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                    status="falha", arquivo=str(dest), erro=resultado.erro,
                ),
            )
            return

        if already_collected(manifest_path, resultado.sha256):
            append_entry(
                manifest_path,
                ManifestEntry(
                    url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                    status="ja_existe", sha256=resultado.sha256, bytes=resultado.bytes_written,
                    arquivo=str(dest),
                ),
            )
            return

        append_entry(
            manifest_path,
            ManifestEntry(
                url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                status="ok", sha256=resultado.sha256, bytes=resultado.bytes_written,
                arquivo=str(dest),
            ),
        )

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None

        frames: list[pl.DataFrame] = []
        for zip_path in sorted(bronze_dir.glob("*.zip")):
            segmento = None
            if zip_path.name.startswith(tuple(self._segmentos)):
                segmento = zip_path.name.split("_")[0]
            try:
                raw_zip = zip_path.read_bytes()
                _nome_csv, raw_csv = extract_single_csv(raw_zip)
            except (zipfile.BadZipFile, ValueError):
                continue
            texto = decode_bytes(raw_csv)
            df = parse_cosif_csv(texto, segmento=segmento)
            if not df.is_empty():
                frames.append(df)

        if not frames:
            return None

        df = pl.concat(frames, how="diagonal_relaxed")
        df = marcar_grupos(df, carregar_mapa_entidades(self.settings), ["cnpj", "cod_conglomerado"])
        out_path = self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet"
        return write_silver_parquet(df, out_path)
