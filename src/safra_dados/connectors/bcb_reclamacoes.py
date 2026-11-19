"""Conector do ranking de instituicoes por indice de reclamacoes do BCB.

ATENCAO: nao e Olinda. O dataset CKAN "ranking-de-instituicoes-por-indice-de-reclamacoes"
aponta para a API REST do RDR:

    GET {base}                      -> JSON {anos:[{ano, periodicidades:[{periodicidade,
                                       periodos:[{periodo, tipos:[{tipo}]}]}]}]}
    GET {base}/arquivo?ano=&periodicidade=&periodo=&tipo=  -> CSV (cp1252, ';')

Limitacao relevante: as linhas do tipo "Conglomerado" (que e onde aparece o
"SAFRA (conglomerado)") NAO trazem CNPJ no arquivo — so as de "Banco/financeira"
trazem. Como a marcacao grupo/is_safra e so por identificador, os conglomerados
ficam com grupo nulo.

data_base = mes de fechamento do periodo (trimestre T -> 3T; semestre S -> 6S; mes M -> M).
"""

from __future__ import annotations

import io
from pathlib import Path
from urllib.parse import quote_plus

import polars as pl

from ..config import Settings
from ..entidades import carregar_mapa_entidades, marcar_grupos
from ..http import DownloadError
from .base import HttpConnector, Recurso, decode_bytes, write_silver_parquet

NOME_FONTE = "bcb_reclamacoes"

_MESES_POR_PERIODO = {"MENSAL": 1, "TRIMESTRAL": 3, "SEMESTRAL": 6, "ANUAL": 12}

_COLUNAS = {
    "Ano": "ano",
    "Trimestre": "periodo",
    "Semestre": "periodo",
    "Mes": "periodo",
    "Categoria": "categoria",
    "Tipo": "tipo_instituicao",
    "CNPJ IF": "cnpj_if",
    "Instituição financeira": "instituicao",
    "Índice": "indice",
    "Quantidade total de reclamações respondidas": "qtd_reclamacoes_respondidas",
    "Quantidade de reclamações procedentes": "qtd_procedentes",
    "Quantidade de reclamações procedentes extrapoladas": "qtd_procedentes_extrapoladas",
    "Quantidade total de reclamações analisadas": "qtd_analisadas",
    "Quantidade total de clientes – CCS e SCR": "qtd_clientes_total",
    "Quantidade de clientes – CCS": "qtd_clientes_ccs",
    "Quantidade de clientes – SCR": "qtd_clientes_scr",
}


def data_base_do_periodo(ano: int, periodicidade: str, periodo: int) -> str:
    mes = periodo * _MESES_POR_PERIODO.get(periodicidade.upper(), 1)
    return f"{ano}{min(mes, 12):02d}"


def periodos_disponiveis(indice: dict, tipos: list[str]) -> list[dict]:
    """Achata o JSON do ranking em [{ano, periodicidade, periodo, tipo, data_base}]."""
    saida = []
    for a in indice.get("anos", []):
        for p in a.get("periodicidades", []):
            for per in p.get("periodos", []):
                for t in per.get("tipos", []):
                    if t["tipo"] in tipos:
                        saida.append(
                            {
                                "ano": int(a["ano"]), "periodicidade": p["periodicidade"],
                                "periodo": int(per["periodo"]), "tipo": t["tipo"],
                                "data_base": data_base_do_periodo(int(a["ano"]), p["periodicidade"], int(per["periodo"])),
                            }
                        )
    return saida


def parse_ranking_csv(texto: str) -> pl.DataFrame:
    """CSV do ranking (cabecalho na 1a linha, ';', decimais com virgula, ';' final)."""
    df = pl.read_csv(
        io.StringIO(texto), separator=";", infer_schema_length=0, truncate_ragged_lines=True,
        quote_char=None,
    )
    df = df.select([c for c in df.columns if c.strip() in _COLUNAS or c in _COLUNAS])
    df = df.rename({c: _COLUNAS[c.strip()] for c in df.columns})
    num = [c for c in df.columns if c in ("indice",) or c.startswith("qtd_")]
    return df.with_columns(
        pl.col("ano").cast(pl.Int32),
        pl.col("periodo").str.extract(r"(\d+)").cast(pl.Int32),
        pl.col("cnpj_if").str.strip_chars().replace("", None),
        *[
            pl.col(c).str.strip_chars().replace("", None).str.replace_all(r"\.", "").str.replace(",", ".")
            .cast(pl.Float64) if c == "indice"
            else pl.col(c).str.strip_chars().replace("", None).cast(pl.Int64, strict=False)
            for c in num
        ],
    )


class BcbReclamacoesConnector(HttpConnector):
    nome = NOME_FONTE

    def __init__(self, settings: Settings, client=None) -> None:
        super().__init__(settings, client)
        cfg = settings.fonte_config(NOME_FONTE)
        self._base_url = cfg["base_url"].rstrip("/")
        self._tipos: list[str] = list(cfg.get("tipos", ["Bancos e financeiras"]))

    def url_arquivo(self, ano: int, periodicidade: str, periodo: int, tipo: str) -> str:
        return (
            f"{self._base_url}/arquivo?ano={ano}&periodicidade={quote_plus(periodicidade)}"
            f"&periodo={periodo}&tipo={quote_plus(tipo)}"
        )

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        resposta = self._get_client().get(self._base_url)
        if resposta.status_code != 200:
            raise DownloadError(f"ranking HTTP {resposta.status_code}")
        d0 = desde.replace("-", "")
        d1 = ate.replace("-", "") if ate else "999912"
        recursos = []
        for p in periodos_disponiveis(resposta.json(), self._tipos):
            if d0 <= p["data_base"] <= d1:
                recursos.append(
                    Recurso(
                        NOME_FONTE, p["data_base"],
                        self.url_arquivo(p["ano"], p["periodicidade"], p["periodo"], p["tipo"]),
                        f"ranking_{p['ano']}_{p['periodicidade']}_{p['periodo']}.csv", extra=p,
                    )
                )
        return recursos

    def normalize(self, data_base: str) -> Path | None:
        bronze_dir = self.settings.bronze_fonte_dir(NOME_FONTE, data_base)
        if not bronze_dir.is_dir():
            return None
        frames = []
        for path in sorted(bronze_dir.glob("ranking_*.csv")):
            df = parse_ranking_csv(decode_bytes(path.read_bytes(), ("utf-8-sig", "cp1252")))
            if not df.is_empty():
                frames.append(df.with_columns(pl.lit(data_base).alias("data_base")))
        if not frames:
            return None
        df = marcar_grupos(
            pl.concat(frames, how="diagonal_relaxed"), carregar_mapa_entidades(self.settings), ["cnpj_if"]
        )
        return write_silver_parquet(df, self.settings.silver_fonte_dir(NOME_FONTE) / f"{data_base}.parquet")
