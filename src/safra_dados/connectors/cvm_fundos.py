"""Fundos de investimento (CVM): cadastro, informe diario e CDA.

Cadastro (pacote `fi-cad`), dois formatos coexistem:
- pos-Resolucao CVM 175: `registro_fundo_classe.zip` com `registro_fundo.csv`
  (administrador/gestor), `registro_classe.csv` e `registro_subclasse.csv`;
- legado: `cad_fi.csv` (fundos ainda nao adaptados; CNPJ_ADMIN/CPF_CNPJ_GESTOR).
So entram no silver os fundos cujo ADMINISTRADOR ou GESTOR pertence ao mapa de
entidades (grupo `safra` ou `pares`); classes e subclasses seguem o fundo.
Marcacao `grupo`: "safra" se qualquer um dos dois e do grupo Safra.

Informe diario (`fi-doc-inf_diario`) e CDA (`fi-doc-cda`): zips mensais com TODOS os
fundos; o silver guarda so os fundos do universo acima (CNPJ da classe, ou do fundo
no legado), por isso o cadastro precisa ter sido ingerido antes (`cvm.fundos_cad`).
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from ..config import Settings
from ..entidades import MapaEntidades
from .base import Recurso, iter_zip_members
from .cvm_comum import (
    CvmConnector, CvmMensalConnector, ler_csv_cvm, marcar_grupos_cvm, normalizar_cnpjs,
    renomear_legado_fundo, tipar,
)

FONTE_CAD = "cvm_fundos_cad"
FONTE_INFORME = "cvm_fundos_informe"
FONTE_CDA = "cvm_fundos_cda"

_NUMERICAS_CAD = ("patrimonio_liquido", "vl_patrim_liq", "taxa_adm", "taxa_perfm")
_NUMERICAS_INFORME = ("vl_total", "vl_quota", "vl_patrim_liq", "captc_dia", "resg_dia")
_INTEIRAS_INFORME = ("nr_cotst",)
_COLS_ADMIN_GESTOR_REG = ["cnpj_administrador", "cpf_cnpj_gestor"]
_COLS_ADMIN_GESTOR_LEGADO = ["cnpj_admin", "cpf_cnpj_gestor"]


# ---------------------------------------------------------------- cadastro

def filtrar_cadastro(
    fundo: pl.DataFrame, classe: pl.DataFrame, subclasse: pl.DataFrame, mapa: MapaEntidades
) -> dict[str, pl.DataFrame]:
    """Parser/filtro puro do formato pos-RCVM 175 (todas as colunas ja normalizadas)."""
    fundo = marcar_grupos_cvm(fundo, mapa, _COLS_ADMIN_GESTOR_REG).filter(pl.col("grupo").is_not_null())
    do_fundo = fundo.select(
        "id_registro_fundo", "cnpj_fundo", "cnpj_administrador", "administrador",
        "cpf_cnpj_gestor", "gestor", "grupo", "is_safra",
    )
    classe = classe.join(do_fundo, on="id_registro_fundo", how="inner")
    subclasse = subclasse.join(
        classe.select("id_registro_classe", "cnpj_classe", "id_registro_fundo", "cnpj_fundo",
                      "cnpj_administrador", "administrador", "cpf_cnpj_gestor", "gestor", "grupo", "is_safra"),
        on="id_registro_classe", how="inner",
    )
    return {"registro_fundo": fundo, "registro_classe": classe, "registro_subclasse": subclasse}


def parse_cadastro_fundos(zip_registro: bytes | None, csv_legado: bytes | None, mapa: MapaEntidades) -> dict[str, pl.DataFrame]:
    saida: dict[str, pl.DataFrame] = {}
    if zip_registro is not None:
        arquivos = dict(iter_zip_members(
            zip_registro, lambda n: n in ("registro_fundo.csv", "registro_classe.csv", "registro_subclasse.csv")
        ))
        ausentes = {"registro_fundo.csv", "registro_classe.csv", "registro_subclasse.csv"} - set(arquivos)
        if ausentes:
            raise ValueError(f"membros ausentes em registro_fundo_classe.zip: {sorted(ausentes)}")
        lidos = {
            n.removesuffix(".csv"): tipar(normalizar_cnpjs(ler_csv_cvm(b)), numericas=_NUMERICAS_CAD)
            for n, b in arquivos.items()
        }
        saida.update(filtrar_cadastro(lidos["registro_fundo"], lidos["registro_classe"], lidos["registro_subclasse"], mapa))
    if csv_legado is not None:
        legado = tipar(normalizar_cnpjs(ler_csv_cvm(csv_legado)), numericas=_NUMERICAS_CAD)
        saida["cad_fi_legado"] = marcar_grupos_cvm(legado, mapa, _COLS_ADMIN_GESTOR_LEGADO).filter(
            pl.col("grupo").is_not_null()
        )
    return saida


class CvmFundosCadConnector(CvmConnector):
    nome = FONTE_CAD

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        return self.descobrir_snapshot([
            ("registro", self.cfg["pacote"], r"^registro_fundo_classe\.zip$"),
            ("legado", self.cfg["pacote"], r"^cad_fi\.csv$"),
        ])

    def normalize(self, data_base: str) -> Path | None:
        arquivos = {p.name.split("__", 1)[0]: p for p in self.bronze_arquivos(data_base) if "__" in p.name}
        if not arquivos:
            return None
        tabelas = parse_cadastro_fundos(
            arquivos["registro"].read_bytes() if "registro" in arquivos else None,
            arquivos["legado"].read_bytes() if "legado" in arquivos else None,
            self.mapa,
        )
        ultimo = None
        for tabela, df in tabelas.items():
            ultimo = self.gravar(df, tabela, data_base)
        return ultimo


def carregar_universo_fundos(settings: Settings, data_base: str | None = None) -> pl.DataFrame:
    """CNPJs (so digitos) dos fundos/classes do grupo e dos pares, com `grupo`/`is_safra`,
    lidos do silver mais recente de `cvm_fundos_cad`. Vazio se o cadastro nao foi ingerido."""
    base = settings.silver_fonte_dir(FONTE_CAD)
    partes = []
    for tabela, coluna in (("registro_fundo", "cnpj_fundo"), ("registro_classe", "cnpj_classe"), ("cad_fi_legado", "cnpj_fundo")):
        arquivos = sorted((base / tabela).glob("*.parquet")) if (base / tabela).is_dir() else []
        if data_base:
            arquivos = [a for a in arquivos if a.stem == data_base]
        if arquivos:
            partes.append(
                pl.read_parquet(arquivos[-1]).select(
                    pl.col(coluna).alias("cnpj"), "grupo", "is_safra"
                ).drop_nulls("cnpj")
            )
    if not partes:
        return pl.DataFrame(schema={"cnpj": pl.String, "grupo": pl.String, "is_safra": pl.Boolean})
    return (
        pl.concat(partes).sort("is_safra", descending=True).unique(subset="cnpj", keep="first", maintain_order=True)
    )


# ---------------------------------------------------------------- informe / CDA

def _ler_membro(nome_membro: str, conteudo: bytes, universo: pl.DataFrame, coluna_origem: str) -> pl.DataFrame:
    df = renomear_legado_fundo(normalizar_cnpjs(ler_csv_cvm(conteudo)))
    if "cnpj_fundo_classe" not in df.columns:
        raise ValueError(f"membro {nome_membro!r} sem coluna CNPJ_FUNDO_CLASSE/CNPJ_FUNDO: {df.columns[:8]}")
    df = df.filter(pl.col("cnpj_fundo_classe").is_in(universo["cnpj"].implode()))
    return df.with_columns(pl.lit(nome_membro).alias(coluna_origem))


class _MensalFundosConnector(CvmMensalConnector):
    """Base de informe/CDA: filtra pelos fundos do universo e junta `grupo`/`is_safra`."""

    numericas: tuple[str, ...] = ()
    inteiras: tuple[str, ...] = ()
    coluna_origem = "arquivo"  # no CDA a coluna e `bloco` (ex.: fi_BLC_4, fie, fi_PL)

    def __init__(self, settings: Settings, client=None, hoje=None) -> None:
        super().__init__(settings, client, hoje)
        self.package_id = self.cfg["pacote"]
        self._universo: pl.DataFrame | None = None

    @property
    def universo(self) -> pl.DataFrame:
        if self._universo is None:
            self._universo = carregar_universo_fundos(self.settings)
        return self._universo

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        if self.universo.is_empty():
            raise RuntimeError(
                f"{self.nome}: universo de fundos vazio; rode antes `safra-dados ingest --fonte cvm.fundos_cad`"
            )
        return super().discover(desde, ate)

    def aceitar_membro(self, nome: str) -> bool:
        return nome.lower().endswith(".csv")

    def bloco(self, nome_membro: str, data_base: str) -> str:
        return nome_membro

    def normalize(self, data_base: str) -> Path | None:
        arquivos = self.bronze_arquivos(data_base)
        if not arquivos:
            return None
        if self.universo.is_empty():
            raise RuntimeError(f"{self.nome}: universo de fundos vazio (ingira cvm.fundos_cad antes)")
        frames = []
        for arquivo in arquivos:
            for nome, conteudo in iter_zip_members(arquivo.read_bytes(), self.aceitar_membro):
                frames.append(_ler_membro(self.bloco(nome, data_base), conteudo, self.universo, self.coluna_origem))
        if not frames:
            return None
        df = pl.concat(frames, how="diagonal_relaxed")
        df = tipar(df, self.numericas, self.inteiras)
        df = df.join(self.universo, left_on="cnpj_fundo_classe", right_on="cnpj", how="left")
        return self.gravar(df, "fundos", data_base)

    def silver_path(self, tabela: str, data_base: str) -> Path:
        # tabela unica: silver/<fonte>/<AAAAMM>.parquet, como os conectores do Bloco 2
        return self.settings.silver_fonte_dir(self.nome) / f"{data_base}.parquet"


class CvmFundosInformeConnector(_MensalFundosConnector):
    nome = FONTE_INFORME
    padrao_arquivo = r"^inf_diario_fi_(\d{6})\.zip$"
    numericas = _NUMERICAS_INFORME
    inteiras = _INTEIRAS_INFORME


class CvmFundosCdaConnector(_MensalFundosConnector):
    nome = FONTE_CDA
    coluna_origem = "bloco"
    padrao_arquivo = r"^cda_fi_(\d{6})\.zip$"
    numericas = (
        "vl_patrim_liq", "qt_venda_negoc", "vl_venda_negoc", "qt_aquis_negoc", "vl_aquis_negoc",
        "qt_pos_final", "vl_merc_pos_final", "vl_custo_pos_final",
    )

    def bloco(self, nome_membro: str, data_base: str) -> str:
        """`cda_fi_BLC_4_202610.csv` -> `fi_BLC_4`; `cda_fie_202610.csv` -> `fie`."""
        m = re.match(rf"^cda_(.+?)_{data_base}\.csv$", nome_membro, re.IGNORECASE)
        return m.group(1) if m else nome_membro
