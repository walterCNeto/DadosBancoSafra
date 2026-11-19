"""Cadastros da CVM: companhias abertas, intermediarios, administradores de carteira
(pessoas juridicas) e coordenadores de ofertas.

Pacotes CKAN (descobertos por `package_show`): cia_aberta-cad, intermed-cad,
adm_cart-cad e coord_oferta-cad. O pacote `emissores` do CKAN so traz planilhas
agregadas .ods (gov.br), sem CSV de cadastro: o cadastro de emissores de valores
mobiliarios aqui e o de companhias abertas (CNPJ_CIA, SIT_EMISSOR).

Instantaneos (so o estado atual): data-base = mes da coleta. Silver completo (sao
tabelas pequenas) em `silver/cvm_cadastros/<tabela>/<AAAAMM>.parquet`, com `grupo`/
`is_safra` marcados por CNPJ.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ..config import Settings
from .base import Recurso, iter_zip_members
from .cvm_comum import CvmConnector, ler_csv_cvm, marcar_grupos_cvm, normalizar_cnpjs, tipar

NOME_FONTE = "cvm_cadastros"

# chave -> (chave do pacote em settings.yaml, regex do arquivo de dados)
_ARQUIVOS = {
    "cia_aberta": r"^cad_cia_aberta\.csv$",
    "intermed": r"^cad_intermed\.zip$",
    "adm_cart": r"^cad_adm_cart\.zip$",
    "coord_oferta": r"^cad_coord_oferta\.zip$",
}
# chave -> [(tabela silver, membro do zip ou None se for csv direto, colunas de CNPJ para marcar)]
_TABELAS = {
    "cia_aberta": [("cia_aberta", None, ["cnpj_cia"])],
    "intermed": [("intermed", "cad_intermed.csv", ["cnpj"])],
    "adm_cart": [("adm_cart_pj", "cad_adm_cart_pj.csv", ["cnpj"])],
    "coord_oferta": [("coord_oferta", "cad_coord_oferta.csv", ["cnpj"])],
}
_NUMERICAS = ("vl_patrim_liq",)


def parse_cadastro(chave: str, nome_arquivo: str, raw: bytes) -> dict[str, tuple[pl.DataFrame, list[str]]]:
    """Parser puro: bytes do arquivo bronze -> {tabela: (df tipado, colunas de CNPJ)}."""
    saida = {}
    for tabela, membro, colunas in _TABELAS[chave]:
        if membro is None:
            conteudo = raw
        else:
            conteudo = next((b for n, b in iter_zip_members(raw, lambda n, m=membro: n == m)), None)
            if conteudo is None:
                continue
        df = tipar(normalizar_cnpjs(ler_csv_cvm(conteudo)), numericas=_NUMERICAS)
        saida[tabela] = (df, colunas)
    return saida


def linhas_para_busca(tabelas: dict[str, pl.DataFrame]) -> list[dict]:
    """Linhas (cnpj, raiz, nome, situacao, ...) de todos os cadastros, para o resolvedor."""
    linhas = []
    for tabela, df in tabelas.items():
        col_cnpj = "cnpj_cia" if "cnpj_cia" in df.columns else "cnpj"
        for r in df.iter_rows(named=True):
            cnpj = r.get(col_cnpj)
            if not cnpj or len(cnpj) != 14 or cnpj[8:12] == "0000":  # placeholder, nao e CNPJ
                continue
            linhas.append({
                "cadastro": tabela,
                "cnpj": cnpj,
                "cnpj_raiz": cnpj[:8],
                "nome": r.get("denom_social"),
                "nome_comercial": r.get("denom_comerc"),
                "situacao": r.get("sit"),
                "categoria": r.get("tp_partic") or r.get("categ_reg") or r.get("setor_ativ"),
            })
    return linhas


class CvmCadastrosConnector(CvmConnector):
    nome = NOME_FONTE

    def __init__(self, settings: Settings, client=None, hoje=None) -> None:
        super().__init__(settings, client, hoje)
        self._pacotes = self.cfg["pacotes"]

    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        return self.descobrir_snapshot(
            [(chave, self._pacotes[chave], padrao) for chave, padrao in _ARQUIVOS.items() if chave in self._pacotes]
        )

    def baixar_para_busca(self) -> dict[str, pl.DataFrame]:
        """Baixa (sem gravar na bronze) e le os cadastros, para o resolvedor de entidades.
        Levanta RuntimeError se algum cadastro nao puder ser obtido."""
        tabelas: dict[str, pl.DataFrame] = {}
        for recurso in self.discover("", None):
            raw = self._baixar(recurso)
            for tabela, (df, _) in parse_cadastro(recurso.extra["chave"], recurso.nome_arquivo, raw).items():
                tabelas[tabela] = df
        if self.falhas_discover:
            raise RuntimeError("; ".join(self.falhas_discover))
        return tabelas

    def normalize(self, data_base: str) -> Path | None:
        ultimo: Path | None = None
        for arquivo in self.bronze_arquivos(data_base):
            chave = arquivo.name.split("__", 1)[0]
            if chave not in _TABELAS:
                continue
            for tabela, (df, colunas) in parse_cadastro(chave, arquivo.name, arquivo.read_bytes()).items():
                ultimo = self.gravar(marcar_grupos_cvm(df, self.mapa, colunas), tabela, data_base)
        return ultimo
