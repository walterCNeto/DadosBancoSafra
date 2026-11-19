"""Marcacao de linhas da silver com as entidades configuradas em
config/entidades.yaml e config/entidades_resolvidas.yaml.

Regra central: a coluna `grupo` ("safra", "pares" ou nulo) e `is_safra` sao
marcadas EXCLUSIVAMENTE por identificadores (CNPJ raiz, CNPJ completo, codigo de
conglomerado). Nome de instituicao nunca marca nada: a busca por nome vive so
no `resolver-entidades`, que gera sugestoes para revisao humana.
"""

from __future__ import annotations

import re
from typing import Any

import polars as pl

from .config import Settings, load_entidades, load_entidades_resolvidas

# status do resolvedor cujos identificadores podem ser usados na marcacao
STATUS_UTILIZAVEIS = {"confirmado", "resolvido"}

_CAMPOS_ID_CONFIG = (
    "cnpj_raiz", "cnpj", "codigo_conglomerado_prudencial", "codigo_conglomerado_financeiro",
)
_CAMPOS_ID_RESOLVIDA = (
    "cnpj", "codigo_conglomerado_prudencial", "codigo_conglomerado_financeiro",
)


def normalizar_identificador(valor: str | None) -> str | None:
    """Normaliza CNPJ/codigo de conglomerado. CNPJ completo (14 digitos, com
    ou sem pontuacao) e reduzido a raiz de 8 digitos."""
    if not valor:
        return None
    limpo = re.sub(r"[^0-9A-Za-z]", "", str(valor)).upper()
    if not limpo:
        return None
    if limpo.isdigit() and len(limpo) == 14:
        return limpo[:8]
    return limpo


def normalizar_nome(valor: str | None) -> str:
    """Maiusculas e sem acentos (uso restrito ao resolvedor)."""
    if not valor:
        return ""
    import unicodedata

    decomposto = unicodedata.normalize("NFKD", valor.upper())
    return "".join(c for c in decomposto if not unicodedata.combining(c))


def _utilizavel(entidade: dict[str, Any]) -> bool:
    """Entrada do arquivo resolvido que pode marcar linhas: status confirmado/
    resolvido, ou aprovada manualmente (`aprovado: true`)."""
    return entidade.get("status") in STATUS_UTILIZAVEIS or bool(entidade.get("aprovado"))


class MapaEntidades:
    """identificador normalizado -> grupo ("safra" tem prioridade em conflito)."""

    def __init__(self, por_identificador: dict[str, str] | None = None) -> None:
        self.por_identificador: dict[str, str] = dict(por_identificador or {})

    def adicionar(self, grupo: str, *valores: str | None) -> None:
        for valor in valores:
            ident = normalizar_identificador(valor)
            if not ident:
                continue
            atual = self.por_identificador.get(ident)
            if atual is None or (grupo == "safra" and atual != "safra"):
                self.por_identificador[ident] = grupo

    @property
    def identificadores(self) -> set[str]:
        return set(self.por_identificador)

    def grupo_de(self, *valores: str | None) -> str | None:
        grupos = {
            self.por_identificador[i]
            for i in (normalizar_identificador(v) for v in valores)
            if i and i in self.por_identificador
        }
        if "safra" in grupos:
            return "safra"
        return next(iter(sorted(grupos)), None)

    def contem(self, *valores: str | None) -> bool:
        return self.grupo_de(*valores) is not None


def mapa_de_dicts(cfg: dict[str, Any], resolvidas: dict[str, Any] | None) -> MapaEntidades:
    """Mapa identificador -> grupo a partir dos dicts de entidades.yaml e do arquivo
    resolvido (so status confirmado/resolvido ou `aprovado: true`; sugestoes nunca
    entram). Sugestoes CVM (`cvm.sugestoes`) entram apenas se `aprovado: true`."""
    mapa = MapaEntidades()
    for grupo, grupo_cfg in (cfg.get("grupos") or {}).items():
        mapa.adicionar(grupo, grupo_cfg.get("cnpj_raiz"))
        for entidade in (grupo_cfg.get("entidades") or {}).values():
            mapa.adicionar(grupo, *(entidade.get(c) for c in _CAMPOS_ID_CONFIG))

    resolvidas = resolvidas or {}
    for grupo, grupo_res in (resolvidas.get("grupos") or {}).items():
        for entidade in (grupo_res.get("entidades") or {}).values():
            if not _utilizavel(entidade):
                continue
            mapa.adicionar(grupo, *(entidade.get(c) for c in _CAMPOS_ID_RESOLVIDA))
            mapa.adicionar(grupo, *(entidade.get("cnpjs_relacionados") or []))

    for grupo, por_entidade in ((resolvidas.get("cvm") or {}).get("sugestoes") or {}).items():
        for itens in (por_entidade or {}).values():
            for item in itens or []:
                if item.get("aprovado"):
                    mapa.adicionar(grupo, item.get("cnpj_raiz"), item.get("cnpj"))

    # sugestoes SUSEP (`susep.sugestoes`): so as aprovadas; marcam por CNPJ (o codigo FIP
    # nao entra no mapa, e resolvido pelo conector via `fips_susep`)
    for grupo, por_entidade in ((resolvidas.get("susep") or {}).get("sugestoes") or {}).items():
        for itens in (por_entidade or {}).values():
            for item in itens or []:
                if item.get("aprovado"):
                    mapa.adicionar(grupo, item.get("cnpj_raiz"), item.get("cnpj"))
    return mapa


def fips_susep(resolvidas: dict[str, Any] | None, somente_aprovados: bool) -> dict[str, str]:
    """codigo FIP (5 digitos) -> grupo, a partir de `susep.sugestoes` do arquivo resolvido.
    Com `somente_aprovados=True` so itens `aprovado: true` (marcam `grupo`); com False, todos
    (candidatos: so entram no universo das tabelas grandes, sem marcar `grupo`)."""
    saida: dict[str, str] = {}
    for grupo, por_entidade in (((resolvidas or {}).get("susep") or {}).get("sugestoes") or {}).items():
        for itens in (por_entidade or {}).values():
            for item in itens or []:
                fip = str(item.get("codigo_fip") or "").strip()
                if not fip or (somente_aprovados and not item.get("aprovado")):
                    continue
                fip = fip.zfill(5)
                if fip not in saida or grupo == "safra":
                    saida[fip] = grupo
    return saida


def carregar_mapa_entidades(settings: Settings) -> MapaEntidades:
    """Constroi o mapa identificador -> grupo a partir do YAML de configuracao
    (identificadores informados a mao) e do arquivo gerado pelo resolvedor
    (apenas entidades com status 'confirmado' ou 'resolvido', ou aprovadas
    manualmente com `aprovado: true`; sugestoes por nome nunca entram)."""
    return mapa_de_dicts(load_entidades(settings), load_entidades_resolvidas(settings))


def codigos_busca_individualizado(settings: Settings, grupo: str = "safra") -> list[str]:
    """Codigos (CNPJ raiz ou codigo de conglomerado) do grupo, usados para montar
    URLs do Cosif individualizado em datas-base anteriores a 2023-03."""
    codigos: set[str] = set()
    cfg = load_entidades(settings)
    grupo_cfg = (cfg.get("grupos") or {}).get(grupo, {})
    if grupo_cfg.get("cnpj_raiz"):
        codigos.add(str(grupo_cfg["cnpj_raiz"]))
    for entidade in (grupo_cfg.get("entidades") or {}).values():
        for campo in ("cnpj_raiz", "codigo_conglomerado_prudencial"):
            if entidade.get(campo):
                codigos.add(str(entidade[campo]))

    resolvidas = load_entidades_resolvidas(settings) or {}
    grupo_res = (resolvidas.get("grupos") or {}).get(grupo, {})
    for entidade in (grupo_res.get("entidades") or {}).values():
        if not _utilizavel(entidade):
            continue
        if entidade.get("cnpj"):
            codigos.add(str(entidade["cnpj"])[:8])
        if entidade.get("codigo_conglomerado_prudencial"):
            codigos.add(str(entidade["codigo_conglomerado_prudencial"]))
    return sorted(codigos)


def marcar_grupos(
    df: pl.DataFrame, mapa: MapaEntidades, colunas_identificador: list[str]
) -> pl.DataFrame:
    """Adiciona `grupo` (String: "safra" | "pares" | nulo) e `is_safra` (Boolean),
    comparando apenas as colunas de identificador normalizadas contra o mapa."""
    existentes = [c for c in colunas_identificador if c in df.columns]
    if not existentes or df.is_empty():
        return df.with_columns(
            pl.lit(None, dtype=pl.String).alias("grupo"),
            pl.lit(False).alias("is_safra"),
        )

    mapeamento = mapa.por_identificador
    por_coluna = []
    for coluna in existentes:
        limpo = pl.col(coluna).cast(pl.String).str.replace_all(r"[^0-9A-Za-z]", "").str.to_uppercase()
        # CNPJ completo (14 digitos) -> raiz de 8
        raiz = pl.when(limpo.str.contains(r"^\d{14}$")).then(limpo.str.slice(0, 8)).otherwise(limpo)
        ident = raiz.replace_strict(mapeamento, default=None, return_dtype=pl.String)
        por_coluna.append(ident)

    grupo = (
        pl.when(pl.any_horizontal(*[e == "safra" for e in por_coluna]))
        .then(pl.lit("safra"))
        .otherwise(pl.coalesce(*por_coluna))
        .alias("grupo")
    )
    df = df.with_columns(grupo)
    return df.with_columns((pl.col("grupo") == "safra").fill_null(False).alias("is_safra"))
