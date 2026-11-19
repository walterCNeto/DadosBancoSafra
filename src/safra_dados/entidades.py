"""Utilitarios para casar linhas da silver com as entidades configuradas em
config/entidades.yaml e config/entidades_resolvidas.yaml (usado para marcar a
coluna is_safra nos conectores, e pelo resolvedor de entidades)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import polars as pl

from .config import Settings, load_entidades, load_entidades_resolvidas


def normalizar_identificador(valor: str | None) -> str | None:
    if not valor:
        return None
    return re.sub(r"[^0-9A-Za-z]", "", str(valor)).upper() or None


def normalizar_nome(valor: str | None) -> str:
    if not valor:
        return ""
    sem_acento = (
        valor.upper()
        .replace("Á", "A").replace("À", "A").replace("Â", "A").replace("Ã", "A")
        .replace("É", "E").replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O").replace("Ô", "O").replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )
    return sem_acento


@dataclass
class EntidadesSafra:
    """Conjunto normalizado de identificadores (CNPJ raiz e codigos de
    conglomerado) conhecidos para o grupo Safra, usado para marcar is_safra."""

    identificadores: set[str]
    termos_nome: list[str]

    def contem(self, *valores: str | None) -> bool:
        for valor in valores:
            ident = normalizar_identificador(valor)
            if ident and ident in self.identificadores:
                return True
        return False

    def nome_corresponde(self, nome: str | None) -> bool:
        nome_norm = normalizar_nome(nome)
        return any(termo in nome_norm for termo in self.termos_nome)


def carregar_entidades_safra(settings: Settings, grupo: str = "safra") -> EntidadesSafra:
    identificadores: set[str] = set()
    termos_nome: list[str] = []

    entidades_cfg = load_entidades(settings)
    grupo_cfg = entidades_cfg.get("grupos", {}).get(grupo, {})
    cnpj_raiz_grupo = grupo_cfg.get("cnpj_raiz")
    if cnpj_raiz_grupo:
        ident = normalizar_identificador(cnpj_raiz_grupo)
        if ident:
            identificadores.add(ident)

    for entidade in grupo_cfg.get("entidades", {}).values():
        for campo in ("cnpj_raiz", "codigo_conglomerado_prudencial_hint", "codigo_conglomerado_financeiro_hint"):
            ident = normalizar_identificador(entidade.get(campo))
            if ident:
                identificadores.add(ident)
        for termo in entidade.get("busca_nome", []):
            termos_nome.append(normalizar_nome(termo))

    resolvidas = load_entidades_resolvidas(settings)
    if resolvidas:
        grupo_resolvido = resolvidas.get("grupos", {}).get(grupo, {})
        for entidade in grupo_resolvido.get("entidades", {}).values():
            for campo in ("cnpj", "codigo_conglomerado_prudencial", "codigo_conglomerado_financeiro"):
                ident = normalizar_identificador(entidade.get(campo))
                if ident:
                    identificadores.add(ident)
            for cnpj in entidade.get("cnpjs_relacionados", []) or []:
                ident = normalizar_identificador(cnpj)
                if ident:
                    identificadores.add(ident)

    return EntidadesSafra(identificadores=identificadores, termos_nome=termos_nome)


def codigos_busca_individualizado(settings: Settings, grupo: str = "safra") -> list[str]:
    """Codigos (CNPJ raiz de 8 digitos ou codigo de conglomerado) conhecidos
    para o grupo, usados para montar URLs do endpoint Cosif individualizado
    quando o arquivo consolidado por segmento nao estiver disponivel (datas
    anteriores a 2023, segundo observado durante o desenvolvimento)."""
    codigos: set[str] = set()

    entidades_cfg = load_entidades(settings)
    grupo_cfg = entidades_cfg.get("grupos", {}).get(grupo, {})
    if grupo_cfg.get("cnpj_raiz"):
        codigos.add(str(grupo_cfg["cnpj_raiz"]))
    for entidade in grupo_cfg.get("entidades", {}).values():
        if entidade.get("cnpj_raiz"):
            codigos.add(str(entidade["cnpj_raiz"]))
        hint = entidade.get("codigo_conglomerado_prudencial_hint")
        if hint:
            codigos.add(str(hint))

    resolvidas = load_entidades_resolvidas(settings)
    if resolvidas:
        grupo_resolvido = resolvidas.get("grupos", {}).get(grupo, {})
        for entidade in grupo_resolvido.get("entidades", {}).values():
            if entidade.get("cnpj"):
                codigos.add(str(entidade["cnpj"])[:8])
            if entidade.get("codigo_conglomerado_prudencial"):
                codigos.add(str(entidade["codigo_conglomerado_prudencial"]))

    return sorted(codigos)


def marcar_is_safra(
    df: pl.DataFrame,
    entidades: EntidadesSafra,
    colunas_identificador: list[str],
    coluna_nome: str | None = None,
) -> pl.DataFrame:
    """Adiciona a coluna booleana `is_safra`, comparando as colunas de
    identificador (CNPJ/codigo de conglomerado) normalizadas e, opcionalmente,
    o nome da instituicao contra os termos de busca configurados."""

    def _linha_e_safra(valores: dict[str, Any]) -> bool:
        ids = [valores.get(c) for c in colunas_identificador]
        if entidades.contem(*ids):
            return True
        if coluna_nome and entidades.nome_corresponde(valores.get(coluna_nome)):
            return True
        return False

    colunas_usadas = list(colunas_identificador) + ([coluna_nome] if coluna_nome else [])
    is_safra = [
        _linha_e_safra(dict(zip(colunas_usadas, valores)))
        for valores in df.select(colunas_usadas).iter_rows()
    ]
    return df.with_columns(pl.Series("is_safra", is_safra, dtype=pl.Boolean))
