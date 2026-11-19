"""Logica do app Streamlit (Bloco 6), sem dependencia de streamlit: conexao DuckDB em memoria com as views
gold/silver, formatacao, paginacao, filtros dinamicos, validacao de SQL somente leitura, exportacao e status das
fontes. Testavel sem rede e sem UI.

O app NUNCA abre `data/gold/safra.duckdb`: cria um banco em memoria e aplica as mesmas definicoes de
`gold.aplicar_views`."""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from .calendario import CALENDARIOS, vencida
from .config import Settings
from .gold import SILVER_VIEWS, ResultadoGold, _bi, _num, _pct, aplicar_views
from .manifest import load_checkpoint

LIMITE_DOWNLOAD = 50_000  # acima disso o arquivo vai para data/exports
LINHAS_EXCEL = 1_048_576
TAMANHO_PAGINA = 100


# ---------------------------------------------------------------- conexao em memoria

def _silver_extras(settings: Settings) -> dict[str, str]:
    """Fontes silver sem view gold-base: nome da view -> glob. Subpastas viram `<fonte>__<sub>`."""
    cobertos = {(settings.silver_dir / p).parent.resolve() for p in SILVER_VIEWS.values()}
    extras: dict[str, str] = {}
    if not settings.silver_dir.is_dir():
        return extras
    for d in sorted(p for p in settings.silver_dir.iterdir() if p.is_dir()):
        if list(d.glob("*.parquet")) and d.resolve() not in cobertos:
            extras[d.name] = (d / "*.parquet").as_posix()
        for sub in sorted(p for p in d.iterdir() if p.is_dir()):
            if list(sub.glob("*.parquet")) and sub.resolve() not in cobertos:
                extras[f"{d.name}__{sub.name}"] = (sub / "*.parquet").as_posix()
    return extras


# views gold pesadas (varrem o informe/CDA de fundos, ~25 s cada) e de resultado pequeno: o app as materializa UMA vez
# como tabela em memoria (o resultado e uma agregacao; nenhuma tabela silver e copiada)
MATERIALIZAR = ("fundos", "fundos_decomposicao", "fundos_conciliacao")


def criar_conexao_memoria(settings: Settings, materializar: tuple[str, ...] = MATERIALIZAR,
                          ) -> tuple[duckdb.DuckDBPyConnection, ResultadoGold]:
    """Banco EM MEMORIA com as views silver/gold (mesmo SQL de `criar_gold`) e as fontes silver extras."""
    con = duckdb.connect(":memory:")
    resultado = aplicar_views(con, settings)
    for nome in materializar:
        if nome in resultado.criadas:
            con.execute(f"create table gold._m as select * from gold.{nome}")
            con.execute(f"drop view gold.{nome}")
            con.execute(f"alter table gold._m rename to {nome}")
    for nome, alvo in _silver_extras(settings).items():
        try:
            con.execute(f'create or replace view silver."{nome}" as select * from read_parquet(\'{alvo}\', union_by_name = true)')
            resultado.silver_views.append(nome)
        except duckdb.Error as exc:
            resultado.erros[f"silver.{nome}"] = str(exc).splitlines()[0]
    return con, resultado


def relacoes_disponiveis(con: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    """{'gold': [...], 'silver': [...]} (views e tabelas de cada schema)."""
    saida: dict[str, list[str]] = {"gold": [], "silver": []}
    for schema, nome in con.execute(
        "select schema_name, view_name from duckdb_views() where not internal and schema_name in ('gold', 'silver') "
        "union all select schema_name, table_name from duckdb_tables() where schema_name in ('gold', 'silver') "
        "order by 1, 2"
    ).fetchall():
        saida[schema].append(nome)
    return saida


def colunas_da_relacao(con: duckdb.DuckDBPyConnection, relacao: str) -> list[tuple[str, str]]:
    """[(coluna, tipo)] de `schema.nome` (validado contra o catalogo)."""
    schema, _, nome = relacao.partition(".")
    if nome not in relacoes_disponiveis(con).get(schema, []):
        raise ValueError(f"relacao desconhecida: {relacao!r}")
    return [(r[0], r[1]) for r in con.execute(f'describe select * from {schema}."{nome}"').fetchall()]


# ---------------------------------------------------------------- formatacao

fmt_bi = _bi    # R$ 29,117 bi
fmt_pct = _pct  # 18,141%
fmt_num = _num  # 1.234,567


def fmt_int(x: Any) -> str:
    return "n/d" if x is None else f"{int(x):,}".replace(",", ".")


_RE_PCT = re.compile(
    r"(indice_(basileia|nivel_1|capital_principal)|razao_alavancagem|^roe|^roa|_roe|_roa|sinistralidade|participacao_na_carteira|"
    r"^taxa_|mediana_pares_ao|^dif_ao_ano|_pct$|^dif_ativo|diferenca_pct)"
)
_RE_BRL = re.compile(
    r"^(patrimonio_referencia|capital_(nivel_1|principal|complementar|nivel_2)|rwa_|exposicao_total|adicional_capital|"
    r"ativo_total|ativo_base|carteira_credito|carteira_total|captacoes|patrimonio_liquido|pl_|lucro_|valor$|valor_|"
    r"premio_|sinistro_|provisoes|margem_solvencia|diferenca(_com_pl_ses)?$|posicoes_em_cotas|cotas_|estban_)"
)


def tipo_formato(coluna: str, tipo_sql: str) -> str:
    """'bi' (reais -> R$ bi), 'pct' (indice em %), 'num' (decimal, 3 casas), 'int' ou 'txt'."""
    t = tipo_sql.upper()
    if t in ("DOUBLE", "FLOAT", "REAL") or t.startswith("DECIMAL"):
        if _RE_PCT.search(coluna):
            return "pct"
        if _RE_BRL.search(coluna) and not coluna.endswith("_pct"):
            return "bi"
        return "num"
    if t in ("BIGINT", "INTEGER", "HUGEINT", "SMALLINT", "TINYINT", "UBIGINT", "UINTEGER"):
        if _RE_BRL.search(coluna) and coluna.startswith(("valor", "pl_")):
            return "bi"
        return "int"
    return "txt"


def formatar_valor(x: Any, formato: str) -> Any:
    if formato == "bi":
        return fmt_bi(x)
    if formato == "pct":
        return fmt_pct(x)
    if formato == "num":
        return fmt_num(x)
    if formato == "int":
        return fmt_int(x)
    return x


def formatar_df(df, tipos: dict[str, str] | None = None):
    """Copia do DataFrame (pandas) com colunas de reais em R$ bi, indices em % e demais decimais com 3 casas,
    separador decimal virgula. So para EXIBICAO; o download usa os valores brutos."""
    saida = df.copy()
    for col in saida.columns:
        dtype = (tipos or {}).get(col) or str(saida[col].dtype)
        mapa = {"float64": "DOUBLE", "Float64": "DOUBLE", "int64": "BIGINT", "Int64": "BIGINT"}
        fmt = tipo_formato(col, mapa.get(dtype, dtype))
        if fmt != "txt":
            saida[col] = pd.Series([formatar_valor(None if x is None or x != x else x, fmt) for x in saida[col]],
                                   index=saida.index, dtype="object")
    return saida


def reais_para_bi(x: float | None) -> float | None:
    return None if x is None else round(x / 1e9, 3)


# ---------------------------------------------------------------- SQL somente leitura

_PROIBIDAS = (
    "insert", "update", "delete", "drop", "create", "alter", "attach", "detach", "copy", "export", "import", "install",
    "load", "pragma", "set", "reset", "call", "truncate", "vacuum", "checkpoint", "use", "begin", "commit", "rollback",
    "grant", "revoke", "merge", "replace", "summarize_cache", "force",
)
_RE_FUNCOES_ARQUIVO = re.compile(r"\b(read_\w+|glob|\w+_scan|sniff_csv|parquet_\w+|query_table|query)\s*\(", re.I)


class SqlNaoPermitido(ValueError):
    pass


def _sem_literais_e_comentarios(sql: str) -> str:
    """Troca literais ('...' e \"...\") por espacos e remove comentarios, para inspecionar so a estrutura."""
    saida: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "-" and sql[i:i + 2] == "--":
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif c == "/" and sql[i:i + 2] == "/*":
            j = sql.find("*/", i + 2)
            if j < 0:
                raise SqlNaoPermitido("comentario /* nao fechado")
            i = j + 2
            saida.append(" ")
        elif c in ("'", '"'):
            j = i + 1
            while True:
                j = sql.find(c, j)
                if j < 0:
                    raise SqlNaoPermitido("literal nao fechado")
                if sql[j + 1:j + 2] == c:  # '' escapado
                    j += 2
                    continue
                break
            saida.append(" " + ("_" if c == '"' else " ") + " ")
            i = j + 1
        else:
            saida.append(c)
            i += 1
    return "".join(saida)


def validar_sql_somente_leitura(sql: str) -> str:
    """Devolve o SQL limpo (sem `;` final) ou levanta `SqlNaoPermitido`. Aceita um unico comando que comece com
    SELECT ou WITH e nao contenha comandos de escrita/administracao nem funcoes de leitura de arquivos."""
    if not sql or not sql.strip():
        raise SqlNaoPermitido("consulta vazia")
    estrutura = _sem_literais_e_comentarios(sql).strip().rstrip(";").strip()
    if not estrutura:
        raise SqlNaoPermitido("consulta vazia")
    if ";" in estrutura:
        raise SqlNaoPermitido("apenas um comando por vez")
    primeira = re.match(r"\(*\s*([A-Za-z_]+)", estrutura)
    if not primeira or primeira.group(1).lower() not in ("select", "with"):
        raise SqlNaoPermitido("so sao permitidos comandos SELECT ou WITH")
    palavras = {p.lower() for p in re.findall(r"[A-Za-z_]+", estrutura)}
    bloqueadas = sorted(palavras & set(_PROIBIDAS))
    if bloqueadas:
        raise SqlNaoPermitido("palavra(s) nao permitida(s): " + ", ".join(bloqueadas))
    if _RE_FUNCOES_ARQUIVO.search(estrutura):
        raise SqlNaoPermitido("funcoes de leitura de arquivos nao sao permitidas")
    return sql.strip().rstrip(";").strip()


# ---------------------------------------------------------------- consulta paginada com filtros dinamicos

@dataclass
class Filtro:
    coluna: str
    tipo: str  # 'contem' | 'intervalo' | 'lista'
    valor: Any  # str | (min, max) | list


def _ident(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def montar_where(filtros: list[Filtro], colunas_validas: set[str]) -> tuple[str, list[Any]]:
    partes: list[str] = []
    params: list[Any] = []
    for f in filtros:
        if f.coluna not in colunas_validas:
            raise ValueError(f"coluna desconhecida: {f.coluna!r}")
        col = _ident(f.coluna)
        if f.tipo == "contem":
            if f.valor not in (None, ""):
                partes.append(f"cast({col} as varchar) ilike ?")
                params.append(f"%{f.valor}%")
        elif f.tipo == "intervalo":
            lo, hi = f.valor
            if lo is not None:
                partes.append(f"{col} >= ?")
                params.append(lo)
            if hi is not None:
                partes.append(f"{col} <= ?")
                params.append(hi)
        elif f.tipo == "lista":
            vals = list(f.valor or [])
            if vals:
                partes.append(f"cast({col} as varchar) in ({', '.join('?' for _ in vals)})")
                params.extend(str(v) for v in vals)
        else:
            raise ValueError(f"tipo de filtro invalido: {f.tipo!r}")
    return (" where " + " and ".join(partes)) if partes else "", params


def montar_consulta(
    relacao: str, colunas_validas: list[str], filtros: list[Filtro] | None = None, colunas: list[str] | None = None,
    ordem: list[tuple[str, bool]] | None = None, limite: int | None = None, offset: int = 0,
) -> tuple[str, list[Any]]:
    """SQL parametrizado `select <colunas> from <relacao> [where] [order by] [limit/offset]`. Colunas e relacao
    precisam ter sido validadas (nome vindo do catalogo)."""
    validas = set(colunas_validas)
    sel = colunas or list(colunas_validas)
    for c in sel:
        if c not in validas:
            raise ValueError(f"coluna desconhecida: {c!r}")
    where, params = montar_where(filtros or [], validas)
    sql = f"select {', '.join(_ident(c) for c in sel)} from {relacao}{where}"
    if ordem:
        for c, _ in ordem:
            if c not in validas:
                raise ValueError(f"coluna desconhecida: {c!r}")
        sql += " order by " + ", ".join(f"{_ident(c)} {'asc' if asc else 'desc'}" for c, asc in ordem)
    if limite is not None:
        sql += f" limit {int(limite)} offset {max(int(offset), 0)}"
    return sql, params


def relacao_sql(relacao: str) -> str:
    schema, _, nome = relacao.partition(".")
    return f'{schema}."{nome}"'


def contar(con: duckdb.DuckDBPyConnection, sql_base: str, params: list[Any] | None = None) -> int:
    return con.cursor().execute(f"select count(*) from ({sql_base}) t", params or []).fetchone()[0]


def pagina(con: duckdb.DuckDBPyConnection, sql_base: str, params: list[Any] | None, numero: int,
           tamanho: int = TAMANHO_PAGINA):
    """DataFrame (pandas) da pagina `numero` (a partir de 1) via LIMIT/OFFSET no DuckDB."""
    off = max(numero - 1, 0) * tamanho
    return con.cursor().execute(f"select * from ({sql_base}) t limit {int(tamanho)} offset {off}", params or []).df()


def total_paginas(total: int, tamanho: int = TAMANHO_PAGINA) -> int:
    return max(1, -(-total // tamanho))


# ---------------------------------------------------------------- download / exportacao

@dataclass
class ResultadoExportacao:
    n_linhas: int
    dados: bytes | None = None   # preenchido quando n_linhas <= LIMITE_DOWNLOAD
    caminho: Path | None = None  # preenchido quando excede o limite (arquivo em data/exports)
    nome: str = ""
    mime: str = ""


def exportar_consulta(con: duckdb.DuckDBPyConnection, settings: Settings, sql_base: str, params: list[Any] | None,
                      formato: str, nome: str) -> ResultadoExportacao:
    """Ate 50.000 linhas: bytes para `st.download_button`. Acima: grava em data/exports e devolve o caminho."""
    if formato not in ("csv", "xlsx"):
        raise ValueError("formato invalido (csv ou xlsx)")
    nome = re.sub(r"[^A-Za-z0-9_.-]+", "_", nome).strip("_") or "consulta"
    n = contar(con, sql_base, params)
    mime = "text/csv" if formato == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if n <= LIMITE_DOWNLOAD:
        df = con.cursor().execute(sql_base, params or []).df()
        if formato == "csv":
            dados = df.to_csv(index=False).encode("utf-8")
        else:
            buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="xlsxwriter")
            dados = buf.getvalue()
        return ResultadoExportacao(n, dados=dados, nome=f"{nome}.{formato}", mime=mime)
    if formato == "xlsx" and n >= LINHAS_EXCEL:
        raise ValueError(f"{n} linhas excedem o limite do Excel; exporte em csv")
    settings.exports_dir.mkdir(parents=True, exist_ok=True)
    destino = settings.exports_dir / f"{nome}.{formato}"
    cur = con.cursor()
    if formato == "csv":
        # parametros nao sao aceitos em COPY: materializa em tabela temporaria
        cur.execute(f"create or replace temp table _exp as {sql_base}", params or [])
        cur.execute(f"copy _exp to '{destino.as_posix()}' (header, delimiter ',')")
        cur.execute("drop table _exp")
    else:
        import polars as pl

        pl.from_pandas(cur.execute(sql_base, params or []).df()).write_excel(destino)
    return ResultadoExportacao(n, caminho=destino, nome=destino.name, mime=mime)


# ---------------------------------------------------------------- status das fontes

def status_fontes(settings: Settings, hoje: date | None = None) -> list[dict[str, Any]]:
    """Uma linha por fonte com calendario: checkpoint, data-base esperada, vencida?"""
    hoje = hoje or date.today()
    linhas = []
    for fonte, cal in CALENDARIOS.items():
        cp = load_checkpoint(settings.checkpoint_path(fonte), fonte)
        info = cp.data_bases
        esperada = cal.esperada(hoje)
        venc, motivo = vencida(fonte, cp, hoje)
        linhas.append({
            "fonte": fonte,
            "ultima_data_base_ok": cp.ultima_data_base_ok,
            "n_data_bases": len(info),
            "arquivos_ok": sum(v.get("arquivos_ok", 0) for v in info.values()),
            "arquivos_falha": sum(v.get("arquivos_falha", 0) for v in info.values()),
            "ultima_coleta": max((v.get("atualizado_em") or "" for v in info.values()), default="") or None,
            "proxima_data_base_esperada": esperada,
            "vencida": venc,
            "situacao": motivo,
            "calendario": cal.descricao,
        })
    return linhas
