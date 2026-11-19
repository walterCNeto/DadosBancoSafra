"""Utilitarios compartilhados do app (barra lateral global, conexao em memoria, cache e graficos).

Somente leitura: o app cria um DuckDB EM MEMORIA com as views de `safra_dados.gold.aplicar_views` e nunca abre
`data/gold/safra.duckdb`."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from safra_dados.app_core import (  # noqa: F401  (reexportados para as paginas)
    colunas_da_relacao, criar_conexao_memoria, fmt_bi, fmt_int, fmt_num, fmt_pct, formatar_df, reais_para_bi,
)
from safra_dados.config import Settings, load_settings

TTL = 3600
COR_SAFRA = "#0B3C5D"
COR_MEDIANA = "#7A7A7A"
PALETA_PARES = ["#D9822B", "#2E8B57", "#B5446E", "#6A5ACD", "#8C6D31", "#17A2B8", "#C4A000", "#5B8C85"]
PERIODO_MIN = date(2015, 1, 1)
GRUPOS = {"Safra": "safra", "Pares": "pares", "Todos": "todos"}


# ---------------------------------------------------------------- recursos em cache

@st.cache_resource(show_spinner=False)
def settings() -> Settings:
    return load_settings()


@st.cache_resource(show_spinner="Montando as views em memória (a primeira carga leva ~30 s)...")
def conexao():
    """(conexao DuckDB em memoria, ResultadoGold). Uma unica vez por processo; `recarregar()` a refaz."""
    return criar_conexao_memoria(settings())


@st.cache_data(ttl=TTL, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Consulta agregada/paginada no DuckDB em memoria, com cache de 1 h. Nunca use para `select *` de silver."""
    con, _ = conexao()
    return con.cursor().execute(sql, list(params)).df()


def existe(view: str) -> bool:
    _, res = conexao()
    return view in res.criadas


def recarregar() -> None:
    st.cache_data.clear()
    st.cache_resource.clear()


# ---------------------------------------------------------------- barra lateral global

def _entidades_do_grupo(grupo: str) -> list[str]:
    if grupo == "todos":
        df = q("select distinct entidade from gold.dim_entidade where grupo in ('safra', 'pares') order by 1")
    else:
        df = q("select distinct entidade from gold.dim_entidade where grupo = ? order by 1", (grupo,))
    return list(df["entidade"])


def sidebar(filtros: bool = True) -> None:
    with st.sidebar:
        st.markdown("### Safra • dados públicos")
        if st.button("Recarregar dados", help="Limpa os caches e refaz as views em memória (use após a atualização das 07:10)."):
            recarregar()
            st.rerun()
        if not filtros:
            st.caption("Esta página não usa os filtros globais.")
            return
        st.selectbox("Grupo", list(GRUPOS), index=2, key="f_grupo_rotulo")
        grupo = GRUPOS[st.session_state["f_grupo_rotulo"]]
        opcoes = _entidades_do_grupo(grupo)
        st.session_state["f_entidades"] = [e for e in st.session_state.get("f_entidades", []) if e in opcoes]
        st.multiselect("Entidade", opcoes, key="f_entidades", placeholder="Todas do grupo")
        st.slider("Período (data-base)", min_value=PERIODO_MIN, max_value=date.today(),
                  value=st.session_state.get("f_periodo", (PERIODO_MIN, date.today())), key="f_periodo", format="MM/YYYY")


def filtros() -> dict[str, Any]:
    """Filtros globais (valores padrao se a barra lateral ainda nao foi desenhada)."""
    rotulo = st.session_state.get("f_grupo_rotulo", "Todos")
    ini, fim = st.session_state.get("f_periodo", (PERIODO_MIN, date.today()))
    return {"grupo": GRUPOS.get(rotulo, "todos"), "entidades": list(st.session_state.get("f_entidades", [])),
            "inicio": ini, "fim": fim}


def clausula(f: dict[str, Any] | None = None, *, entidade: bool = True, grupo: bool = True, data: str | None = "data_base",
             prefixo: str = "") -> tuple[str, tuple]:
    """Trecho `where` (sem a palavra where) e parametros para views gold com grupo/entidade/data_base."""
    f = f or filtros()
    p = f"{prefixo}." if prefixo else ""
    partes, params = [], []
    if grupo:
        partes.append(f"{p}grupo in ('safra', 'pares')" if f["grupo"] == "todos" else f"{p}grupo = ?")
        if f["grupo"] != "todos":
            params.append(f["grupo"])
    if entidade and f["entidades"]:
        partes.append(f"{p}entidade in ({', '.join('?' for _ in f['entidades'])})")
        params += f["entidades"]
    if data:
        partes.append(f"{p}{data} between ? and ?")
        params += [f["inicio"], f["fim"]]
    return " and ".join(partes) or "true", tuple(params)


def nada_a_mostrar(msg: str = "Sem dados para os filtros escolhidos (ou fonte ainda não coletada).") -> None:
    st.info(msg)


def tabela(df: pd.DataFrame, **kw) -> None:
    st.dataframe(formatar_df(df), hide_index=True, width="stretch", **kw)


# ---------------------------------------------------------------- graficos

def _cores(entidades: list[tuple[str, str]]) -> dict[str, str]:
    cores, i = {}, 0
    for ent, grupo in entidades:
        if grupo == "safra":
            cores[ent] = COR_SAFRA
        else:
            cores[ent] = PALETA_PARES[i % len(PALETA_PARES)]
            i += 1
    return cores


def linhas(df: pd.DataFrame, y: str, *, titulo: str, unidade: str = "num", x: str = "data_base", serie: str = "entidade",
           mediana: pd.DataFrame | None = None, rotulo_mediana: str = "Mediana dos pares", marcador_x: tuple | None = None,
           altura: int = 380) -> None:
    """Grafico de linhas por entidade (Safra em destaque). `unidade`: bi (valores em reais -> R$ bi), pct ou num."""
    if df.empty or df[y].dropna().empty:
        nada_a_mostrar()
        return
    esc = 1e9 if unidade == "bi" else 1.0
    suf = {"bi": "", "pct": "%", "num": ""}[unidade]
    grupos = df["grupo"] if "grupo" in df else ["pares"] * len(df)
    pares = list(dict.fromkeys(zip(df[serie], grupos)))
    cores = _cores(sorted(pares, key=lambda p: p[1] != "safra"))
    fig = go.Figure()
    for nome, g in df.sort_values(x).groupby(serie, sort=False):
        safra = cores[nome] == COR_SAFRA
        fig.add_scatter(x=g[x], y=g[y] / esc, name=str(nome), mode="lines+markers",
                        line=dict(color=cores[nome], width=4 if safra else 2), marker=dict(size=7 if safra else 5),
                        connectgaps=True, hovertemplate="%{x|%d/%m/%Y}<br>%{y:,.3f}" + suf + "<extra>" + str(nome) + "</extra>")
    if mediana is not None and not mediana.empty:
        m = mediana.dropna().sort_values(x)
        fig.add_scatter(x=m[x], y=m[y] / esc, name=rotulo_mediana, mode="lines",
                        line=dict(color=COR_MEDIANA, width=2, dash="dash"),
                        hovertemplate="%{x|%d/%m/%Y}<br>%{y:,.3f}" + suf + "<extra>" + rotulo_mediana + "</extra>")
    if marcador_x:
        xv, texto = marcador_x
        fig.add_shape(type="line", x0=xv, x1=xv, y0=0, y1=1, yref="paper", line=dict(color="#C0392B", width=1.5, dash="dot"))
        fig.add_annotation(x=xv, y=1, yref="paper", text=texto, showarrow=False, yanchor="bottom",
                           font=dict(color="#C0392B", size=11))
    fig.update_layout(title=titulo, height=altura, separators=",.", hovermode="x unified", margin=dict(t=60, b=30, l=10, r=10),
                      legend=dict(orientation="h", y=-0.2),
                      yaxis=dict(ticksuffix=suf or None, tickformat=",.3f", title="R$ bi" if unidade == "bi" else None),
                      xaxis=dict(tickformat="%m/%Y"))
    st.plotly_chart(fig, width="stretch")


def barras(df: pd.DataFrame, x: str, y: str, *, titulo: str, cor: str | None = None, unidade: str = "num",
           empilhado: bool = True, altura: int = 380, horizontal: bool = False) -> None:
    if df.empty:
        nada_a_mostrar()
        return
    esc = 1e9 if unidade == "bi" else 1.0
    suf = "%" if unidade == "pct" else ""
    cores = [COR_SAFRA, *PALETA_PARES, "#999999", "#444444", "#AA3377"]
    fig = go.Figure()
    grupos = [(None, df)] if cor is None else list(df.groupby(cor, sort=False))
    for i, (nome, g) in enumerate(grupos):
        fig.add_bar(x=g[y] / esc if horizontal else g[x], y=g[x] if horizontal else g[y] / esc,
                    name=str(nome) if nome is not None else y, orientation="h" if horizontal else "v",
                    marker_color=cores[i % len(cores)],
                    hovertemplate=("%{x:,.3f}" if horizontal else "%{y:,.3f}") + suf)
    eixo = dict(tickformat=",.3f", ticksuffix=suf or None, title="R$ bi" if unidade == "bi" else None)
    fig.update_layout(title=titulo, height=altura, separators=",.", barmode="stack" if empilhado else "group",
                      margin=dict(t=60, b=30, l=10, r=10), legend=dict(orientation="h", y=-0.25),
                      xaxis=eixo if horizontal else dict(tickformat="%m/%Y" if "data" in x else None),
                      yaxis=dict(autorange="reversed") if horizontal else eixo)
    st.plotly_chart(fig, width="stretch")


def cabecalho(titulo: str, legenda: str | None = None) -> None:
    st.title(titulo)
    if legenda:
        st.caption(legenda)
    _, res = conexao()
    for nome, erro in res.erros.items():
        st.warning(f"view {nome} com erro: {erro}")
