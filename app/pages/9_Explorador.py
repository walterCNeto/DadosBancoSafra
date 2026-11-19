"""Explorador: qualquer view gold ou fonte silver, com filtros por coluna, paginação, download e SQL somente leitura."""

import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

from safra_dados.app_core import (  # noqa: E402
    LIMITE_DOWNLOAD, Filtro, SqlNaoPermitido, contar, exportar_consulta, montar_consulta, relacao_sql, relacoes_disponiveis,
    total_paginas, validar_sql_somente_leitura,
)

com.cabecalho("Explorador", "Nada é carregado por inteiro: a página mostra uma fatia (LIMIT/OFFSET) e o DuckDB filtra e ordena. "
              f"Download direto até {LIMITE_DOWNLOAD:,} linhas".replace(",", ".") + "; acima disso o arquivo vai para data\\exports.")
con, _ = com.conexao()


@st.cache_data(ttl=com.TTL, show_spinner=False)
def _colunas(rel: str):
    return com.colunas_da_relacao(com.conexao()[0], rel)


@st.cache_data(ttl=com.TTL, show_spinner="Lendo valores distintos...")
def _distintos(rel: str, col: str) -> list[str]:
    c = com.conexao()[0].cursor()
    return [r[0] for r in c.execute(
        f'select distinct cast("{col}" as varchar) v from {relacao_sql(rel)} where "{col}" is not null order by 1 limit 500').fetchall()]


def _num(txt: str):
    txt = (txt or "").strip().replace(".", "").replace(",", ".") if txt.count(",") else (txt or "").strip()
    return float(txt) if txt else None


def _paginar_e_baixar(sql_base: str, params: list, chave: str, formato_tipos: dict | None, nome: str) -> None:
    total = contar(con, sql_base, params)
    c1, c2, c3 = st.columns([1, 1, 2])
    tam = c1.selectbox("Linhas por página", [50, 100, 500, 1000], index=1, key=f"{chave}_tam")
    npag = total_paginas(total, tam)
    pag = c2.number_input("Página", min_value=1, max_value=npag, value=1, step=1, key=f"{chave}_pag")
    c3.markdown(f"**{total:,}** linhas · página {pag} de {npag}".replace(",", "."))
    df = com.q(f"select * from ({sql_base}) t limit {int(tam)} offset {(int(pag) - 1) * int(tam)}", tuple(params))
    if st.checkbox("Formatar (R$ bi / % / 3 casas, vírgula decimal)", value=True, key=f"{chave}_fmt"):
        st.dataframe(com.formatar_df(df, formato_tipos), hide_index=True, width="stretch")
    else:
        st.dataframe(df, hide_index=True, width="stretch")

    st.markdown("**Download** (valores brutos, sem formatação)")
    d1, d2 = st.columns(2)
    for col, fmt in ((d1, "csv"), (d2, "xlsx")):
        with col:
            sig = f"{chave}|{fmt}|{sql_base}|{params}"
            if st.button(f"Preparar {fmt}", key=f"{chave}_prep_{fmt}"):
                try:
                    st.session_state[f"{chave}_exp_{fmt}"] = (sig, exportar_consulta(con, com.settings(), sql_base, params, fmt, nome))
                except ValueError as exc:
                    st.error(str(exc))
            guardado = st.session_state.get(f"{chave}_exp_{fmt}")
            if guardado and guardado[0] == sig:
                r = guardado[1]
                if r.dados is not None:
                    st.download_button(f"Baixar {r.nome} ({r.n_linhas:,} linhas)".replace(",", "."), r.dados, file_name=r.nome,
                                       mime=r.mime, on_click="ignore", key=f"{chave}_dl_{fmt}")
                else:
                    st.success(f"{r.n_linhas:,} linhas (acima de {LIMITE_DOWNLOAD:,}): arquivo gravado em `{r.caminho}`".replace(",", "."))


aba_tab, aba_sql = st.tabs(["Tabela", "SQL (somente leitura)"])

with aba_tab:
    rels = relacoes_disponiveis(con)
    a, b = st.columns([1, 3])
    esquema = a.radio("Origem", ["gold", "silver"], horizontal=True)
    nome = b.selectbox("View / fonte", rels[esquema], key=f"exp_rel_{esquema}")
    rel = f"{esquema}.{nome}"
    cols = _colunas(rel)
    tipos = {c: t for c, t in cols}
    nomes = [c for c, _ in cols]

    with st.expander("Colunas, filtros e ordenação", expanded=True):
        sel = st.multiselect("Colunas exibidas", nomes, default=nomes[:30], key=f"exp_cols_{rel}")
        escolhidas = st.multiselect("Filtrar por coluna", nomes, key=f"exp_fcols_{rel}")
        filtros: list[Filtro] = []
        erros = False
        for c in escolhidas:
            t = tipos[c].upper()
            numerico = t in ("BIGINT", "INTEGER", "HUGEINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "REAL") or t.startswith("DECIMAL")
            data = t.startswith(("DATE", "TIMESTAMP"))
            k = f"exp_f_{rel}_{c}"
            if numerico or data:
                x, y = st.columns(2)
                lo = x.text_input(f"{c} ≥ ({'AAAA-MM-DD' if data else 'número'})", key=k + "_lo")
                hi = y.text_input(f"{c} ≤", key=k + "_hi")
                try:
                    vlo, vhi = ((lo.strip() or None, hi.strip() or None) if data else (_num(lo), _num(hi)))
                except ValueError:
                    st.error(f"valor numérico inválido em {c}")
                    erros = True
                    continue
                filtros.append(Filtro(c, "intervalo", (vlo, vhi)))
            else:
                modo = st.radio(f"{c}", ["contém", "lista"], horizontal=True, key=k + "_m")
                if modo == "contém":
                    filtros.append(Filtro(c, "contem", st.text_input(f"{c} contém", key=k + "_t")))
                else:
                    filtros.append(Filtro(c, "lista", st.multiselect(f"{c} em", _distintos(rel, c), key=k + "_l")))
        o1, o2 = st.columns([3, 1])
        ordem_col = o1.selectbox("Ordenar por", ["(nenhuma)"] + nomes, key=f"exp_ord_{rel}")
        asc = o2.radio("Direção", ["crescente", "decrescente"], horizontal=True, key=f"exp_dir_{rel}") == "crescente"

    if erros or not sel:
        st.stop()
    try:
        sql_base, params = montar_consulta(relacao_sql(rel), nomes, filtros, sel, [(ordem_col, asc)] if ordem_col != "(nenhuma)" else None)
        _paginar_e_baixar(sql_base, params, f"exp_{rel}", {c: tipos[c] for c in sel}, nome)
    except (duckdb.Error, ValueError) as exc:
        st.error(f"Erro na consulta: {exc}")

with aba_sql:
    st.caption("Apenas SELECT ou WITH (um comando por vez). Escrita, DDL, PRAGMA, ATTACH, COPY e leitura direta de arquivos são bloqueados. "
               "Use `gold.<view>` e `silver.<fonte>`.")
    padrao = "select data_base, entidade, indice_basileia, indice_basileia_mediana_pares\nfrom gold.capital\norder by data_base desc"
    texto = st.text_area("SQL", value=st.session_state.get("exp_sql", padrao), height=160, key="exp_sql")
    if st.button("Executar", key="exp_sql_run"):
        st.session_state["exp_sql_ok"] = texto
    if "exp_sql_ok" in st.session_state:
        try:
            limpo = validar_sql_somente_leitura(st.session_state["exp_sql_ok"])
        except SqlNaoPermitido as exc:
            st.error(f"Bloqueado: {exc}")
        else:
            try:
                _paginar_e_baixar(limpo, [], "exp_sql", None, "consulta_sql")
            except duckdb.Error as exc:
                st.error(f"Erro de SQL: {exc}")
