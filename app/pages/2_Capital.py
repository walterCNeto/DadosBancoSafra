"""Capital: PR, RWA por tipo e índices, Safra vs. pares."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

com.cabecalho("Capital", "IF.data relatório 5 (Basileia). Valores em R$ bi; índices em %. Linha tracejada: mediana dos pares.")
if not com.existe("capital"):
    com.nada_a_mostrar("gold.capital indisponível: falta a fonte bcb.ifdata.")
    st.stop()

f = com.filtros()
where, params = com.clausula(f)
df = com.q(f"select * from gold.capital where {where} order by data_base, entidade", params)
if df.empty:
    com.nada_a_mostrar()
    st.stop()

st.subheader("Índices")
indice = st.radio("Índice", ["Basileia", "Nível I", "Capital Principal", "Razão de alavancagem"], horizontal=True)
col, med = {
    "Basileia": ("indice_basileia", "indice_basileia_mediana_pares"),
    "Nível I": ("indice_nivel_1", "indice_nivel_1_mediana_pares"),
    "Capital Principal": ("indice_capital_principal", "indice_capital_principal_mediana_pares"),
    "Razão de alavancagem": ("razao_alavancagem", "razao_alavancagem_mediana_pares"),
}[indice]
mediana = df[["data_base", med]].drop_duplicates("data_base").rename(columns={med: col})
com.linhas(df, col, titulo=f"{indice} (%)", unidade="pct", mediana=mediana)

st.subheader("Patrimônio de Referência")
com.linhas(df, "patrimonio_referencia", titulo="Patrimônio de Referência (R$ bi)", unidade="bi")

st.subheader("RWA por tipo de risco")
ent = st.selectbox("Entidade", sorted(df["entidade"].unique()), key="cap_ent")
r = df[df["entidade"] == ent]
long = r.melt(id_vars=["data_base"], value_vars=["rwa_credito", "rwa_mercado", "rwa_operacional", "rwa_pagamentos"],
              var_name="tipo", value_name="rwa").dropna()
long["tipo"] = long["tipo"].str.replace("rwa_", "RWA ", regex=False)
com.barras(long, "data_base", "rwa", titulo=f"RWA por tipo, {ent} (R$ bi)", cor="tipo", unidade="bi")

st.subheader("Tabela")
com.tabela(df.drop(columns=["cod_inst"]))
