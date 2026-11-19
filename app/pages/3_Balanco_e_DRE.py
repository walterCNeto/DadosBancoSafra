"""Balanço e DRE: ativo, carteira, captações, PL, lucro, ROE/ROA e tabela trimestral."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

com.cabecalho("Balanço e DRE", "IF.data relatório 1 (resumo), com conferência pelo Cosif. Valores em R$ bi; ROE/ROA anualizados em %.")
if not com.existe("balanco_dre"):
    com.nada_a_mostrar("gold.balanco_dre indisponível: faltam as fontes bcb.ifdata e bcb.cosif.")
    st.stop()

where, params = com.clausula()
df = com.q(f"select * from gold.balanco_dre where {where} order by data_base, entidade", params)
if df.empty:
    com.nada_a_mostrar()
    st.stop()

metricas = {
    "Ativo total": "ativo_total", "Carteira de crédito": "carteira_credito", "Captações": "captacoes",
    "Patrimônio líquido": "patrimonio_liquido", "Lucro líquido acumulado no ano": "lucro_liquido_acum",
    "Lucro líquido do trimestre": "lucro_liquido_trimestre",
}
escolha = st.multiselect("Séries (R$ bi)", list(metricas), default=["Ativo total", "Carteira de crédito"])
for nome in escolha:
    com.linhas(df, metricas[nome], titulo=f"{nome} (R$ bi)", unidade="bi", altura=320)

st.subheader("Rentabilidade")
a, b = st.columns(2)
with a:
    m = df[["data_base", "roe_anualizado_mediana_pares"]].drop_duplicates("data_base").rename(columns={"roe_anualizado_mediana_pares": "roe_anualizado"})
    com.linhas(df, "roe_anualizado", titulo="ROE anualizado (%)", unidade="pct", mediana=m, altura=340)
with b:
    m = df[["data_base", "roa_anualizado_mediana_pares"]].drop_duplicates("data_base").rename(columns={"roa_anualizado_mediana_pares": "roa_anualizado"})
    com.linhas(df, "roa_anualizado", titulo="ROA anualizado (%)", unidade="pct", mediana=m, altura=340)

st.subheader("Tabela trimestral")
cols = ["data_base", "grupo", "entidade", "ativo_total", "carteira_credito", "captacoes", "patrimonio_liquido",
        "lucro_liquido_acum", "lucro_liquido_trimestre", "roe_anualizado", "roa_anualizado", "roe_anualizado_mediana_pares",
        "roa_anualizado_mediana_pares"]
com.tabela(df[cols].sort_values(["data_base", "entidade"], ascending=[False, True]))
with st.expander("Conferência com o Cosif (documento 4060)"):
    com.tabela(df[["data_base", "entidade", "ativo_total_cosif", "patrimonio_liquido_cosif", "lucro_liquido_acum_cosif",
                   "roe_anualizado_cosif", "roa_anualizado_cosif", "dif_ativo_cosif_vs_ifdata"]].dropna(subset=["ativo_total_cosif"]))
