"""Seguros: prêmios, sinistralidade, provisões e PL das entidades Safra vs. pares (SUSEP SES)."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

com.cabecalho("Seguros", "SUSEP SES. Valores em R$ bi; sinistralidade em % (sinistros ocorridos / prêmio ganho, acumulado no ano).")
if not com.existe("seguros"):
    com.nada_a_mostrar("gold.seguros indisponível: falta a fonte susep.ses.")
    st.stop()

where, params = com.clausula()
df = com.q(f"select * from gold.seguros where {where} order by data_base, entidade", params)
if df.empty:
    com.nada_a_mostrar()
    st.stop()

a, b = st.columns(2)
with a:
    com.linhas(df, "premio_direto_ano", titulo="Prêmio direto acumulado no ano (R$ bi)", unidade="bi", altura=340)
with b:
    m = df[["data_base", "sinistralidade_ano_mediana_pares"]].drop_duplicates("data_base").rename(
        columns={"sinistralidade_ano_mediana_pares": "sinistralidade_ano"})
    com.linhas(df, "sinistralidade_ano", titulo="Sinistralidade acumulada no ano (%)", unidade="pct", mediana=m, altura=340)
a, b = st.columns(2)
with a:
    com.linhas(df, "provisoes_tecnicas", titulo="Provisões técnicas (R$ bi)", unidade="bi", altura=340)
with b:
    com.linhas(df, "patrimonio_liquido", titulo="Patrimônio líquido (R$ bi)", unidade="bi", altura=340)

st.subheader("Tabela")
com.tabela(df[["data_base", "grupo", "entidade", "seguradora", "premio_direto", "premio_ganho", "sinistro_ocorrido",
               "premio_direto_ano", "sinistralidade", "sinistralidade_ano", "sinistralidade_ano_mediana_pares",
               "provisoes_tecnicas", "patrimonio_liquido", "pl_ajustado", "margem_solvencia"]]
           .sort_values(["data_base", "entidade"], ascending=[False, True]))
