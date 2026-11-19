"""Juros: taxa do Safra vs. mediana dos pares por modalidade e período."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

com.cabecalho("Juros", "Taxas de juros por instituição (BCB). Taxas em % ao ano; linha tracejada: mediana dos pares na mesma modalidade/período.")
if not com.existe("juros"):
    com.nada_a_mostrar("gold.juros indisponível: falta a fonte bcb.juros.")
    st.stop()

where, params = com.clausula()
per = com.q(f"select distinct periodicidade from gold.juros where {where} order by 1", params)["periodicidade"].tolist()
if not per:
    com.nada_a_mostrar()
    st.stop()
periodicidade = st.radio("Periodicidade", per, horizontal=True)
mods = com.q(f"select modalidade, count(*) as n from gold.juros where periodicidade = ? and {where} "
             f"group by 1 order by n desc, 1", (periodicidade, *params))["modalidade"].tolist()
modalidade = st.selectbox("Modalidade", mods)
segs = com.q(f"select distinct segmento from gold.juros where periodicidade = ? and modalidade = ? and {where} order by 1",
             (periodicidade, modalidade, *params))["segmento"].tolist()
seg = st.selectbox("Segmento", segs) if len(segs) > 1 else (segs[0] if segs else None)
segsql, segp = ("segmento is null", ()) if seg is None else ("segmento = ?", (seg,))
base = f"periodicidade = ? and modalidade = ? and {segsql} and {where}"
bp = (periodicidade, modalidade, *segp, *params)

df = com.q(f"select * from gold.juros where {base} order by data_base, fim_periodo, entidade", bp)
if df.empty:
    com.nada_a_mostrar()
    st.stop()
import pandas as pd  # noqa: E402

# diaria: uma janela por semana/dia dentro do mes -> eixo x = fim da janela; mensal: data_base
fim = pd.to_datetime(df["fim_periodo"].astype(str), errors="coerce")
df["data_base"] = pd.to_datetime(fim.where(df["periodicidade"] != "mensal", df["data_base"]).fillna(df["data_base"]))
med = df[["data_base", "mediana_pares_ao_ano"]].dropna().drop_duplicates("data_base").rename(columns={"mediana_pares_ao_ano": "taxa_ao_ano"})
com.linhas(df, "taxa_ao_ano", titulo=f"Taxa a.a. (%): {modalidade}", unidade="pct", mediana=med)

st.subheader("Diferença do Safra para a mediana dos pares (p.p.)")
dif = df[df["grupo"] == "safra"]
com.linhas(dif, "dif_ao_ano_vs_mediana_pares", titulo="Safra menos mediana dos pares (p.p. a.a.)", unidade="num", altura=300)

st.subheader("Tabela")
com.tabela(df[["data_base", "periodicidade", "segmento", "modalidade", "grupo", "entidade", "taxa_ao_mes", "taxa_ao_ano",
               "mediana_pares_ao_mes", "mediana_pares_ao_ano", "dif_ao_ano_vs_mediana_pares", "n_pares", "melhor_posicao"]]
           .sort_values("data_base", ascending=False))
