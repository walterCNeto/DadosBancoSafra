"""Carteira: cortes por modalidade, porte e indexador."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

com.cabecalho("Carteira de crédito", "IF.data (relatórios 7, 11, 13, 14 e 16). Valores em R$ bi; participação em % da carteira total.")
if not com.existe("carteira"):
    com.nada_a_mostrar("gold.carteira indisponível: falta a fonte bcb.ifdata.")
    st.stop()

where, params = com.clausula()
cortes = com.q(f"select distinct corte from gold.carteira where {where} order by 1", params)["corte"].tolist()
if not cortes:
    com.nada_a_mostrar()
    st.stop()
rotulos = {"modalidade": "Modalidade", "porte": "Porte (PJ)", "indexador": "Indexador", "carteira_res4966": "Carteira Res. 4.966",
           "nivel_risco": "Nível de risco"}
corte = st.radio("Corte", cortes, format_func=lambda c: rotulos.get(c, c), horizontal=True)
segs = com.q(f"select distinct segmento from gold.carteira where corte = ? and {where} order by 1", (corte, *params))["segmento"].tolist()
seg = st.selectbox("Segmento", segs) if len(segs) > 1 else (segs[0] if segs else None)
base = f"corte = ? and segmento = ? and {where}"
bp = (corte, seg, *params)

datas = com.q(f"select distinct data_base from gold.carteira where {base} order by 1 desc", bp)["data_base"].tolist()
if not datas:
    com.nada_a_mostrar()
    st.stop()
data = st.selectbox("Data-base", datas, format_func=lambda d: f"{d:%d/%m/%Y}")

df = com.q(f"select entidade, categoria, valor, participacao_na_carteira from gold.carteira "
           f"where {base} and data_base = ? and categoria not ilike 'Total%' and valor is not null order by valor desc", (*bp, data))
medida = st.radio("Medida", ["Valor (R$ bi)", "Participação (%)"], horizontal=True)
y, un = ("valor", "bi") if medida.startswith("Valor") else ("participacao_na_carteira", "pct")
ent = st.selectbox("Entidade (barras)", sorted(df["entidade"].unique()))
d1 = df[df["entidade"] == ent].sort_values(y, ascending=False).head(25)
com.barras(d1, "categoria", y, titulo=f"{rotulos.get(corte, corte)}: {ent}, {data:%m/%Y}", unidade=un, horizontal=True,
           altura=max(300, 28 * len(d1) + 100))

st.subheader("Evolução")
hist = com.q(f"select data_base, entidade, categoria, valor, participacao_na_carteira from gold.carteira "
             f"where {base} and entidade = ? and categoria not ilike 'Total%' and valor is not null order by data_base",
             (*bp, ent))
top = hist.groupby("categoria")["valor"].sum().nlargest(8).index
com.barras(hist[hist["categoria"].isin(top)], "data_base", y, titulo=f"Principais categorias, {ent}", cor="categoria", unidade=un)

st.subheader("Tabela")
com.tabela(df)
