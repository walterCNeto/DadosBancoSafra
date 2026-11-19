"""Reclamações (ranking do BCB) e agências (Estban)."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

com.cabecalho("Reclamações e agências")

st.header("Reclamações")
st.caption("Ranking de reclamações do BCB: índice = reclamações procedentes por milhão de clientes (não é %).")
if com.existe("reclamacoes"):
    where, params = com.clausula()
    tipos = com.q(f"select distinct tipo_instituicao from gold.reclamacoes where {where} order by 1", params)["tipo_instituicao"].tolist()
    if tipos:
        tipo = st.radio("Tipo de instituição", tipos, horizontal=True)
        df = com.q(f"select * from gold.reclamacoes where tipo_instituicao = ? and {where} order by data_base, entidade", (tipo, *params))
        df["serie"] = df["entidade"].fillna(df["instituicao"])
        d = df.assign(entidade=df["serie"])
        # tres medianas: pares nomeados e da categoria do Safra (so faz sentido onde ha Safra no filtro)
        mp = df[["data_base", "indice_mediana_pares"]].dropna().drop_duplicates("data_base").rename(columns={"indice_mediana_pares": "indice"})
        com.linhas(d, "indice", titulo="Índice de reclamações (por milhão de clientes)", unidade="num", mediana=mp,
                   rotulo_mediana="Mediana dos pares nomeados")
        sf = df[df["grupo"] == "safra"]
        if not sf.empty:
            mc = sf[["data_base", "indice_mediana_categoria"]].dropna().drop_duplicates("data_base").rename(
                columns={"indice_mediana_categoria": "indice"})
            st.caption("Mediana da categoria: todas as instituições do ranking na categoria do Safra (inclusive o Safra). "
                       "Categoria do Safra: " + "; ".join(sorted(sf["categoria"].dropna().unique())))
            com.linhas(sf.assign(entidade="Safra"), "indice", titulo="Safra vs. mediana da categoria", unidade="num", mediana=mc,
                       rotulo_mediana="Mediana da categoria", altura=300)
        com.tabela(df[["data_base", "grupo", "entidade", "instituicao", "categoria", "indice", "indice_mediana_pares",
                       "indice_mediana_categoria", "n_categoria", "qtd_reclamacoes_respondidas", "qtd_procedentes",
                       "qtd_clientes_total"]].sort_values(["data_base", "entidade"], ascending=[False, True]))
    else:
        com.nada_a_mostrar()
else:
    com.nada_a_mostrar("gold.reclamacoes indisponível: falta a fonte bcb.reclamacoes.")

st.header("Agências")
st.caption("Estban: contagem de agências e municípios atendidos por entidade.")
if com.existe("agencias"):
    where, params = com.clausula()
    ag = com.q(f"select * from gold.agencias where {where} order by data_base, entidade", params)
    if ag.empty:
        com.nada_a_mostrar()
    else:
        a, b = st.columns(2)
        with a:
            m = ag[["data_base", "n_agencias_mediana_pares"]].drop_duplicates("data_base").rename(columns={"n_agencias_mediana_pares": "n_agencias"})
            com.linhas(ag, "n_agencias", titulo="Número de agências", unidade="num", mediana=m, altura=340)
        with b:
            m = ag[["data_base", "n_municipios_mediana_pares"]].drop_duplicates("data_base").rename(columns={"n_municipios_mediana_pares": "n_municipios"})
            com.linhas(ag, "n_municipios", titulo="Municípios com agência", unidade="num", mediana=m, altura=340)
        ult = ag[ag["data_base"] == ag["data_base"].max()]
        st.write(f"**Último período ({pd.Timestamp(ult['data_base'].max()):%m/%Y}):**")
        com.tabela(ult.drop(columns=["data_base"]))
else:
    com.nada_a_mostrar("gold.agencias indisponível: falta a fonte bcb.estban.")
