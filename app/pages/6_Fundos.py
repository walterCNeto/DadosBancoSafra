"""Fundos: série do PL (principal, teto e mínimo), decomposição por papel e conciliação com a Nota 9."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

from safra_dados.gold import DESDE_PL_MINIMO_PADRAO, carregar_referencias  # noqa: E402

com.cabecalho("Fundos", "CVM (informe diário e CDA). Valores em R$ bi. Esta página usa só o intervalo de período da barra lateral "
              "(o grupo/entidade de fundos é sempre `fundos_safra`).")
if not com.existe("fundos"):
    com.nada_a_mostrar("gold.fundos indisponível: faltam as fontes cvm.fundos_informe, cvm.fundos_cda e cvm.fundos_cad.")
    st.stop()

f = com.filtros()
desde = str(carregar_referencias(com.settings()).get("fundos_pl_minimo_desde", DESDE_PL_MINIMO_PADRAO))
desde = pd.Timestamp(desde + "-01" if len(desde) == 7 else desde)
visao = st.radio("Visão", ["administrado", "gerido", "ambos"], horizontal=True,
                 help="administrado: fundos cujo administrador é do grupo; gerido: gestor do grupo; ambos: união.")
encerrados = st.checkbox("Somente meses encerrados", value=True)
extra = " and mes_encerrado" if encerrados else ""
df = com.q(f"select * from gold.fundos where visao = ? and data_base between ? and ?{extra} order by data_base",
           (visao, f["inicio"], f["fim"]))
if df.empty:
    com.nada_a_mostrar()
    st.stop()

serie = df.melt(id_vars=["data_base", "grupo"], value_vars=["pl_deduplicado_principal", "pl_deduplicado", "pl_deduplicado_minimo"],
                var_name="entidade", value_name="pl")
serie["entidade"] = serie["entidade"].map({
    "pl_deduplicado_principal": "PL principal", "pl_deduplicado": "Teto (só cotas identificadas)",
    "pl_deduplicado_minimo": "Mínimo (cotas confidenciais como intragrupo)"})
serie["grupo"] = "pares"
serie.loc[serie["entidade"] == "PL principal", "grupo"] = "safra"
st.caption(f"Marcação: a partir de {desde:%m/%Y} o PL principal passa a ser o mínimo (antes, o deduplicado pelas cotas identificadas).")
com.linhas(serie, "pl", titulo=f"PL de fundos, visão {visao} (R$ bi)", unidade="bi",
           marcador_x=(desde.to_pydatetime(), f"quebra de método: {desde:%m/%Y}"))

st.subheader("Decomposição por papel do grupo (PL bruto)")
if com.existe("fundos_decomposicao"):
    dec = com.q("select data_base, papel, n_fundos, pl_bruto from gold.fundos_decomposicao where data_base between ? and ? "
                "order by data_base", (f["inicio"], f["fim"]))
    dec["papel"] = dec["papel"].map({"administra_e_gere": "Administra e gere", "so_administrado": "Só administra (gestor externo)",
                                     "so_gerido": "Só gere (administrador externo)"})
    com.barras(dec, "data_base", "pl_bruto", titulo="PL bruto por papel (R$ bi)", cor="papel", unidade="bi")
else:
    com.nada_a_mostrar("gold.fundos_decomposicao indisponível.")

st.subheader("Conciliação com a Nota 9 (IFRS)")
if com.existe("fundos_conciliacao"):
    con = com.q("select * from gold.fundos_conciliacao order by data_base, id")
    if con.empty:
        com.nada_a_mostrar("Sem referências em config\\referencias.yaml.")
    else:
        for _, r in con.iterrows():
            if pd.isna(r["valor_gold"]):
                st.write(f"**{r['id']}**: sem PL no gold para {pd.Timestamp(r['data_base']):%d/%m/%Y}.")
                continue
            a, b, c, d = st.columns(4)
            a.metric(f"{r['id']}: referência", com.fmt_bi(r["valor_referencia"]))
            b.metric("Gold (principal)", com.fmt_bi(r["valor_gold"]))
            c.metric("Diferença", com.fmt_bi(r["diferenca"]), delta=com.fmt_pct(r["diferenca_pct"]), delta_color="off")
            d.metric("Teto (só cotas identificadas)", com.fmt_bi(r["valor_gold_teto"]), delta=com.fmt_pct(r["diferenca_teto_pct"]),
                     delta_color="off")
        com.tabela(con.drop(columns=["grupo", "entidade"]))
else:
    com.nada_a_mostrar("gold.fundos_conciliacao indisponível.")

st.subheader("Tabela")
com.tabela(df.sort_values("data_base", ascending=False))
