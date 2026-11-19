"""Visão geral: último período do Safra vs. mediana dos pares e data-base de cada fonte."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

from safra_dados.calendario import CALENDARIOS  # noqa: E402
from safra_dados.manifest import load_checkpoint  # noqa: E402

com.cabecalho("Visão geral", "Último período do Safra vs. mediana dos pares (cada indicador no seu último período disponível). "
              "Desta página, só o fim do intervalo de período da barra lateral é usado.")
fim = com.filtros()["fim"]


def v(r, k):
    return None if r is None else r[k]


def um(sql: str, params: tuple = ()):
    df = com.q(sql, params)
    return None if df.empty else df.iloc[0]


def cartao(col, rotulo, valor, formato, mediana=None, data=None, pp=True):
    with col:
        if valor is None or pd.isna(valor):
            st.metric(rotulo, "n/d")
            return
        txt = {"pct": com.fmt_pct, "bi": com.fmt_bi, "num": com.fmt_num}[formato](valor)
        delta = None
        if mediana is not None and not pd.isna(mediana):
            dif = valor - mediana
            if formato == "pct":
                delta = f"{'+' if dif >= 0 else '-'}{com.fmt_num(abs(dif))} p.p. vs mediana pares ({com.fmt_pct(mediana)})"
            elif formato == "bi":
                delta = f"{com.fmt_bi(mediana)} mediana pares"
            else:
                delta = f"{'+' if dif >= 0 else '-'}{com.fmt_num(abs(dif))} vs mediana pares ({com.fmt_num(mediana)})"
        st.metric(rotulo, txt, delta=delta, delta_color="off", help=f"Data-base: {data}" if data is not None else None)
        if data is not None:
            st.caption(f"data-base {pd.Timestamp(data):%d/%m/%Y}")


if com.existe("capital"):
    c = um("select * from gold.capital where grupo = 'safra' and data_base <= ? order by data_base desc limit 1", (fim,))
else:
    c = None
if com.existe("balanco_dre"):
    b = um("select * from gold.balanco_dre where grupo = 'safra' and data_base <= ? order by data_base desc limit 1", (fim,))
    mc = um("select median(carteira_credito) as m from gold.balanco_dre where grupo = 'pares' and data_base = ?",
            (b["data_base"],)) if b is not None else None
else:
    b = mc = None
if com.existe("fundos"):
    fu = um("select * from gold.fundos where visao = 'administrado' and mes_encerrado and data_base <= ? "
            "order by data_base desc limit 1", (fim,))
else:
    fu = None
if com.existe("reclamacoes"):
    rc = um("select * from gold.reclamacoes where grupo = 'safra' and tipo_instituicao = 'Conglomerado' and data_base <= ? "
            "order by data_base desc limit 1", (fim,))
else:
    rc = None

st.subheader("Capital e rentabilidade")
k = st.columns(5)
cartao(k[0], "Índice de Basileia", v(c, "indice_basileia"), "pct", v(c, "indice_basileia_mediana_pares"), v(c, "data_base"))
cartao(k[1], "Nível I", v(c, "indice_nivel_1"), "pct", v(c, "indice_nivel_1_mediana_pares"), v(c, "data_base"))
cartao(k[2], "Capital Principal", v(c, "indice_capital_principal"), "pct", v(c, "indice_capital_principal_mediana_pares"),
       v(c, "data_base"))
cartao(k[3], "ROE anualizado", v(b, "roe_anualizado"), "pct", v(b, "roe_anualizado_mediana_pares"), v(b, "data_base"))
cartao(k[4], "ROA anualizado", v(b, "roa_anualizado"), "pct", v(b, "roa_anualizado_mediana_pares"), v(b, "data_base"))

st.subheader("Carteira, fundos e reclamações")
k = st.columns(3)
cartao(k[0], "Carteira de crédito", v(b, "carteira_credito"), "bi", v(mc, "m"), v(b, "data_base"))
cartao(k[1], "PL de fundos (principal, administrados)", v(fu, "pl_deduplicado_principal"), "bi", None, v(fu, "data_base"))
with k[1]:
    if fu is not None:
        st.caption(f"método: {fu['metodo_principal']}; pares: não há equivalente")
cartao(k[2], "Índice de reclamações (por milhão de clientes)", v(rc, "indice"), "num", v(rc, "indice_mediana_pares"),
       v(rc, "data_base"))

st.subheader("Data-base de cada fonte")
linhas = []
for fonte in CALENDARIOS:
    cp = load_checkpoint(com.settings().checkpoint_path(fonte), fonte)
    linhas.append({"fonte": fonte, "última data-base coletada": cp.ultima_data_base_ok})
st.dataframe(pd.DataFrame(linhas), hide_index=True, width="stretch")

st.subheader("Último período de cada view gold")
vs = []
for v in ("capital", "balanco_dre", "carteira", "juros", "reclamacoes", "agencias", "fundos", "seguros"):
    if com.existe(v) or v in ("fundos",):
        try:
            r = com.q(f"select max(data_base) as m from gold.{v}").iloc[0]["m"]
        except Exception:
            r = None
        vs.append({"view": f"gold.{v}", "último data_base": None if r is None or pd.isna(r) else f"{pd.Timestamp(r):%d/%m/%Y}"})
st.dataframe(pd.DataFrame(vs), hide_index=True, width="stretch")
