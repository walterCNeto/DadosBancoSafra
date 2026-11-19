"""Fontes e status: manifest/checkpoint por fonte, calendário e documentos de RI importados."""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common as com  # noqa: E402

from safra_dados.app_core import status_fontes  # noqa: E402
from safra_dados.importacao_manual import EXTENSOES, FONTE_RI, STATUS_IMPORTADO  # noqa: E402
from safra_dados.manifest import read_entries, ultimas_por_url  # noqa: E402

com.cabecalho("Fontes e status", "Checkpoint de cada fonte e calendário de publicação (mesma regra do `safra-dados atualizar`).")
st_ = com.settings()
snapshot = com.snapshot_info()  # None no modo local
if snapshot is not None:
    st.info(com.legenda_snapshot() + " A tabela abaixo reflete o estado das fontes nessa data (não há bronze/checkpoints na nuvem).")


@st.cache_data(ttl=com.TTL, show_spinner=False)
def _status() -> pd.DataFrame:
    if snapshot is not None:
        return pd.DataFrame(snapshot["fontes"])
    return pd.DataFrame(status_fontes(com.settings()))


df = _status()
vencidas = int(df["vencida"].sum())
a, b, c = st.columns(3)
a.metric("Fontes", len(df))
b.metric("Vencidas (a coletar)", vencidas)
c.metric("Com falha de arquivo", int((df["arquivos_falha"] > 0).sum()))
st.dataframe(df.rename(columns={
    "fonte": "fonte", "ultima_data_base_ok": "última data-base ok", "n_data_bases": "data-bases", "arquivos_ok": "arquivos ok",
    "arquivos_falha": "arquivos com falha", "ultima_coleta": "última coleta (UTC)",
    "proxima_data_base_esperada": "data-base esperada hoje", "vencida": "vencida?", "situacao": "situação", "calendario": "calendário",
}), hide_index=True, width="stretch")

st.subheader("Manifest de uma fonte")
fonte = st.selectbox("Fonte", df["fonte"].tolist())
raiz = st_.bronze_fonte_dir(fonte)
bases = sorted(p.name for p in raiz.iterdir() if p.is_dir()) if raiz.is_dir() else []
if snapshot is not None:
    st.info("Manifest por arquivo indisponível no modo nuvem (o bronze não é publicado).")
elif not bases:
    st.info("Sem bronze para esta fonte.")
else:
    base = st.selectbox("Data-base", bases[::-1])
    entradas = ultimas_por_url(read_entries(st_.manifest_path(fonte, base)))
    if entradas:
        m = pd.DataFrame([e.__dict__ for e in entradas])
        st.write(m["status"].value_counts().rename_axis("status").reset_index(name="arquivos").to_dict("records"))
        st.dataframe(m[["status", "coletado_em", "bytes", "arquivo", "url", "erro"]], hide_index=True, width="stretch")
    else:
        st.info("Manifest vazio para esta data-base.")

st.subheader("Documentos de RI do Banco Safra")
st.caption(f"Catálogo (`silver.safra_ri`) e arquivos importados manualmente em `{st_.manual_dir / FONTE_RI}` "
           f"(status `{STATUS_IMPORTADO}`).")
con, res = com.conexao()
if "safra_ri" in res.silver_views:
    cat = com.q("select status, count(*) as documentos from silver.safra_ri group by 1 order by 2 desc")
    st.dataframe(cat, hide_index=True, width="stretch")
    imp = com.q("select data_base, tipo, titulo, formato, status, arquivo_local, bytes, coletado_em from silver.safra_ri "
                "where status = ? or baixado order by data_base desc, tipo", (STATUS_IMPORTADO,))
    if imp.empty:
        st.info("Nenhum documento importado/baixado ainda (use `safra-dados importar-manual --fonte safra_ri`).")
    else:
        st.dataframe(imp, hide_index=True, width="stretch")
else:
    st.info("silver.safra_ri indisponível: rode `safra-dados ingest --fonte safra.ri`.")
pasta = st_.manual_dir / FONTE_RI
arqs = sorted(p for p in pasta.glob("*") if p.is_file() and p.suffix.lower() in EXTENSOES) if pasta.is_dir() else []
st.write(f"**Arquivos em data\\manual\\{FONTE_RI}:** {len(arqs)}")
if arqs:
    st.dataframe(pd.DataFrame([{"arquivo": p.name, "MB": round(p.stat().st_size / 1e6, 3)} for p in arqs]), hide_index=True,
                 width="stretch")
