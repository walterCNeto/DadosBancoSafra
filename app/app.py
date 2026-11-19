"""App Streamlit (somente leitura) para explorar os dados do projeto safra-dados.

Execute com `safra-dados app` ou `streamlit run app\\app.py`."""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common as com  # noqa: E402

st.set_page_config(page_title="Safra • dados públicos", page_icon="📊", layout="wide")

PAGINAS = [
    st.Page("pages/1_Visao_geral.py", title="Visão geral", icon="🏠", default=True),
    st.Page("pages/2_Capital.py", title="Capital", icon="🏦"),
    st.Page("pages/3_Balanco_e_DRE.py", title="Balanço e DRE", icon="📒"),
    st.Page("pages/4_Carteira.py", title="Carteira", icon="💼"),
    st.Page("pages/5_Juros.py", title="Juros", icon="💳"),
    st.Page("pages/6_Fundos.py", title="Fundos", icon="📈"),
    st.Page("pages/7_Seguros.py", title="Seguros", icon="🛡️"),
    st.Page("pages/8_Reclamacoes_e_agencias.py", title="Reclamações e agências", icon="📣"),
    st.Page("pages/9_Explorador.py", title="Explorador", icon="🔎"),
    st.Page("pages/10_Fontes_e_status.py", title="Fontes e status", icon="🗂️"),
]
SEM_FILTROS = {"Explorador", "Fontes e status"}

pagina = st.navigation(PAGINAS)
com.sidebar(filtros=pagina.title not in SEM_FILTROS)
pagina.run()
