"""Testes de integracao que acessam os servicos reais do BCB. Desabilitados
por padrao (`pytest -m "not rede"`); para roda-los: `pytest -m rede`."""

from __future__ import annotations

import pytest

from safra_dados.config import load_settings
from safra_dados.connectors.bcb_cosif_balancetes import (
    BcbCosifBalancetesConnector,
    latest_available_anomes as cosif_latest_anomes,
)
from safra_dados.connectors.bcb_ifdata import BcbIfDataConnector


@pytest.mark.rede
def test_ifdata_lista_relatorios_real():
    settings = load_settings()
    connector = BcbIfDataConnector(settings)
    relatorios = connector.lista_relatorios()
    assert any(r["NumeroRelatorio"] == "1" for r in relatorios)


@pytest.mark.rede
def test_cosif_discover_ultimo_trimestre_real():
    settings = load_settings()
    connector = BcbCosifBalancetesConnector(settings)
    anomes = cosif_latest_anomes()
    desde = f"{anomes[:4]}-{anomes[4:]}"
    recursos = connector.discover(desde, desde)
    assert recursos
    assert all(r.data_base == anomes for r in recursos)


@pytest.mark.rede
def test_ifdata_cadastro_e_valores_reais():
    import json

    from safra_dados.connectors.bcb_ifdata import url_cadastro, url_valores, latest_available_anomes as ult

    settings = load_settings()
    connector = BcbIfDataConnector(settings)
    client = connector._get_client()
    anomes = ult()
    cad = client.get(url_cadastro(connector.base_url, anomes, top=5))
    assert cad.status_code == 200 and cad.json()["value"]
    val = client.get(url_valores(connector.base_url, anomes, 1, "1", top=5))
    assert val.status_code == 200 and "Saldo" in json.dumps(val.json()["value"][0])


# ---------------- Bloco 2: um teste de rede por conector ----------------

def _fetch_primeiro(connector, recursos):
    assert recursos, "discover nao devolveu recursos"
    resposta = connector._get_client().get(recursos[0].url)
    assert resposta.status_code == 200, resposta.text[:200]
    return resposta


@pytest.mark.rede
def test_juros_real():
    from safra_dados.connectors.bcb_taxas_juros import BcbTaxasJurosConnector
    from safra_dados.periodos import mes_anterior

    c = BcbTaxasJurosConnector(load_settings())
    m = mes_anterior()
    recursos = c.discover(f"{m[:4]}-{m[4:]}", None)
    assert _fetch_primeiro(c, recursos).json()["value"]


@pytest.mark.rede
def test_tarifas_real():
    from safra_dados.connectors.bcb_tarifas import BcbTarifasConnector

    s = load_settings()
    c = BcbTarifasConnector(s)
    base = s.fonte_config("bcb_tarifas")["base_url"]
    r = c._get_client().get(c.url_instituicoes())
    assert r.status_code == 200 and r.json()["value"]
    r = c._get_client().get(c.url_tarifas("F", "58160789"))
    assert r.status_code == 200 and r.json()["value"] and base


@pytest.mark.rede
def test_unicad_real():
    from safra_dados.connectors.bcb_unicad import BcbUnicadConnector
    from safra_dados.periodos import mes_anterior

    c = BcbUnicadConnector(load_settings())
    m = mes_anterior()
    linhas = _fetch_primeiro(c, c.discover(f"{m[:4]}-{m[4:]}", None)).json()["value"]
    assert any(r["codigoCNPJ8"] == "58160789" for r in linhas)


@pytest.mark.rede
def test_reclamacoes_real():
    from safra_dados.connectors.bcb_reclamacoes import BcbReclamacoesConnector, parse_ranking_csv
    from safra_dados.connectors.base import decode_bytes

    c = BcbReclamacoesConnector(load_settings())
    recursos = c.discover("2025-01", None)
    resposta = c._get_client().get(recursos[-1].url)
    assert resposta.status_code == 200
    df = parse_ranking_csv(decode_bytes(resposta.content, ("utf-8-sig", "cp1252")))
    assert df.height > 50 and "indice" in df.columns


@pytest.mark.rede
def test_estban_real():
    from safra_dados.connectors.bcb_estban import BcbEstbanConnector
    from safra_dados.periodos import meses_entre

    c = BcbEstbanConnector(load_settings())
    ultimos = meses_entre("2026-01", None)[-6:]
    recursos = c.discover(f"{ultimos[0][:4]}-{ultimos[0][4:]}", None)
    assert {r.extra["nivel"] for r in recursos} == {"municipio", "agencia"}


@pytest.mark.rede
def test_sgs_real():
    from safra_dados.connectors.bcb_sgs import BcbSgsConnector
    from safra_dados.periodos import mes_anterior

    c = BcbSgsConnector(load_settings())
    m = mes_anterior()
    recursos = [r for r in c.discover(f"{m[:4]}-{m[4:]}", f"{m[:4]}-{m[4:]}") if r.extra["serie"] == 433]
    assert _fetch_primeiro(c, recursos).json()[0]["valor"]


# ---------------- Bloco 3 (CVM): um teste de rede por conector ----------------

def _cvm_discover_e_head(conector, desde, ate=None):
    """discover real via CKAN + HEAD no primeiro recurso (evita baixar zips grandes)."""
    recursos = conector.discover(desde, ate)
    assert recursos, f"discover vazio: {conector.falhas_discover}"
    assert not conector.falhas_discover
    assert conector._get_client().exists(recursos[0].url), recursos[0].url
    return recursos


@pytest.mark.rede
def test_cvm_cadastros_real():
    from safra_dados.connectors.cvm_cadastros import CvmCadastrosConnector, linhas_para_busca

    c = CvmCadastrosConnector(load_settings())
    tabelas = c.baixar_para_busca()
    assert {"cia_aberta", "intermed", "adm_cart_pj", "coord_oferta"} <= set(tabelas)
    assert any(l["cnpj_raiz"] == "58160789" for l in linhas_para_busca(tabelas))  # Banco Safra


@pytest.mark.rede
def test_cvm_fundos_cad_real():
    from safra_dados.connectors.cvm_fundos import CvmFundosCadConnector

    recursos = _cvm_discover_e_head(CvmFundosCadConnector(load_settings()), "2024-01")
    assert {r.nome_arquivo for r in recursos} == {"registro__registro_fundo_classe.zip", "legado__cad_fi.csv"}


@pytest.mark.rede
def test_cvm_fundos_informe_real():
    import polars as pl
    from safra_dados.connectors.cvm_fundos import CvmFundosInformeConnector
    from safra_dados.periodos import mes_anterior

    c = CvmFundosInformeConnector(load_settings())
    c._universo = pl.DataFrame({"cnpj": ["x"], "grupo": ["safra"], "is_safra": [True]})
    m = mes_anterior()
    _cvm_discover_e_head(c, f"{m[:4]}-{m[4:]}")
    # mes fora da janela de 12 meses do CKAN: tem que sair do template + HEAD
    antigos = c.discover("2024-01", "2024-01")
    assert [r.data_base for r in antigos] == ["202401"]


@pytest.mark.rede
def test_cvm_fundos_cda_real():
    import datetime

    import polars as pl
    from safra_dados.connectors.cvm_fundos import CvmFundosCdaConnector
    from safra_dados.periodos import mes_anterior

    c = CvmFundosCdaConnector(load_settings())
    c._universo = pl.DataFrame({"cnpj": ["x"], "grupo": ["safra"], "is_safra": [True]})
    # o CDA sai com defasagem: o ultimo mes publicado e o de dois meses atras
    m = mes_anterior(datetime.date.today().replace(day=1) - datetime.timedelta(days=1))
    _cvm_discover_e_head(c, f"{m[:4]}-{m[4:]}")


@pytest.mark.rede
def test_cvm_ofertas_real():
    from safra_dados.connectors.cvm_ofertas import CvmOfertasConnector

    _cvm_discover_e_head(CvmOfertasConnector(load_settings()), "2024-01")


@pytest.mark.rede
@pytest.mark.parametrize("classe", ["CvmDfpConnector", "CvmItrConnector", "CvmFreConnector", "CvmIpeConnector"])
def test_cvm_documentos_cia_aberta_real(classe):
    import datetime

    from safra_dados.connectors import cvm_cia_aberta

    c = getattr(cvm_cia_aberta, classe)(load_settings())
    ano = datetime.date.today().year - 1
    recursos = _cvm_discover_e_head(c, f"{ano}-01")
    assert all(int(r.data_base) >= ano for r in recursos)


# ---------------- Bloco 4 (SUSEP SES e RI Safra): um teste de rede por conector ----------------

@pytest.mark.rede
def test_susep_ses_real():
    """Pagina do SES + HEAD no zip e na LISTAEMPRESAS (sem baixar os ~580 MB)."""
    from safra_dados.connectors.susep_ses import SusepSesConnector

    c = SusepSesConnector(load_settings())
    recursos = c.discover("2020-01")
    assert {r.extra["chave"] for r in recursos} == {"lista", "base"}, c.falhas_discover
    assert len(recursos[0].data_base) == 6
    lista = next(r for r in recursos if r.extra["chave"] == "lista")
    assert c._get_client().exists(lista.url)


@pytest.mark.rede
def test_safra_ri_real():
    """robots.txt + listagens de RI (HTML); nao baixa arquivos. Os arquivos de /data/ sao bloqueados."""
    from safra_dados.connectors.safra_ri import SafraRiConnector

    c = SafraRiConnector(load_settings())
    recursos = c.discover("2023-01")
    docs = [r for r in recursos if r.extra["kind"] == "documento"]
    assert docs, c.falhas_discover
    assert {"demonstracoes_ifrs", "pilar3_gir"} <= {r.extra["tipo"] for r in docs}
    assert all(r.url.startswith("https://www.safra.com.br/") for r in docs)
