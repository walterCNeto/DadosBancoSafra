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
