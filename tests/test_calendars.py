from __future__ import annotations

from datetime import date, timedelta

from safra_dados.connectors import bcb_cosif_balancetes, bcb_ifdata


def test_ifdata_publication_date():
    assert bcb_ifdata.publication_date("202306") == date(2023, 6, 30) + timedelta(days=60)
    assert bcb_ifdata.publication_date("202312") == date(2023, 12, 31) + timedelta(days=90)


def test_ifdata_latest_available_anomes_respeita_prazo_de_divulgacao():
    # 2026-09-30 + 60 dias = 2026-11-29: em 2026-11-19 ainda nao foi divulgado
    assert bcb_ifdata.latest_available_anomes(date(2026, 11, 19)) == "202606"
    assert bcb_ifdata.latest_available_anomes(date(2026, 12, 1)) == "202609"


def test_ifdata_quarters_between():
    anomes = bcb_ifdata.quarters_between("2024-01", "2024-12")
    assert anomes == ["202403", "202406", "202409", "202412"]


def test_cosif_publication_date_calendario_oficial():
    assert bcb_cosif_balancetes.publication_date("202603") == date(2026, 5, 31)
    assert bcb_cosif_balancetes.publication_date("202606") == date(2026, 8, 31)
    assert bcb_cosif_balancetes.publication_date("202609") == date(2026, 11, 30)
    assert bcb_cosif_balancetes.publication_date("202612") == date(2027, 3, 31)


def test_cosif_latest_available_anomes_respeita_prazo_de_divulgacao():
    assert bcb_cosif_balancetes.latest_available_anomes(date(2026, 11, 19)) == "202606"
    assert bcb_cosif_balancetes.latest_available_anomes(date(2026, 12, 1)) == "202609"
