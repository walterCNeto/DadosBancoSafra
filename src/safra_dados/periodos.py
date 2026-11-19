"""Calendario mensal compartilhado pelos conectores do Bloco 2 (datas-base AAAAMM)."""

from __future__ import annotations

import calendar
from datetime import date


def mes_anterior(hoje: date | None = None) -> str:
    hoje = hoje or date.today()
    ano, mes = (hoje.year, hoje.month - 1) if hoje.month > 1 else (hoje.year - 1, 12)
    return f"{ano}{mes:02d}"


def meses_entre(
    desde: str, ate: str | None, hoje: date | None = None, *, incluir_mes_corrente: bool = False
) -> list[str]:
    """Meses "AAAAMM" de `desde` ("AAAA-MM") ate `ate` (inclusive). Sem `ate`, vai
    ate o mes anterior (ou o corrente, se `incluir_mes_corrente`)."""
    hoje = hoje or date.today()
    ano, mes = (int(p) for p in desde.split("-"))
    if ate:
        fim = tuple(int(p) for p in ate.split("-"))
    elif incluir_mes_corrente:
        fim = (hoje.year, hoje.month)
    else:
        anterior = mes_anterior(hoje)
        fim = (int(anterior[:4]), int(anterior[4:]))
    saida = []
    while (ano, mes) <= fim:
        saida.append(f"{ano}{mes:02d}")
        ano, mes = (ano, mes + 1) if mes < 12 else (ano + 1, 1)
    return saida


def ultimo_dia(anomes: str, hoje: date | None = None) -> date:
    """Ultimo dia do mes, limitado a `hoje` (mes corrente nao termina no futuro)."""
    ano, mes = int(anomes[:4]), int(anomes[4:6])
    dia = date(ano, mes, calendar.monthrange(ano, mes)[1])
    return min(dia, hoje or date.today())
