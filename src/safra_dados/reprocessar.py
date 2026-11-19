"""Reprocessamento: refaz a silver de uma fonte a partir do bronze existente, sem rede,
aplicando as entidades aprovadas atuais (config/entidades*.yaml)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import Settings
from .connectors.base import Connector


@dataclass
class ResultadoReprocessar:
    fonte: str
    data_base: str
    silver: Path | None
    erro: str | None = None


def data_bases_bronze(settings: Settings, fonte: str) -> list[str]:
    """Data-bases com bronze (subpastas de `bronze/<fonte>/` que tem arquivos)."""
    base = settings.bronze_fonte_dir(fonte)
    if not base.is_dir():
        return []
    return sorted(
        d.name for d in base.iterdir()
        if d.is_dir() and not d.name.startswith("tmp_") and any(p.is_file() for p in d.iterdir())
    )


def reprocessar_fonte(settings: Settings, connector: Connector, fonte: str) -> list[ResultadoReprocessar]:
    """Chama `normalize` para cada data-base do bronze. Nao faz nenhuma requisicao HTTP:
    o conector e usado so para normalizar."""
    saida: list[ResultadoReprocessar] = []
    for data_base in data_bases_bronze(settings, fonte):
        try:
            saida.append(ResultadoReprocessar(fonte, data_base, connector.normalize(data_base)))
        except Exception as exc:  # uma data-base com problema nao impede as demais
            saida.append(ResultadoReprocessar(fonte, data_base, None, f"{type(exc).__name__}: {exc}"))
    return saida
