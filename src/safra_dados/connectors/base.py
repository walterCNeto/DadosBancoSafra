"""Interface comum dos conectores e utilitarios compartilhados (deteccao de
encoding/separador de CSV, extracao segura de zip)."""

from __future__ import annotations

import io
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from ..config import Settings

# Encodings tipicos de arquivos do BCB, nesta ordem de preferencia.
_CANDIDATE_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")


@dataclass
class Recurso:
    """Descreve um recurso remoto a ser baixado (um arquivo, uma chamada de API)."""

    fonte: str
    data_base: str
    url: str
    nome_arquivo: str
    params: dict[str, Any] | None = None
    extra: dict[str, Any] | None = None


class Connector(ABC):
    """Interface comum: discover() -> recursos, fetch(recurso) -> bronze, normalize() -> silver."""

    nome: str

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @abstractmethod
    def discover(self, desde: str, ate: str | None = None) -> list[Recurso]:
        """Lista os recursos disponiveis entre `desde` e `ate` (datas-base "AAAA-MM")."""

    @abstractmethod
    def fetch(self, recurso: Recurso) -> None:
        """Baixa um recurso para a camada bronze e registra no manifest.jsonl."""

    @abstractmethod
    def normalize(self, data_base: str) -> Path | None:
        """Le a camada bronze de uma data-base e grava o parquet correspondente na silver."""


def decode_bytes(raw: bytes, encodings: Iterable[str] = _CANDIDATE_ENCODINGS) -> str:
    """Decodifica bytes de um arquivo do BCB tentando utf-8, depois cp1252/latin-1."""
    last_exc: Exception | None = None
    for encoding in encodings:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError as exc:
            last_exc = exc
    # latin-1 nunca falha (mapeia todo byte), mas se chegou aqui por algum motivo:
    if last_exc is not None:
        return raw.decode("latin-1")
    raise AssertionError("lista de encodings vazia")


def extract_single_csv(zip_bytes: bytes) -> tuple[str, bytes]:
    """Extrai com seguranca o (unico, ou primeiro) arquivo de dados de um zip em memoria.
    Protege contra zip-slip validando que nenhum membro escapa do diretorio."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for info in zf.infolist():
            name = info.filename
            if name.startswith("/") or ".." in Path(name).parts:
                raise ValueError(f"entrada de zip suspeita (zip-slip): {name!r}")
        members = [i for i in zf.infolist() if not i.is_dir()]
        if not members:
            raise ValueError("zip sem arquivos")
        # prioriza .csv se houver mais de um membro
        csv_members = [m for m in members if m.filename.lower().endswith(".csv")]
        chosen = csv_members[0] if csv_members else members[0]
        return chosen.filename, zf.read(chosen)


def write_silver_parquet(df: pl.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path
