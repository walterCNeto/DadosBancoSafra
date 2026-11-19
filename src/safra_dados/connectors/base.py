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


class HttpConnector(Connector):
    """Conector que coleta recursos via HTTP (GET) e grava o corpo na bronze.

    Implementa `fetch` uma unica vez: baixa (com paginacao Olinda se `paginar`),
    calcula o sha256, so grava se o conteudo for novo e registra no manifest.
    Subclasses implementam discover() e normalize()."""

    paginar_olinda: bool = False

    def __init__(self, settings: Settings, client: Any | None = None) -> None:
        super().__init__(settings)
        self._client = client

    def _get_client(self):
        from ..http import SafraDadosClient

        if self._client is None:
            self._client = SafraDadosClient(self.settings.http)
        return self._client

    def _baixar(self, recurso: Recurso) -> bytes:
        """Devolve o corpo do recurso; levanta DownloadError/RuntimeError em falha."""
        client = self._get_client()
        if self.paginar_olinda:
            from ..olinda import juntar_paginas

            return juntar_paginas(client, recurso.url)
        resposta = client.get(recurso.url)
        if resposta.status_code != 200:
            raise RuntimeError(f"HTTP {resposta.status_code}: {resposta.text[:300]}")
        return resposta.content

    def fetch(self, recurso: Recurso) -> None:
        from ..http import DownloadError, sha256_bytes
        from ..manifest import ManifestEntry, already_collected, append_entry, now_iso

        manifest_path = self.settings.manifest_path(self.nome, recurso.data_base)
        dest = self.settings.bronze_fonte_dir(self.nome, recurso.data_base) / recurso.nome_arquivo

        def _registrar(**campos: Any) -> None:
            append_entry(
                manifest_path,
                ManifestEntry(
                    url=recurso.url, data_base=recurso.data_base, coletado_em=now_iso(),
                    arquivo=str(dest), **campos,
                ),
            )

        try:
            content = self._baixar(recurso)
        except (DownloadError, RuntimeError) as exc:
            _registrar(status="falha", erro=str(exc))
            return

        sha256 = sha256_bytes(content)
        if already_collected(manifest_path, sha256) and dest.is_file():
            _registrar(status="ja_existe", sha256=sha256, bytes=len(content))
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        _registrar(status="ok", sha256=sha256, bytes=len(content))
