"""Manifesto (bronze/<fonte>/<data_base>/manifest.jsonl) e checkpoint incremental
por fonte, usados para tornar a ingestao idempotente (nao baixa de novo se o
sha256 do recurso já constar como coletado com sucesso)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class ManifestEntry:
    url: str
    data_base: str
    coletado_em: str
    status: str  # "ok" | "falha" | "ja_existe"
    sha256: str = ""
    bytes: int = 0
    arquivo: str = ""
    erro: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> "ManifestEntry":
        data = json.loads(line)
        return ManifestEntry(**data)


def append_entry(manifest_path: Path, entry: ManifestEntry) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as fh:
        fh.write(entry.to_json())
        fh.write("\n")


def read_entries(manifest_path: Path) -> list[ManifestEntry]:
    if not manifest_path.is_file():
        return []
    entries = []
    with manifest_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                entries.append(ManifestEntry.from_json(line))
    return entries


def ultimas_por_url(entries: list[ManifestEntry]) -> list[ManifestEntry]:
    """Estado atual: so a ultima entrada de cada URL (falhas antigas ja
    superadas por uma coleta bem-sucedida nao contam)."""
    return list({e.url: e for e in entries}.values())


def already_collected(manifest_path: Path, sha256: str) -> bool:
    """Verifica se um recurso com o mesmo sha256 ja foi coletado com sucesso."""
    if not sha256:
        return False
    return any(e.sha256 == sha256 and e.status == "ok" for e in read_entries(manifest_path))


@dataclass
class Checkpoint:
    fonte: str
    ultima_data_base_ok: str | None = None
    data_bases: dict[str, Any] = field(default_factory=dict)

    def marcar(self, data_base: str, *, arquivos_ok: int, arquivos_falha: int) -> None:
        self.data_bases[data_base] = {
            "arquivos_ok": arquivos_ok,
            "arquivos_falha": arquivos_falha,
            "atualizado_em": now_iso(),
        }
        if arquivos_falha == 0 and (
            self.ultima_data_base_ok is None or data_base > self.ultima_data_base_ok
        ):
            self.ultima_data_base_ok = data_base


def load_checkpoint(path: Path, fonte: str) -> Checkpoint:
    if not path.is_file():
        return Checkpoint(fonte=fonte)
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return Checkpoint(
        fonte=data.get("fonte", fonte),
        ultima_data_base_ok=data.get("ultima_data_base_ok"),
        data_bases=data.get("data_bases", {}),
    )


def save_checkpoint(path: Path, checkpoint: Checkpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(asdict(checkpoint), fh, ensure_ascii=False, indent=2)
