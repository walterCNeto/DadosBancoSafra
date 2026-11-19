from __future__ import annotations

from safra_dados.manifest import (
    Checkpoint,
    ManifestEntry,
    already_collected,
    append_entry,
    load_checkpoint,
    read_entries,
    save_checkpoint,
)


def test_append_and_read_roundtrip(tmp_path):
    manifest_path = tmp_path / "manifest.jsonl"
    entry1 = ManifestEntry(
        url="http://x/1", data_base="202306", coletado_em="2024-01-01T00:00:00+00:00",
        status="ok", sha256="abc", bytes=10, arquivo="f1",
    )
    entry2 = ManifestEntry(
        url="http://x/2", data_base="202306", coletado_em="2024-01-01T00:00:01+00:00",
        status="falha", erro="boom",
    )
    append_entry(manifest_path, entry1)
    append_entry(manifest_path, entry2)

    entries = read_entries(manifest_path)
    assert len(entries) == 2
    assert entries[0].url == "http://x/1"
    assert entries[1].status == "falha"
    assert entries[1].erro == "boom"


def test_read_entries_arquivo_inexistente(tmp_path):
    assert read_entries(tmp_path / "nao-existe.jsonl") == []


def test_already_collected(tmp_path):
    manifest_path = tmp_path / "manifest.jsonl"
    append_entry(
        manifest_path,
        ManifestEntry(url="u", data_base="202306", coletado_em="t", status="ok", sha256="abc123"),
    )
    assert already_collected(manifest_path, "abc123") is True
    assert already_collected(manifest_path, "outro-hash") is False
    assert already_collected(manifest_path, "") is False


def test_already_collected_ignora_falhas(tmp_path):
    manifest_path = tmp_path / "manifest.jsonl"
    append_entry(
        manifest_path,
        ManifestEntry(url="u", data_base="202306", coletado_em="t", status="falha", sha256="abc123"),
    )
    assert already_collected(manifest_path, "abc123") is False


def test_checkpoint_roundtrip(tmp_path):
    path = tmp_path / "checkpoint.json"
    checkpoint = Checkpoint(fonte="bcb_ifdata")
    checkpoint.marcar("202303", arquivos_ok=5, arquivos_falha=0)
    checkpoint.marcar("202306", arquivos_ok=3, arquivos_falha=2)
    save_checkpoint(path, checkpoint)

    loaded = load_checkpoint(path, "bcb_ifdata")
    assert loaded.ultima_data_base_ok == "202303"
    assert loaded.data_bases["202306"]["arquivos_falha"] == 2


def test_checkpoint_sem_arquivo(tmp_path):
    checkpoint = load_checkpoint(tmp_path / "nao-existe.json", "bcb_cosif")
    assert checkpoint.ultima_data_base_ok is None
    assert checkpoint.data_bases == {}
