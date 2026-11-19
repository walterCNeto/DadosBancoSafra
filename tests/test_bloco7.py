"""Testes do Bloco 7 (snapshot publicavel e app em modo nuvem), sem rede: fixtures parquet do Bloco 5 e file://."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from safra_dados import snapshot as snap
from safra_dados.snapshot import SnapshotInvalido
from test_bloco5 import (
    _settings, silver_cosif, silver_estban, silver_fundos, silver_ifdata, silver_juros, silver_reclamacoes, silver_ses,
)

APP = Path(__file__).resolve().parents[1] / "app" / "app.py"


def _novo(st, rel: str) -> Path:
    p = st.silver_dir / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture
def st_app(tmp_path):
    st = _settings(tmp_path)
    for f in (silver_ifdata, silver_cosif, silver_juros, silver_reclamacoes, silver_estban, silver_fundos, silver_ses):
        f(st)
    # fonte extra: tabela com coluna grupo (uma linha fora de safra/pares) e tabela pequena sem grupo
    pl.DataFrame({"k": [1, 2, 3], "grupo": ["safra", "pares", "outros"]}).write_parquet(_novo(st, "teste_grupo/a.parquet"))
    pl.DataFrame({"serie": [1, 2], "valor": [1.5, 2.5]}).write_parquet(_novo(st, "bcb_sgs/b.parquet"))
    return st


@pytest.fixture
def gerado(st_app, monkeypatch):
    monkeypatch.setattr(snap, "LIMITE_TABELA_PEQUENA", 0)  # forca o filtro por grupo nas fixtures pequenas
    return snap.gerar_snapshot(st_app)


# ---------------------------------------------------------------- geracao

def test_gerar_snapshot_gold_silver_e_manifest(st_app, gerado):
    r = gerado
    rel = {(a["tipo"], a["relacao"]) for a in r.arquivos}
    assert {("gold", "fundos"), ("gold", "fundos_decomposicao"), ("gold", "fundos_conciliacao"), ("gold", "capital"),
            ("gold", "dim_entidade"), ("silver", "informe"), ("silver", "teste_grupo"), ("silver", "bcb_sgs")} <= rel
    m = json.loads((r.pasta / snap.MANIFEST).read_text(encoding="utf-8"))
    assert m["gerado_em"] and m["total_bytes"] == r.total_bytes == sum(a["bytes"] for a in m["arquivos"])
    assert isinstance(m["fontes"], list) and m["fontes"] and "ultima_data_base_ok" in m["fontes"][0]
    for a in m["arquivos"]:
        f = r.pasta / a["caminho"]
        assert f.stat().st_size == a["bytes"] and snap.sha256_arquivo(f) == a["sha256"] and len(a["sha256"]) == 64
    # silver com grupo: so safra/pares; sem grupo: completa
    con = duckdb.connect()
    grupos = {g for (g,) in con.execute(f"select grupo from read_parquet('{(r.pasta / 'silver/teste_grupo.parquet').as_posix()}')").fetchall()}
    assert grupos == {"safra", "pares"}
    assert con.execute(f"select count(*) from read_parquet('{(r.pasta / 'silver/bcb_sgs.parquet').as_posix()}')").fetchone()[0] == 2
    n_fundos = con.execute(f"select count(*) from read_parquet('{(r.pasta / 'gold/fundos.parquet').as_posix()}')").fetchone()[0]
    assert n_fundos > 0
    assert not st_app.gold_db_path.exists()  # nada e gravado fora de data/snapshot


def test_aviso_de_tamanho_e_maiores(gerado, monkeypatch):
    assert not gerado.acima_do_limite
    monkeypatch.setattr(snap, "AVISO_TOTAL_BYTES", 1)
    assert gerado.acima_do_limite
    assert [a["bytes"] for a in gerado.maiores(3)] == sorted((a["bytes"] for a in gerado.arquivos), reverse=True)[:3]


def test_cli_snapshot_avisa_acima_do_limite_e_exclui(st_app, monkeypatch):
    from safra_dados import cli

    monkeypatch.setattr(cli, "load_settings", lambda: st_app)
    monkeypatch.setattr(snap, "AVISO_TOTAL_BYTES", 1)
    silver = st_app.data_root / "snapshot" / "silver"
    r = CliRunner().invoke(cli.app, ["snapshot", "--excluir", "bcb_sgs"])
    assert r.exit_code == 0, r.output
    assert "AVISO" in r.output and "snapshot em" in r.output
    assert (silver / "ifdata.parquet").exists() and not (silver / "bcb_sgs.parquet").exists()
    assert not (silver / "cda.parquet").exists() and not (silver / "informe.parquet").exists()  # padrao
    assert (st_app.data_root / "snapshot" / "gold" / "fundos.parquet").exists()
    assert CliRunner().invoke(cli.app, ["snapshot", "--completo"]).exit_code == 0
    assert (silver / "cda.parquet").exists() and (silver / "informe.parquet").exists()


# ---------------------------------------------------------------- manifest / sha256

def test_validar_snapshot_ok_e_detecta_corrupcao(gerado):
    assert snap.validar_snapshot(gerado.pasta)["gerado_em"]
    alvo = gerado.pasta / "gold" / "capital.parquet"
    original = alvo.read_bytes()
    alvo.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))  # mesmo tamanho, 1 bit trocado
    with pytest.raises(SnapshotInvalido, match="sha256"):
        snap.validar_snapshot(gerado.pasta)
    alvo.write_bytes(original + b"x")
    with pytest.raises(SnapshotInvalido, match="tamanho"):
        snap.validar_snapshot(gerado.pasta)
    alvo.unlink()
    with pytest.raises(SnapshotInvalido, match="ausente"):
        snap.validar_snapshot(gerado.pasta)
    (gerado.pasta / snap.MANIFEST).write_text("{nao e json", encoding="utf-8")
    with pytest.raises(SnapshotInvalido, match="invalido"):
        snap.validar_snapshot(gerado.pasta)


def test_zip_roundtrip_via_file_url_e_views(gerado, tmp_path):
    zip_path = snap.compactar_snapshot(gerado.pasta, tmp_path / "snapshot.zip")
    assert zipfile.ZipFile(zip_path).namelist()[0] == snap.MANIFEST
    base = tmp_path / "tmp"
    base.mkdir()
    pasta, manifest = snap.baixar_e_extrair(zip_path.as_uri(), base=base)
    assert manifest["total_bytes"] == gerado.total_bytes and pasta.name == "dados"
    con, res = snap.registrar_views(pasta, manifest)
    assert {"fundos", "fundos_decomposicao", "fundos_conciliacao", "capital"} <= set(res.criadas)
    assert {"informe", "teste_grupo"} <= set(res.silver_views) and not res.erros
    assert con.execute("select count(*) from gold.fundos").fetchone()[0] > 0
    assert con.execute("select count(*) from silver.bcb_sgs").fetchone()[0] == 2


def test_baixar_e_extrair_rejeita_zip_adulterado_e_limpa(gerado, tmp_path):
    ruim = tmp_path / "ruim.zip"
    with zipfile.ZipFile(ruim, "w") as z:
        for f in gerado.pasta.rglob("*"):
            if f.is_file():
                z.writestr(f.relative_to(gerado.pasta).as_posix(),
                           b"corrompido" if f.name == "capital.parquet" else f.read_bytes())
    base = tmp_path / "tmp"
    base.mkdir()
    with pytest.raises(SnapshotInvalido):
        snap.baixar_e_extrair(ruim.as_uri(), base=base)
    assert list(base.iterdir()) == []  # diretorio temporario removido
    traversal = tmp_path / "pt.zip"
    with zipfile.ZipFile(traversal, "w") as z:
        z.writestr("../fora.txt", "x")
    with pytest.raises(SnapshotInvalido, match="insegura"):
        snap.baixar_e_extrair(traversal.as_uri(), base=base)
    assert not (tmp_path / "fora.txt").exists()


def test_url_do_snapshot_prioridade(monkeypatch):
    monkeypatch.delenv(snap.ENV_URL, raising=False)
    assert snap.url_do_snapshot() == snap.URL_PADRAO == (
        "https://github.com/walterCNeto/DadosBancoSafra/releases/download/dados-latest/snapshot.zip")
    assert snap.url_do_snapshot({snap.ENV_URL: "file:///segredo.zip"}) == "file:///segredo.zip"
    monkeypatch.setenv(snap.ENV_URL, "file:///env.zip")
    assert snap.url_do_snapshot({snap.ENV_URL: "file:///segredo.zip"}) == "file:///env.zip"

    class Quebrado:
        def get(self, _):
            raise FileNotFoundError("sem secrets.toml")

    monkeypatch.delenv(snap.ENV_URL)
    assert snap.url_do_snapshot(Quebrado()) == snap.URL_PADRAO


# ---------------------------------------------------------------- publicacao (gh simulado)

def test_publicar_zip_cria_release_e_substitui_asset(monkeypatch, tmp_path):
    chamadas: list[list[str]] = []

    class R:
        def __init__(self, rc=0):
            self.returncode, self.stderr = rc, ""

    def fake(args, capturar=True):
        chamadas.append(args)
        return R(1) if args[:2] == ["release", "view"] else R(0)

    monkeypatch.setattr(snap, "_gh", fake)
    z = tmp_path / "snapshot.zip"
    z.write_bytes(b"x")
    assert snap.publicar_zip(z) == snap.URL_PADRAO
    assert chamadas[2][:3] == ["release", "create", "dados-latest"]
    assert chamadas[3][:3] == ["release", "upload", "dados-latest"] and "--clobber" in chamadas[3]
    assert "walterCNeto/DadosBancoSafra" in chamadas[3]


def test_publicar_zip_exige_login(monkeypatch, tmp_path):
    class R:
        returncode, stderr = 1, ""

    monkeypatch.setattr(snap, "_gh", lambda *a, **k: R())
    with pytest.raises(RuntimeError, match="gh auth login"):
        snap.publicar_zip(tmp_path / "x.zip")


# ---------------------------------------------------------------- app em modo nuvem (AppTest, file://)

def test_app_modo_nuvem_le_snapshot_local(st_app, gerado, tmp_path, monkeypatch):
    pytest.importorskip("streamlit")
    pytest.importorskip("plotly")
    import streamlit as st
    from streamlit.testing.v1 import AppTest

    zip_path = snap.compactar_snapshot(gerado.pasta, tmp_path / "snapshot.zip")
    vazio = tmp_path / "sem_dados"  # sem silver => modo nuvem
    vazio.mkdir()
    monkeypatch.setenv("SAFRA_DADOS_CONFIG_DIR", str(st_app.config_dir))
    monkeypatch.setenv("SAFRA_DADOS_HOME", str(vazio))
    monkeypatch.setenv(snap.ENV_URL, zip_path.as_uri())
    st.cache_resource.clear()
    st.cache_data.clear()
    try:
        at = AppTest.from_file(str(APP), default_timeout=300).run()
        assert not at.exception, [e.value for e in at.exception]
        assert any("Snapshot dos dados gerado em" in i.value for i in at.info)
        assert any("Basileia" in m.label for m in at.metric)
        for pagina in ("pages/2_Capital.py", "pages/6_Fundos.py", "pages/10_Fontes_e_status.py"):
            at.switch_page(pagina).run()
            assert not at.exception, (pagina, [e.value for e in at.exception])
            assert not at.error, (pagina, [e.value for e in at.error])
        assert any("Snapshot dos dados gerado em" in i.value for i in at.info)  # Fontes e status
        at.switch_page("pages/9_Explorador.py").run()  # Explorador desabilitado na nuvem (fora da navegacao e inerte)
        assert not at.exception and not at.text_area and any("indisponível" in i.value for i in at.info)
        assert not (vazio / "exports").exists()
    finally:
        st.cache_resource.clear()
        st.cache_data.clear()
