"""Testes do Bloco 4.1 (SES incremental, reprocessar, importacao manual do RI), sem rede."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import yaml

from safra_dados.connectors.safra_ri import SafraRiConnector
from safra_dados.connectors.susep_ses import SusepSesConnector
from safra_dados.importacao_manual import casar, carregar_catalogo, importar_manual, normalizar_nome
from safra_dados.manifest import read_entries
from safra_dados.reprocessar import data_bases_bronze, reprocessar_fonte
from test_bloco4 import ROBOTS_SAFRA, _conector_ri, _conector_ses

RAIZ = Path(__file__).resolve().parent.parent

APROVADOS = {
    "01627", "09938",
    "05321", "05096", "06912", "05444", "06866", "05312", "23566", "06564", "05070", "01589", "29319",
    "02305", "02682", "06785", "04707", "20141", "06211",
}
NAO_APROVADOS = {"03182", "74934", "37052"}


class SemRede:
    """Qualquer uso de rede quebra o teste."""

    def __getattr__(self, nome):
        raise AssertionError(f"acesso a rede ({nome}) em fluxo que deveria ser offline")


def test_config_real_aprova_exatamente_os_fips_susep_combinados():
    dados = yaml.safe_load((RAIZ / "config" / "entidades_resolvidas.yaml").read_text(encoding="utf-8"))
    itens = [
        (str(i["codigo_fip"]).zfill(5), i["aprovado"])
        for por_ent in dados["susep"]["sugestoes"].values() for itens in por_ent.values() for i in itens
    ]
    aprovados = {f for f, a in itens if a}
    assert aprovados == APROVADOS
    assert NAO_APROVADOS <= {f for f, a in itens if not a}


# ---------------------------------------------------------------- SES incremental

def test_ses_nao_baixa_zip_de_novo_se_ha_bronze_para_o_mes_de_carga(settings_factory):
    st, c = _conector_ses(settings_factory)
    for r in c.discover("2020-01"):
        c.fetch(r)
    cliente = c._client
    baixou = lambda: sum("redarq.asp" in u for u in cliente.urls)  # noqa: E731
    assert baixou() == 1
    zip_path = next(st.bronze_fonte_dir("susep_ses", "202609").glob("base__*.zip"))
    mtime = zip_path.stat().st_mtime_ns
    # segunda execucao: a pagina e lida (mes de carga), mas o zip nao e baixado
    for r in c.discover("2020-01"):
        c.fetch(r)
    assert baixou() == 1
    assert zip_path.stat().st_mtime_ns == mtime
    base = [e for e in read_entries(st.manifest_path("susep_ses", "202609")) if e.url.endswith("BaseCompleta%2ezip")]
    assert [e.status for e in base] == ["ok", "ja_existe"] and base[0].sha256 == base[1].sha256
    # mes de carga novo -> baixa
    novo = [r for r in c.discover("2020-01") if r.extra["chave"] == "base"][0]
    novo.data_base = "202610"
    st.bronze_fonte_dir("susep_ses", "202610").mkdir(parents=True)
    c.fetch(novo)
    assert baixou() == 2


# ---------------------------------------------------------------- reprocessar

def test_reprocessar_refaz_silver_so_do_bronze_com_entidades_aprovadas(settings_factory):
    st, c = _conector_ses(settings_factory)
    for r in c.discover("2020-01"):
        c.fetch(r)
    c.normalize("202609")
    caminho = st.silver_fonte_dir("susep_ses") / "seguros" / "202609.parquet"
    assert pl.read_parquet(caminho).filter(pl.col("codigo_fip") == "05444")["grupo"][0] is None

    dados = yaml.safe_load(st.entidades_resolvidas_path().read_text(encoding="utf-8"))
    for i in dados["susep"]["sugestoes"]["pares"]["bradesco"]:
        i["aprovado"] = True
    st.entidades_resolvidas_path().write_text(yaml.safe_dump(dados), encoding="utf-8")

    assert data_bases_bronze(st, "susep_ses") == ["202609"]
    res = reprocessar_fonte(st, SusepSesConnector(st, client=SemRede()), "susep_ses")
    assert [(r.data_base, r.erro) for r in res] == [("202609", None)] and res[0].silver
    assert pl.read_parquet(caminho).filter(pl.col("codigo_fip") == "05444")["grupo"][0] == "pares"


def test_reprocessar_fonte_sem_bronze_nao_faz_nada(settings_factory):
    st, _ = _conector_ses(settings_factory)
    assert reprocessar_fonte(st, SusepSesConnector(st, client=SemRede()), "susep_ses") == []


def test_reprocessar_reporta_erro_por_data_base_sem_abortar(settings_factory):
    st, c = _conector_ses(settings_factory)
    for r in c.discover("2020-01"):
        c.fetch(r)
    (st.bronze_fonte_dir("susep_ses", "202610")).mkdir(parents=True)
    (st.bronze_fonte_dir("susep_ses", "202610") / "base__BaseCompleta.zip").write_bytes(b"nao e zip")
    res = reprocessar_fonte(st, SusepSesConnector(st, client=SemRede()), "susep_ses")
    por_db = {r.data_base: r for r in res}
    assert por_db["202609"].erro is None and por_db["202609"].silver
    assert por_db["202610"].erro and "BadZipFile" in por_db["202610"].erro


# ---------------------------------------------------------------- importacao manual

def _catalogo_ri(settings_factory):
    st, cliente, c = _conector_ri(settings_factory, ROBOTS_SAFRA)
    for r in c.discover("2023-01"):
        c.fetch(r)
    for db in sorted({r.data_base for r in c.discover("2023-01")}):
        c.normalize(db)
    return st, cliente


def test_normalizar_nome_e_casar():
    assert normalizar_nome("Pilar 3 1T23 (1).pdf") == normalizar_nome("Pilar%203 1T23.PDF".replace("%20", " "))
    assert normalizar_nome("Demonstrações_IFRS.pdf") == "demonstracoes ifrs"


def test_importar_manual_casa_copia_e_atualiza_manifest_sem_rede(settings_factory):
    st, cliente = _catalogo_ri(settings_factory)
    n_requisicoes = len(cliente.urls)
    pasta = st.manual_dir / "safra_ri"
    pasta.mkdir(parents=True)
    (pasta / "IFRS 09.2026 VF.pdf").write_bytes(b"%PDF ifrs manual")            # nome igual ao original
    (pasta / "Pilar 3 1T23 (1).pdf").write_bytes(b"%PDF pilar manual")          # sufixo de download repetido
    (pasta / "Relatorio_Anual_2025.pdf").write_bytes(b"%PDF anual manual")      # so o titulo/nome normalizado
    (pasta / "desconhecido.pdf").write_bytes(b"%PDF ???")                       # sem correspondencia
    (pasta / "leiame.txt").write_text("ignorado")

    res = {r.arquivo: r for r in importar_manual(st, "safra_ri")}
    assert res["IFRS 09.2026 VF.pdf"].estado == "importado" and res["IFRS 09.2026 VF.pdf"].data_base == "202609"
    assert res["Pilar 3 1T23 (1).pdf"].estado == "importado" and res["Pilar 3 1T23 (1).pdf"].data_base == "202303"
    assert res["Relatorio_Anual_2025.pdf"].estado == "importado"
    assert res["desconhecido.pdf"].estado == "pendente" and "--tipo" in res["desconhecido.pdf"].detalhe
    assert "leiame.txt" not in res
    assert len(cliente.urls) == n_requisicoes  # nenhuma requisicao

    destino = st.bronze_fonte_dir("safra_ri", "202609") / "doc__demonstracoes_ifrs__IFRS 09.2026 VF.pdf"
    assert destino.read_bytes() == b"%PDF ifrs manual"
    ultima = read_entries(st.manifest_path("safra_ri", "202609"))[-1]
    assert ultima.status == "importado_manual" and ultima.arquivo == str(destino)
    import hashlib
    assert ultima.sha256 == hashlib.sha256(b"%PDF ifrs manual").hexdigest()

    # silver reflete o arquivo importado; sem importacao o item seguiria bloqueado
    conector = SafraRiConnector(st, client=SemRede(), hoje=date(2026, 11, 19))
    conector.normalize("202609")
    s = pl.read_parquet(st.silver_fonte_dir("safra_ri") / "202609.parquet").filter(pl.col("tipo") == "demonstracoes_ifrs").row(0, named=True)
    assert s["baixado"] and s["status"] == "importado_manual" and s["sha256"] == ultima.sha256

    # idempotente
    res2 = {r.arquivo: r.estado for r in importar_manual(st, "safra_ri")}
    assert res2["IFRS 09.2026 VF.pdf"] == "ja_importado"


def test_importar_manual_sem_correspondencia_usa_tipo_e_data_base(settings_factory):
    st, _ = _catalogo_ri(settings_factory)
    pasta = st.manual_dir / "safra_ri"
    pasta.mkdir(parents=True)
    (pasta / "balanco avulso.pdf").write_bytes(b"%PDF avulso")
    res = importar_manual(st, "safra_ri", tipo="demonstracoes_brgaap", data_base="2026-06")
    assert [(r.estado, r.data_base, r.tipo) for r in res] == [("importado", "202606", "demonstracoes_brgaap")]
    cat = [i for i in carregar_catalogo(st) if i.data_base == "202606"]
    assert len(cat) == 1 and cat[0].url.startswith("manual://safra_ri/202606/")
    SafraRiConnector(st, client=SemRede(), hoje=date(2026, 11, 19)).normalize("202606")
    s = pl.read_parquet(st.silver_fonte_dir("safra_ri") / "202606.parquet").row(0, named=True)
    assert s["baixado"] and s["tipo"] == "demonstracoes_brgaap" and s["grupo"] == "safra"
    import pytest
    with pytest.raises(ValueError):
        importar_manual(st, "safra_ri", tipo="invalido", data_base="2026-06")
    with pytest.raises(ValueError):
        importar_manual(st, "safra_ri", tipo="outros", data_base="2026-13")


def test_casar_ambiguo_nao_casa(settings_factory):
    st, _ = _catalogo_ri(settings_factory)
    cat = carregar_catalogo(st)
    item, motivo = casar("Pilar 3.pdf", cat)
    assert item is None


def test_ingest_posterior_nao_desfaz_importacao_manual(settings_factory):
    st, cliente = _catalogo_ri(settings_factory)
    pasta = st.manual_dir / "safra_ri"
    pasta.mkdir(parents=True)
    (pasta / "IFRS 09.2026 VF.pdf").write_bytes(b"%PDF ifrs manual")
    importar_manual(st, "safra_ri")
    c = SafraRiConnector(st, client=cliente, hoje=date(2026, 11, 19))
    for r in c.discover("2023-01"):
        c.fetch(r)
    c.normalize("202609")
    s = pl.read_parquet(st.silver_fonte_dir("safra_ri") / "202609.parquet").filter(pl.col("tipo") == "demonstracoes_ifrs").row(0, named=True)
    assert s["status"] == "importado_manual" and s["baixado"]
    assert not any("/data/files/" in u for u in cliente.urls)
