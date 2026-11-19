"""Testes do Bloco 4 (SUSEP SES e RI Safra) com fixtures, sem rede."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date

import polars as pl
import pytest

from safra_dados.connectors.safra_ri import (
    SafraRiConnector, classificar, extrair_data_base, parse_listagem,
)
from safra_dados.connectors.susep_ses import (
    SusepSesConnector, ler_tabela_grande, montar_universo, parse_lista_empresas, parse_pagina_ses,
)
from safra_dados.entidades import carregar_mapa_entidades, fips_susep, mapa_de_dicts
from safra_dados.http import DownloadResult, sha256_bytes
from safra_dados.manifest import read_entries
from safra_dados.resolver import resolver_entidades, sugerir_susep
from safra_dados.robots import RegrasRobots, carregar_robots

PAGINA_SES = "https://www2.susep.gov.br/menuestatistica/ses/principal.aspx"
HTML_SES = """<html><body>
<a href="download/LISTAEMPRESAS.csv">Lista</a>
<div id="GEEST"><a href="https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip">Base de Dados do SES, atualizada
at&eacute; <span id="ctl00_ContentPlaceHolder1_lblMesAnoCarga">202609</span></a>
(gerada em <span id="ctl00_ContentPlaceHolder1_lblDataCarga">16/11/2026 00:38:30</span>)</div></body></html>"""

LISTA_CSV = (
    "CodigoFIP;NomeEntidade;CNPJ\n"
    "01627;Safra Seguros Gerais S.A.;06109373000181\n"
    "09938;Safra Vida e Previdência S.A.;30902142000105\n"
    "05444;BRADESCO SEGUROS S.A;33055146000193\n"
    "06513;CHUBB SEGUROS BRASIL S.A.;03502099000118\n"
    "00001;OUTRA SEGURADORA;11111111000111\n"
).encode("cp1252")

ENTIDADES = {
    "grupos": {
        "safra": {"cnpj_raiz": "58160789", "entidades": {
            "banco_safra": {"cnpj_raiz": "58160789"},
            "seguradora_safra": {"busca_nome_susep": ["safra seguros", "safra vida"]},
        }},
        "pares": {"entidades": {
            "bradesco": {"cnpj_raiz": "60746948", "busca_nome_susep": ["bradesco"]},
            "bb": {"cnpj_raiz": "00000000", "busca_nome_susep": ["bb capitalizacao", "brasilseg"]},
        }},
    }
}
FONTES = {
    "susep_ses": {
        "pagina_url": PAGINA_SES, "desde": "2020-01",
        "tabelas_grandes": ["seguros", "provramos", "prov", "balanco", "pl_margem", "valores_mov_ramos", "uf"],
    },
    "safra_ri": {
        "base_url": "https://www.safra.com.br", "pagina_inicial": "/sobre/relacoes-com-investidores.htm",
        "prefixo_paginas": "/sobre/relacoes-com-investidores", "desde": "2023-01",
        "intervalo_min_segundos": 0, "max_paginas": 5, "cnpj_emissor": "58160789",
    },
}


class Resp:
    def __init__(self, corpo, status=200):
        self.status_code = status
        self.content = corpo if isinstance(corpo, bytes) else corpo.encode("utf-8")
        self.text = self.content.decode("utf-8", "replace")


class FakeClient:
    """GET por substring da URL (a mais especifica primeiro); `download_to_file` grava bytes."""

    def __init__(self, rotas):
        self.rotas, self.urls = rotas, []

    def get(self, url, params=None):
        self.urls.append(url)
        for trecho in sorted(self.rotas, key=len, reverse=True):
            if trecho in url:
                r = self.rotas[trecho]
                return r if isinstance(r, Resp) else Resp(r)
        return Resp("nao mockado", 404)

    def download_to_file(self, url, dest, params=None):
        r = self.get(url)
        if r.status_code != 200:
            return DownloadResult(0, "", "falha", f"HTTP {r.status_code}")
        dest.write_bytes(r.content)
        return DownloadResult(len(r.content), sha256_bytes(r.content), "ok")


def _settings(settings_factory, resolvidas=None):
    return settings_factory(ENTIDADES, entidades_resolvidas=resolvidas, fontes=FONTES)


def zip_de(membros: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for nome, conteudo in membros.items():
            zf.writestr(nome, conteudo)
    return buf.getvalue()


def csv_ses(linhas: list[str]) -> bytes:
    return ("\n".join(linhas) + "\n").encode("cp1252")


def base_ses() -> bytes:
    """Zip minimo no layout real: nomes com caixa variada, cp1252, `;`, virgula decimal, FIP com espacos."""
    return zip_de({
        "Ses_cias.csv": csv_ses([
            "Coenti;Noenti;Cogrupo;Nogrupo",
            "01627     ;SAFRA SEGUROS GERAIS S.A. ;     ;",
            "09938     ;SAFRA VIDA E PREVIDÊNCIA S.A. ;     ;",
            "05444     ;BRADESCO SEGUROS S.A ;     ;",
            "00001     ;OUTRA SEGURADORA ;     ;",
        ]),
        "Ses_ramos.csv": csv_ses(["coramo;noramo", "0977      ;0977 - Vida Individual", "0531      ;0531 - Automóvel"]),
        "ses_gruposramos.csv": csv_ses(["GRAID;GRANOME;GRACODIGO", "1;01 - Patrimonial;01"]),
        "Ses_campos.csv": csv_ses([
            "nuitem;noitem;nuquad;mercado;inivigencia;fimvigencia",
            "11155;  Caixa e Equivalentes;22A;;201106;210001",
            "3209;Provisões Técnicas;22P;;201106;210001",
        ]),
        "Ses_grupos_economicos.csv": csv_ses([
            "damesano;coenti;noenti;cogrupo;nogrupo",
            "201912;01627;SAFRA SEGUROS GERAIS;00345;SAFRA",
            "202608;01627;SAFRA SEGUROS GERAIS;00345;SAFRA",
            "202608;05444;BRADESCO SEGUROS;00027;BRADESCO",
        ]),
        "Ses_seguros.csv": csv_ses([
            "damesano;coenti;cogrupo;coramo;premio_direto;sinistro_direto;",
            "201912;01627;00345;0531;999,5;1;",           # antes de `desde`
            "202609;01627;00345;0531;1500,25;300,10;",
            "202609;05444;00027;0531;2000;400;",
            "202609;00001;00001;0531;7;7;",               # fora do universo
        ]),
        "ses_provramos.csv": csv_ses(["damesano;coenti;coramo;ppng", "202609;09938;0977;12,5"]),
        "Ses_prov.csv": csv_ses(["coenti;damesano;valor", "09938;202609;100,5"]),
        "SES_Balanco.csv": csv_ses([
            "coenti;damesano;cmpid;valor;seq;quadro",
            "01627;202609;11155;1234,56;470;22A ",
            "01627;202609;3209;99;60;22P ",
            "00001;202609;11155;5;470;22A ",
        ]),
        "Ses_pl_margem.csv": csv_ses(["coenti;damesano;plajustado;margem;pl", "05444;202609;10;5;12"]),
        "SES_ValoresMovRamos.csv": csv_ses([
            "coenti;damesano;cmpid;ramcodigo;gracodigo;valor;seq;quadro", "01627;202609;994;45;07;3,2;10000;2",
        ]),
        "SES_UF2.csv": csv_ses([
            "coenti;damesano;ramos;UF;premio_dir;premio_ret;sin_dir;prem_ret_liq;gracodigo;salvados;recuperacao",
            "01627;202609;0531;SP;10;9;1;8;05;0;0",
        ]),
    })


# ---------------------------------------------------------------- robots

ROBOTS_SAFRA = """User-agent: *
User-agent: GPTBot
Allow: /
Disallow: /data/
Disallow: /external-files/
Disallow: /safra-asset/*.pdf
Content-Signal: ai-train=no
Sitemap: https://www.safra.com.br/sitemap.xml
"""


def test_robots_maior_correspondencia_nao_primeira_regra():
    r = RegrasRobots.de_texto(ROBOTS_SAFRA)
    # `Allow: /` vem primeiro, mas `Disallow: /data/` e mais especifico
    assert not r.permitido("https://www.safra.com.br/data/files/00/Relatorio.pdf")
    assert not r.permitido("/external-files/x")
    assert not r.permitido("/safra-asset/folder/a.pdf")
    assert r.permitido("/safra-asset/folder/a.html")
    assert r.permitido("https://www.safra.com.br/sobre/relacoes-com-investidores.htm")


def test_robots_grupo_especifico_allow_empate_e_vazio():
    txt = "User-agent: safra-dados\nDisallow: /x\nAllow: /x/livre\n\nUser-agent: *\nDisallow: /\n"
    r = RegrasRobots.de_texto(txt, "safra-dados/0.1")
    assert not r.permitido("/x/a") and r.permitido("/x/livre/a") and r.permitido("/y")
    assert RegrasRobots.de_texto("User-agent: *\nDisallow:\n").permitido("/qualquer")
    assert not RegrasRobots.de_texto("User-agent: *\nDisallow: /*.zip$\n").permitido("/a/b.zip")


def test_carregar_robots_politica_de_falha():
    assert carregar_robots(FakeClient({"robots.txt": Resp("nope", 404)}), "https://h.com/x").permitido("/data/a")
    assert not carregar_robots(FakeClient({"robots.txt": Resp("err", 503)}), "https://h.com/x").permitido("/a")
    assert not carregar_robots(FakeClient({"robots.txt": Resp(ROBOTS_SAFRA)}), "https://h.com/x").permitido("/data/a")


# ---------------------------------------------------------------- SUSEP: parsing

def test_parse_pagina_ses_extrai_links_e_mes_de_carga():
    info = parse_pagina_ses(HTML_SES, PAGINA_SES)
    assert info["base_url"] == "https://www2.susep.gov.br/redarq.asp?arq=BaseCompleta%2ezip"
    assert info["lista_url"] == "https://www2.susep.gov.br/menuestatistica/ses/download/LISTAEMPRESAS.csv"
    assert info["mes_carga"] == "202609" and info["gerada_em"] == "16/11/2026 00:38:30"


def test_parse_lista_empresas_cp1252_e_fip_padronizado():
    df = parse_lista_empresas(LISTA_CSV.replace(b"01627", b" 1627"))
    assert df["codigo_fip"][0] == "01627" and df["cnpj"][0] == "06109373000181"
    assert "Previdência" in df["nome"][1]


def test_ler_tabela_grande_filtra_converte_virgula_e_descarta_coluna_sem_nome(tmp_path):
    z = zipfile.ZipFile(io.BytesIO(base_ses()))
    p = tmp_path / "seguros.csv"
    p.write_bytes(z.read("Ses_seguros.csv"))
    df = ler_tabela_grande(p, ("damesano", "coenti", "cogrupo", "coramo"), {"01627", "05444"}, "202001")
    assert df.height == 2  # 201912 e o FIP fora do universo ficaram de fora
    linha = df.filter(pl.col("codigo_fip") == "01627").row(0, named=True)
    assert linha["premio_direto"] == 1500.25 and linha["sinistro_direto"] == 300.10
    assert linha["periodo"] == date(2026, 9, 1) and linha["coramo"] == "0531"
    assert all(c.strip() for c in df.columns)


# ---------------------------------------------------------------- sugestoes / mapa

def test_sugerir_susep_por_nome_por_cnpj_e_sem_falso_positivo():
    lista = parse_lista_empresas(LISTA_CSV).to_dicts()
    cfg = ENTIDADES["grupos"]
    seg = sugerir_susep(cfg["safra"]["entidades"]["seguradora_safra"], {}, lista, set())
    assert [i["codigo_fip"] for i in seg] == ["01627", "09938"] and not any(i["aprovado"] for i in seg)
    assert seg[0]["origem"] == "nome" and seg[0]["cnpj_raiz"] == "06109373"
    # "bb seguros" casaria com CHUBB SEGUROS: os termos do bloco sao mais especificos
    assert sugerir_susep(cfg["pares"]["entidades"]["bb"], {}, lista, set()) == []
    por_cnpj = sugerir_susep({"cnpj_raiz": "33055146"}, {}, lista, {"33055146"})
    assert por_cnpj[0]["origem"] == "identificador" and por_cnpj[0]["no_mapa"] is True
    # sem `busca_nome_susep` so casa por CNPJ
    assert sugerir_susep({"cnpj_raiz": "58160789"}, {}, lista, set()) == []


RESOLVIDAS = {"susep": {"sugestoes": {
    "safra": {"seguradora_safra": [
        {"codigo_fip": "01627", "cnpj": "06109373000181", "cnpj_raiz": "06109373", "aprovado": True},
        {"codigo_fip": "09938", "cnpj": "30902142000105", "cnpj_raiz": "30902142", "aprovado": False},
    ]},
    "pares": {"bradesco": [{"codigo_fip": "05444", "cnpj": "33055146000193", "cnpj_raiz": "33055146", "aprovado": False}]},
}}}


def test_mapa_e_fips_so_com_sugestao_susep_aprovada():
    mapa = mapa_de_dicts(ENTIDADES, RESOLVIDAS)
    assert mapa.grupo_de("06109373000181") == "safra" and mapa.grupo_de("30902142000105") is None
    assert fips_susep(RESOLVIDAS, True) == {"01627": "safra"}
    assert fips_susep(RESOLVIDAS, False) == {"01627": "safra", "09938": "safra", "05444": "pares"}


def test_montar_universo_grupo_so_aprovado_e_candidatos_com_grupo_sugerido():
    mapa = mapa_de_dicts(ENTIDADES, RESOLVIDAS)
    cad = parse_lista_empresas(LISTA_CSV)
    u = montar_universo(cad, mapa, fips_susep(RESOLVIDAS, True), fips_susep(RESOLVIDAS, False)).sort("codigo_fip")
    assert u["codigo_fip"].to_list() == ["01627", "05444", "09938"]  # 00001 e CHUBB ficam de fora
    assert u["grupo"].to_list() == ["safra", None, None]
    assert u["is_safra"].to_list() == [True, False, False]
    assert u["grupo_sugerido"].to_list() == ["safra", "pares", "safra"]


def test_resolver_grava_secao_susep_e_preserva_aprovado(settings_factory):
    st = _settings(settings_factory)
    lista = parse_lista_empresas(LISTA_CSV).to_dicts()
    res = resolver_entidades(st, anomes="202606", cadastro=[], incluir_cvm=False, lista_susep=lista)
    itens = res["susep"]["sugestoes"]["safra"]["seguradora_safra"]
    assert {i["codigo_fip"] for i in itens} == {"01627", "09938"}
    # revisao humana + nova execucao
    import yaml
    caminho = st.entidades_resolvidas_path()
    dados = yaml.safe_load(caminho.read_text(encoding="utf-8"))
    for i in dados["susep"]["sugestoes"]["safra"]["seguradora_safra"]:
        i["aprovado"] = i["codigo_fip"] == "01627"
    caminho.write_text(yaml.safe_dump(dados), encoding="utf-8")
    res2 = resolver_entidades(st, anomes="202606", cadastro=[], incluir_cvm=False, lista_susep=lista)
    aprov = {i["codigo_fip"]: i["aprovado"] for i in res2["susep"]["sugestoes"]["safra"]["seguradora_safra"]}
    assert aprov == {"01627": True, "09938": False}
    assert carregar_mapa_entidades(st).grupo_de("06109373000181") == "safra"
    # sem consultar a SUSEP, a secao anterior e preservada
    res3 = resolver_entidades(st, anomes="202606", cadastro=[], incluir_cvm=False, incluir_susep=False)
    assert res3["susep"]["sugestoes"]["safra"]["seguradora_safra"]


# ---------------------------------------------------------------- SUSEP: conector

def _conector_ses(settings_factory, resolvidas=RESOLVIDAS, robots=None):
    st = _settings(settings_factory, resolvidas)
    cliente = FakeClient({
        "robots.txt": Resp("nao ha", 404) if robots is None else Resp(robots),
        "principal.aspx": HTML_SES,
        "LISTAEMPRESAS": LISTA_CSV,
        "BaseCompleta": base_ses(),
    })
    return st, SusepSesConnector(st, client=cliente, hoje=date(2026, 11, 19))


def test_ses_discover_usa_a_pagina_e_robots(settings_factory):
    st, c = _conector_ses(settings_factory)
    recursos = c.discover("2020-01")
    assert {r.extra["chave"] for r in recursos} == {"lista", "base"}
    assert {r.data_base for r in recursos} == {"202609"}  # mes de carga da pagina, nao da coleta
    assert not c.falhas_discover
    st, c = _conector_ses(settings_factory, robots="User-agent: *\nDisallow: /redarq.asp\n")
    assert [r.extra["chave"] for r in c.discover("2020-01")] == ["lista"] and c.falhas_discover


def test_ses_discover_registra_falha_se_pagina_mudou(settings_factory):
    st = _settings(settings_factory)
    c = SusepSesConnector(st, client=FakeClient({"robots.txt": Resp("x", 404), "principal.aspx": "<html>sem links</html>"}))
    assert c.discover("2020-01") == [] and "pagina do SES mudou" in c.falhas_discover[0]


def test_ses_fetch_incremental_e_normalize_completo(settings_factory):
    st, c = _conector_ses(settings_factory)
    for r in c.discover("2020-01"):
        c.fetch(r)
    manifest = read_entries(st.manifest_path("susep_ses", "202609"))
    assert [e.status for e in manifest] == ["ok", "ok"]
    # segunda execucao: mesmo sha256 -> ja_existe, sem sobrescrever
    for r in c.discover("2020-01"):
        c.fetch(r)
    assert [e.status for e in read_entries(st.manifest_path("susep_ses", "202609"))][2:] == ["ja_existe", "ja_existe"]
    assert not list(st.bronze_fonte_dir("susep_ses", "202609").glob("*.novo"))

    assert c.normalize("202609")
    base = st.silver_fonte_dir("susep_ses")
    ler = lambda t: pl.read_parquet(base / t / "202609.parquet")  # noqa: E731

    cad = ler("cadastro").sort("codigo_fip")
    assert cad["codigo_fip"].to_list() == ["00001", "01627", "05444", "06513", "09938"]  # universo completo
    assert cad.filter(pl.col("codigo_fip") == "01627").row(0, named=True)["nogrupo_susep"] == "SAFRA"
    assert cad.filter(pl.col("codigo_fip") == "01627")["grupo"][0] == "safra"  # FIP aprovado + CNPJ no mapa
    assert cad.filter(pl.col("codigo_fip") == "09938")["grupo"][0] is None     # candidato nao aprovado
    assert ler("ramos").height == 2 and ler("ramos")["coramo"][0] == "0977"
    ge = ler("grupos_economicos")
    assert ge.height == 2 and "201912" not in ge["damesano"].to_list()  # filtrado por `desde`

    seg = ler("seguros")
    assert sorted(seg["codigo_fip"].to_list()) == ["01627", "05444"]
    assert seg.filter(pl.col("codigo_fip") == "01627").row(0, named=True)["premio_direto"] == 1500.25
    por_fip = {r["codigo_fip"]: r for r in seg.iter_rows(named=True)}
    assert (por_fip["01627"]["grupo"], por_fip["01627"]["is_safra"]) == ("safra", True)
    assert (por_fip["05444"]["grupo"], por_fip["05444"]["grupo_sugerido"]) == (None, "pares")

    bal = ler("balanco").sort("cmpid")
    assert bal.height == 2 and bal.filter(pl.col("cmpid") == "11155")["noitem"][0] == "Caixa e Equivalentes"
    for t in ("provramos", "prov", "pl_margem", "valores_mov_ramos", "uf"):
        assert ler(t).height == 1, t
    assert not list(st.data_root.glob("tmp_ses_*"))  # extracao temporaria removida


def test_ses_reprocessa_silver_apos_aprovar_sem_baixar_de_novo(settings_factory):
    import yaml
    st, c = _conector_ses(settings_factory)
    for r in c.discover("2020-01"):
        c.fetch(r)
    c.normalize("202609")
    dados = yaml.safe_load(st.entidades_resolvidas_path().read_text(encoding="utf-8"))
    for i in dados["susep"]["sugestoes"]["pares"]["bradesco"]:
        i["aprovado"] = True
    st.entidades_resolvidas_path().write_text(yaml.safe_dump(dados), encoding="utf-8")
    c2 = SusepSesConnector(st, client=None)  # sem cliente: normalize so le a bronze
    c2.normalize("202609")
    seg = pl.read_parquet(st.silver_fonte_dir("susep_ses") / "seguros" / "202609.parquet")
    assert seg.filter(pl.col("codigo_fip") == "05444")["grupo"][0] == "pares"


# ---------------------------------------------------------------- RI Safra

PAGINA_RI = "https://www.safra.com.br/sobre/relacoes-com-investidores/informacoes-financeiras.htm"
HTML_RI = """<html><body>
<a href="/sobre/relacoes-com-investidores/informacoes-de-governanca.htm">Governanca</a>
<a href="/sobre/relacoes-com-investidores/informacoes-de-governanca/Politica.pdf">pdf, nao e pagina</a>
<a href="/outra/pagina.htm">fora do prefixo</a>
<a class="acc-toggle" data-target="x"><p class="titulo">Demonstrações Contábeis Consolidadas</p></a>
<div class="doc 2026" data-month="09"><div class="tbody"><div class="s-col t-wrap">
<p class="titulo" style="padding-left: 0rem;"> Demonstrações Contábeis Consolidadas em IFRS - set 2026<span class="badge pdf">PDF</span>
<a class="dl-mobile" href="../../data/files/60/D6/IFRS%2009.2026%20VF.pdf">Download</a></p></div>
<div class="dl-wrap"><p><a href="../../data/files/60/D6/IFRS%2009.2026%20VF.pdf">Download</a></p></div></div></div>
<a class="acc-toggle" data-target="y"><p class="titulo">Relatório de Gerenciamento de Riscos</p></a>
<div class="doc 2023" data-month="05"><div class="tbody"><div class="s-col t-wrap">
<h3 class="titulo recuado">Relatório de Gerenciamento de Riscos - Pilar 3 – 1T23<span class="badge pdf">PDF</span>
<a class="dl-mobile" href="../../data/files/03/23/Pilar%203%201T23.pdf">Download</a></h3>
<h3 class="titulo recuado">Anexo Pilar 3 - Tabelas 1T23<span class="badge excel">Excel</span>
<a class="dl-mobile" href="../../data/files/21/23/Tabelas%201T23.xlsx">Download</a></h3></div></div></div>
<a class="acc-toggle" data-target="z"><p class="titulo">Relatório Anual</p></a>
<div class="doc 2025" data-month="12"><p class="titulo"> Relatório Anual 2025<span class="badge pdf">PDF</span>
<a class="dl-mobile" href="../../data/files/2D/Relatorio%20Anual%202025.pdf">Download</a></p></div>
<div class="doc 2019" data-month="03"><p class="titulo"> Relatório Anual 2018<span class="badge pdf">PDF</span>
<a class="dl-mobile" href="../../data/files/AA/Relatorio%20Anual%202018.pdf">Download</a></p></div>
<a class="acc-toggle" data-target="w"><p class="titulo">Letra Imobiliária Garantida (LIG)</p></a>
<div class="doc 2026" data-month="07"><p class="titulo"> DCA Julho 2026<span class="badge pdf">PDF</span>
<a class="dl-mobile" href="../../data/files/1E/DCA.pdf">Download</a></p></div>
</body></html>"""
HTML_GOV = "<html><body><p>sem documentos</p></body></html>"
HOME_RI = '<html><a href="/sobre/relacoes-com-investidores/informacoes-financeiras.htm">Info</a></html>'


def test_parse_listagem_documentos_secao_formato_e_links():
    docs, links = parse_listagem(HTML_RI, PAGINA_RI)
    assert len(docs) == 6  # um por titulo, sem duplicar o link `dl-wrap`
    d = docs[0]
    assert d.url == "https://www.safra.com.br/data/files/60/D6/IFRS%2009.2026%20VF.pdf"
    assert (d.formato, d.secao, d.ano_publicacao, d.mes_publicacao) == ("pdf", "Demonstrações Contábeis Consolidadas", 2026, 9)
    assert {x.formato for x in docs} == {"pdf", "excel"}
    assert "https://www.safra.com.br/sobre/relacoes-com-investidores/informacoes-de-governanca.htm" in links


@pytest.mark.parametrize("titulo,secao,url,tipo", [
    ("Demonstrações Contábeis Consolidadas em IFRS - set 2026", "Demonstrações Contábeis Consolidadas", "/data/files/a/x.pdf", "demonstracoes_ifrs"),
    ("IFRS Consolidated Financial Statements - mar 2025", "Consolidated Financial Statements", "/data/files/a/x.pdf", "demonstracoes_ifrs"),
    ("Demonstrações Contábeis Consolidadas - jun 2017", "Demonstrações Contábeis - Conglomerado Prudencial", "/d/x.pdf", "demonstracoes_brgaap"),
    ("Resumo Consolidado e Principais Indicadores - set 2026", "Resumo das Demonstrações Contábeis Consolidadas", "/d/x.pdf", "demonstracoes_brgaap_resumo"),
    ("Relatório de Gerenciamento de Riscos - Pilar 3 – 2T25", "x", "/d/x.pdf", "pilar3_gir"),
    ("Anexo Pilar 3 - Tabelas 4T25", "x", "/d/x.xlsx", "pilar3_gir"),
    ("Relatório Anual 2025", "Relatório Anual", "/d/x.pdf", "relatorio_anual"),
    ("Annual Report 2024", "Annual Report", "/d/x.pdf", "relatorio_anual"),
    ("Relatório de Sustentabilidade 2023", "x", "/d/x.pdf", "outros"),
    ("DCA Julho 2026", "Letra Imobiliária Garantida (LIG)", "/d/x.pdf", "outros"),
])
def test_classificar_tipo(titulo, secao, url, tipo):
    assert classificar(titulo, secao, url) == tipo


@pytest.mark.parametrize("titulo,url,esperado", [
    ("Pilar 3 – 1T23", "/d/a.pdf", "202303"),
    ("Pilar 3 – 4T 2025", "/d/a.pdf", "202512"),
    ("IFRS - set 2026", "/d/a.pdf", "202609"),
    ("Demonstrações - dez 2025", "/d/a.pdf", "202512"),
    ("Relatório Anual 2025", "/d/a.pdf", "202512"),
    ("Balanço", "/d/Demonstracoes%20Prudencial%20-%2006_2020.pdf", "202006"),
    ("DCA Junho 2026", "/d/a.pdf", "202606"),
    ("Tabelas 1S20", "/d/a.xlsx", "202006"),
])
def test_extrair_data_base(titulo, url, esperado):
    assert extrair_data_base(titulo, url, 2030, 1)[0] == esperado


def test_extrair_data_base_cai_na_publicacao():
    assert extrair_data_base("Sem pista", "/d/a.pdf", 2024, 8) == ("202408", "publicacao")


def _conector_ri(settings_factory, robots):
    st = _settings(settings_factory)
    cliente = FakeClient({
        "robots.txt": Resp(robots), "relacoes-com-investidores.htm": HOME_RI,
        "informacoes-financeiras.htm": HTML_RI, "informacoes-de-governanca.htm": HTML_GOV,
        "/data/files/60/D6/": b"%PDF ifrs", "/data/files/03/23/": b"%PDF pilar", "/data/files/21/23/": b"xlsx pilar",
        "/data/files/2D/": b"%PDF anual", "/data/files/1E/": b"%PDF dca",
    })
    return st, cliente, SafraRiConnector(st, client=cliente, hoje=date(2026, 11, 19))


def test_ri_robots_bloqueia_arquivos_em_data_mas_cataloga_metadados(settings_factory):
    st, cliente, c = _conector_ri(settings_factory, ROBOTS_SAFRA)
    recursos = c.discover("2023-01")
    docs = [r for r in recursos if r.extra["kind"] == "documento"]
    assert len(docs) == 5 and not c.falhas_discover  # 2018 ficou de fora (desde 2023-01); governanca sem docs
    assert len(c.paginas_descobertas) == 3
    assert not any(r.extra["robots_permitido"] for r in docs)
    for r in recursos:
        c.fetch(r)
    assert not any("/data/files/" in u for u in cliente.urls)  # NENHUMA requisicao a /data/
    assert not list(st.bronze_fonte_dir("safra_ri").rglob("doc__*"))

    for db in sorted({r.data_base for r in docs}):
        assert c.normalize(db)
    silver = pl.concat([pl.read_parquet(p) for p in (st.silver_fonte_dir("safra_ri")).glob("*.parquet")])
    assert silver.height == 5 and silver["baixado"].sum() == 0
    assert set(silver["status"]) == {"bloqueado_robots"}
    assert set(silver["grupo"]) == {"safra"} and set(silver["is_safra"]) == {True}
    assert set(silver["tipo"]) == {"demonstracoes_ifrs", "pilar3_gir", "relatorio_anual", "outros"}
    ifrs = silver.filter(pl.col("tipo") == "demonstracoes_ifrs").row(0, named=True)
    assert ifrs["data_base"] == "202609" and ifrs["titulo"].endswith("set 2026") and ifrs["sha256"] is None
    # paginas de listagem (permitidas) foram para a bronze, com hash
    paginas = list(st.bronze_fonte_dir("safra_ri").rglob("pagina__*"))
    assert len(paginas) == 3
    manifest = read_entries(st.manifest_path("safra_ri", "202611"))
    assert all(e.status in ("ok", "ja_existe") and e.sha256 for e in manifest)


def test_ri_baixa_documentos_se_robots_permitir_e_e_incremental(settings_factory):
    st, cliente, c = _conector_ri(settings_factory, "User-agent: *\nAllow: /\n")
    for _ in range(2):
        c._html_pendente.clear()
        for r in c.discover("2023-01"):
            c.fetch(r)
    arquivos = {p.name: p for p in st.bronze_fonte_dir("safra_ri").rglob("doc__*")}
    assert set(arquivos) == {
        "doc__demonstracoes_ifrs__IFRS 09.2026 VF.pdf", "doc__pilar3_gir__Pilar 3 1T23.pdf",
        "doc__pilar3_gir__Tabelas 1T23.xlsx", "doc__relatorio_anual__Relatorio Anual 2025.pdf",
    }  # "outros" (DCA) nao e baixado
    e = {x.url: x for x in read_entries(st.manifest_path("safra_ri", "202609"))}
    url_ifrs = "https://www.safra.com.br/data/files/60/D6/IFRS%2009.2026%20VF.pdf"
    estados = [x.status for x in read_entries(st.manifest_path("safra_ri", "202609")) if x.url == url_ifrs]
    assert estados == ["ok", "ja_existe"]
    c.normalize("202609")
    s = pl.read_parquet(st.silver_fonte_dir("safra_ri") / "202609.parquet").row(0, named=True)
    assert s["baixado"] and s["sha256"] == sha256_bytes(b"%PDF ifrs") and s["bytes"] == len(b"%PDF ifrs")
    c.normalize("202607")
    dca = pl.read_parquet(st.silver_fonte_dir("safra_ri") / "202607.parquet").row(0, named=True)
    assert dca["status"] == "nao_baixado" and dca["tipo"] == "outros"


def test_ri_sem_robots_confiavel_nao_coleta_nada(settings_factory):
    st = _settings(settings_factory)
    c = SafraRiConnector(st, client=FakeClient({"robots.txt": Resp("erro", 503)}))
    assert c.discover("2023-01") == [] and "robots.txt indisponivel" in c.falhas_discover[0]
