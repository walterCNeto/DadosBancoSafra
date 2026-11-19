"""Testes do Bloco 3 (CVM) com fixtures, sem rede."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date

import polars as pl
import pytest

from safra_dados.connectors.base import iter_zip_members
from safra_dados.connectors.cvm_cadastros import CvmCadastrosConnector, linhas_para_busca
from safra_dados.connectors.cvm_cia_aberta import CvmDfpConnector, CvmIpeConnector
from safra_dados.connectors.cvm_comum import (
    expr_grupo, ler_csv_cvm, marcar_grupos_cvm, normalizar_cnpjs, tipar,
)
from safra_dados.connectors.cvm_fundos import (
    CvmFundosCadConnector, CvmFundosCdaConnector, CvmFundosInformeConnector, carregar_universo_fundos,
)
from safra_dados.connectors.cvm_ofertas import CvmOfertasConnector
from safra_dados.entidades import MapaEntidades, carregar_mapa_entidades, mapa_de_dicts
from safra_dados.manifest import read_entries
from safra_dados.resolver import resolver_entidades, sugerir_cvm

CKAN = "https://dados.cvm.gov.br/api/3/action"
DADOS = "https://dados.cvm.gov.br/dados"

ENTIDADES = {
    "grupos": {
        "safra": {"cnpj_raiz": "58160789", "entidades": {"banco_safra": {"cnpj_raiz": "58160789"}}},
        "pares": {"entidades": {"bb": {"cnpj_raiz": "00000000"}}},
    }
}
PACOTES = {
    "cia_aberta": "cia_aberta-cad", "intermed": "intermed-cad",
    "adm_cart": "adm_cart-cad", "coord_oferta": "coord_oferta-cad",
}
FONTES = {
    "cvm_cadastros": {"ckan_base_url": CKAN, "pacotes": PACOTES},
    "cvm_fundos_cad": {"ckan_base_url": CKAN, "pacote": "fi-cad"},
    "cvm_fundos_informe": {"ckan_base_url": CKAN, "pacote": "fi-doc-inf_diario"},
    "cvm_fundos_cda": {"ckan_base_url": CKAN, "pacote": "fi-doc-cda"},
    "cvm_ofertas": {"ckan_base_url": CKAN, "pacote": "oferta-distrib"},
    "cvm_dfp": {"ckan_base_url": CKAN, "pacote": "cia_aberta-doc-dfp"},
    "cvm_ipe": {"ckan_base_url": CKAN, "pacote": "cia_aberta-doc-ipe"},
}
HOJE = date(2026, 11, 19)


# ---------------------------------------------------------------- infraestrutura de teste

def csv_latin1(linhas: list[str]) -> bytes:
    return ("\n".join(linhas) + "\n").encode("latin-1")


def zip_de(membros: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for nome, conteudo in membros.items():
            zf.writestr(nome, conteudo)
    return buf.getvalue()


def pacote(*urls: str) -> dict:
    return {"success": True, "result": {"resources": [{"url": u, "format": "ZIP"} for u in urls]}}


class Resp:
    def __init__(self, corpo, status=200):
        self.status_code = status
        self.content = corpo if isinstance(corpo, bytes) else json.dumps(corpo).encode()
        self.text = self.content.decode("utf-8", "replace")

    def json(self):
        return json.loads(self.content)


class FakeClient:
    """GET por substring da URL; `exists` por conjunto de URLs; registra as chamadas."""

    def __init__(self, rotas=None, existentes=()):
        self.rotas, self.existentes, self.urls = rotas or {}, set(existentes), []

    def get(self, url, params=None):
        assert params is None
        self.urls.append(url)
        for trecho, resp in self.rotas.items():
            if trecho in url:
                return resp if isinstance(resp, Resp) else Resp(resp)
        return Resp({"erro": "nao mockado"}, 404)

    def exists(self, url):
        return url in self.existentes


def _settings(settings_factory, resolvidas=None):
    return settings_factory(ENTIDADES, entidades_resolvidas=resolvidas, fontes=FONTES)


def _bronze(settings, fonte, data_base, nome, conteudo: bytes):
    d = settings.bronze_fonte_dir(fonte, data_base)
    d.mkdir(parents=True, exist_ok=True)
    (d / nome).write_bytes(conteudo)


# ---------------------------------------------------------------- utilitarios

def test_iter_zip_members_filtra_e_barra_zip_slip():
    z = zip_de({"a.csv": b"1", "b.csv": b"2", "dmpl.csv": b"3"})
    assert [n for n, _ in iter_zip_members(z, lambda n: n != "dmpl.csv")] == ["a.csv", "b.csv"]
    with pytest.raises(ValueError, match="zip-slip"):
        list(iter_zip_members(zip_de({"../fora.csv": b"x"})))


def test_ler_csv_latin1_cnpj_e_tipagem_sem_perder_valores():
    raw = csv_latin1([
        "CNPJ_CIA;DENOM_SOCIAL;DT_REG;DT_OUTRA;VL_X;VL_Y",
        "00.000.000/0001-91;BANCO DO BRASIL S.A. - ÇÃO;2020-01-31;31/12/2019;1.5;1,5",
        "58.160.789/0001-28;SAFRA;;;2;3",
    ])
    df = tipar(normalizar_cnpjs(ler_csv_cvm(raw)), numericas=("vl_x", "vl_y"))
    assert df["cnpj_cia"].to_list() == ["00000000000191", "58160789000128"]
    assert "ÇÃO" in df["denom_social"][0]
    assert df.schema["dt_reg"] == pl.Date and df.schema["dt_outra"] == pl.Date  # ISO e dd/mm/aaaa
    assert df.schema["vl_x"] == pl.Float64
    assert df.schema["vl_y"] == pl.String and df["vl_y"].to_list() == ["1,5", "3"]  # cast perderia "1,5"


def test_placeholder_de_cnpj_nao_vira_banco_do_brasil():
    mapa = MapaEntidades({"00000000": "pares", "58160789": "safra"})
    df = pl.DataFrame({"cnpj": ["00000000000000", "00000000000191", "58160789000128", None]})
    marcado = marcar_grupos_cvm(df, mapa, ["cnpj"])
    assert marcado["grupo"].to_list() == [None, "pares", "safra", None]
    assert marcado["is_safra"].to_list() == [False, False, True, False]
    assert df.select(expr_grupo(mapa, "cnpj").alias("g"))["g"].to_list() == [None, "pares", "safra", None]


# ---------------------------------------------------------------- cadastros

CIA = csv_latin1([
    "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;SIT;CD_CVM;CATEG_REG",
    "58.160.789/0001-28;BANCO SAFRA S/A;;CANCELADA;1406;",
    "00.000.000/0001-91;BANCO DO BRASIL S.A.;BB;ATIVO;1023;Categoria A",
    "11.111.111/0001-11;OUTRA CIA;;ATIVO;1;Categoria A",
])
INTERMED = zip_de({
    "cad_intermed.csv": csv_latin1([
        "TP_PARTIC;CNPJ;DENOM_SOCIAL;DENOM_COMERC;SIT;VL_PATRIM_LIQ",
        "CORRETORAS;65.913.436/0001-17;SAFRA ASSET CORRETORA DE TÍTULOS;;EM FUNCIONAMENTO NORMAL;1234.50",
    ]),
    "cad_intermed_resp.csv": b"x;y\n",
})
ADM = zip_de({"cad_adm_cart_pj.csv": csv_latin1([
    "CNPJ;DENOM_SOCIAL;SIT;CATEG_REG", "11.120.589/0001-88;SAFRA GESTÃO DE RECURSOS LTDA;EM FUNCIONAMENTO NORMAL;Gestor",
]), "cad_adm_cart_pf.csv": b"x\n"})
COORD = zip_de({"cad_coord_oferta.csv": csv_latin1([
    "CNPJ;DENOM_SOCIAL;SIT", "58.160.789/0001-28;BANCO SAFRA S/A;EM FUNCIONAMENTO NORMAL",
])})


def _rotas_cadastros():
    rotas = {}
    arquivos = {
        "cia_aberta-cad": (f"{DADOS}/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv", f"{DADOS}/CIA_ABERTA/CAD/META/meta_cad_cia_aberta.txt"),
        "intermed-cad": (f"{DADOS}/INTERMED/CAD/DADOS/cad_intermed.zip", f"{DADOS}/INTERMED/CAD/META/meta_cad_intermed.zip"),
        "adm_cart-cad": (f"{DADOS}/ADM_CART/CAD/DADOS/cad_adm_cart.zip", f"{DADOS}/ADM_CART/CAD/META/meta_cad_adm_cart.zip"),
        "coord_oferta-cad": (f"{DADOS}/COORD_OFERTA/CAD/DADOS/cad_coord_oferta.zip", f"{DADOS}/COORD_OFERTA/CAD/META/meta.zip"),
    }
    for pkg, (dados, meta) in arquivos.items():
        rotas[f"package_show?id={pkg}"] = pacote(meta, dados)
    rotas[arquivos["cia_aberta-cad"][0]] = Resp(CIA)
    rotas[arquivos["intermed-cad"][0]] = Resp(INTERMED)
    rotas[arquivos["adm_cart-cad"][0]] = Resp(ADM)
    rotas[arquivos["coord_oferta-cad"][0]] = Resp(COORD)
    return rotas


def test_cadastros_discover_ckan_ignora_meta_e_normalize_marca_grupo(settings_factory):
    st = _settings(settings_factory)
    cli = FakeClient(_rotas_cadastros())
    c = CvmCadastrosConnector(st, client=cli, hoje=HOJE)
    recursos = c.discover("2024-01")
    assert len(recursos) == 4 and all("/DADOS/" in r.url for r in recursos)
    assert {r.data_base for r in recursos} == {"202611"}
    assert any("package_show?id=intermed-cad" in u for u in cli.urls)
    for r in recursos:
        c.fetch(r)
    assert all(e.status == "ok" for e in read_entries(st.manifest_path("cvm_cadastros", "202611")))
    c.normalize("202611")
    cia = pl.read_parquet(c.silver_path("cia_aberta", "202611"))
    assert dict(zip(cia["cnpj_cia"], cia["grupo"])) == {
        "58160789000128": "safra", "00000000000191": "pares", "11111111000111": None}
    assert cia.schema["cnpj_cia"] == pl.String
    # asset corretora (65913436) nao esta no mapa de teste: nome nunca marca
    assert pl.read_parquet(c.silver_path("intermed", "202611"))["grupo"].to_list() == [None]
    assert pl.read_parquet(c.silver_path("intermed", "202611")).schema["vl_patrim_liq"] == pl.Float64
    assert pl.read_parquet(c.silver_path("coord_oferta", "202611"))["is_safra"].to_list() == [True]
    assert pl.read_parquet(c.silver_path("adm_cart_pj", "202611")).height == 1

    # segunda coleta: mesmo sha256 -> ja_existe
    c.fetch(recursos[0])
    assert read_entries(st.manifest_path("cvm_cadastros", "202611"))[-1].status == "ja_existe"


def test_cadastros_falha_de_ckan_vira_falha_de_discover(settings_factory):
    st = _settings(settings_factory)
    rotas = _rotas_cadastros()
    rotas["package_show?id=intermed-cad"] = Resp({"success": False, "error": {"message": "x"}})
    c = CvmCadastrosConnector(st, client=FakeClient(rotas), hoje=HOJE)
    assert len(c.discover("2024-01")) == 3
    assert any("intermed-cad" in f for f in c.falhas_discover)
    with pytest.raises(RuntimeError):
        CvmCadastrosConnector(st, client=FakeClient(rotas), hoje=HOJE).baixar_para_busca()


def test_linhas_para_busca_ignora_placeholder():
    df = pl.DataFrame({"cnpj": ["58160789000128", "00000000000000", None], "denom_social": ["A", "B", "C"], "sit": ["ATIVO"] * 3})
    linhas = linhas_para_busca({"intermed": df})
    assert [l["cnpj_raiz"] for l in linhas] == ["58160789"]


# ---------------------------------------------------------------- fundos: cadastro

REG_FUNDO = csv_latin1([
    "ID_Registro_Fundo;CNPJ_Fundo;Codigo_CVM;Tipo_Fundo;Denominacao_Social;Situacao;CNPJ_Administrador;Administrador;CPF_CNPJ_Gestor;Gestor;Patrimonio_Liquido",
    "1;11111111000111;1;FI;FUNDO ADM SAFRA;Em Funcionamento Normal;58160789000128;BANCO SAFRA S/A;99999999000199;GESTOR X;100.5",
    "2;22222222000122;2;FI;FUNDO GESTOR SAFRA;Em Funcionamento Normal;88888888000188;ADM Y;58160789000128;BANCO SAFRA S/A;200",
    "3;33333333000133;3;FI;FUNDO BB;Em Funcionamento Normal;00000000000191;BB;77777777000177;Z;300",
    "4;44444444000144;4;FI;FUNDO DE TERCEIROS;Em Funcionamento Normal;88888888000188;ADM Y;99999999000199;GESTOR X;400",
])
REG_CLASSE = csv_latin1([
    "ID_Registro_Fundo;ID_Registro_Classe;CNPJ_Classe;Codigo_CVM;Denominacao_Social;Situacao;Patrimonio_Liquido",
    "1;10;11111111000111;11;CLASSE DO 1;Em Funcionamento Normal;50",
    "4;40;44444444000144;44;CLASSE DO 4;Em Funcionamento Normal;50",
])
REG_SUB = csv_latin1([
    "ID_Registro_Classe;ID_Subclasse;Codigo_CVM;Denominacao_Social;Situacao",
    "10;SUBA;111;SUBCLASSE A;Em Funcionamento Normal",
    "40;SUBB;444;SUBCLASSE B;Em Funcionamento Normal",
])
REGISTRO_ZIP = zip_de({"registro_fundo.csv": REG_FUNDO, "registro_classe.csv": REG_CLASSE, "registro_subclasse.csv": REG_SUB})
CAD_FI = csv_latin1([
    "TP_FUNDO;CNPJ_FUNDO;DENOM_SOCIAL;SIT;VL_PATRIM_LIQ;CNPJ_ADMIN;ADMIN;CPF_CNPJ_GESTOR;GESTOR",
    "FI;55555555000155;LEGADO SAFRA;EM FUNCIONAMENTO NORMAL;10;58.160.789/0001-28;BANCO SAFRA S/A;1;X",
    "FI;66666666000166;LEGADO TERCEIRO;EM FUNCIONAMENTO NORMAL;10;88.888.888/0001-88;Y;2;X",
])


def _cad_fundos_pronto(st):
    rotas = {"package_show?id=fi-cad": pacote(
        f"{DADOS}/FI/CAD/META/meta_cad_fi.zip",
        f"{DADOS}/FI/CAD/DADOS/registro_fundo_classe.zip",
        f"{DADOS}/FI/CAD/DADOS/cad_fi.csv",
        f"{DADOS}/FI/CAD/DADOS/cad_fi_hist.zip",
    ), "registro_fundo_classe.zip": Resp(REGISTRO_ZIP), "cad_fi.csv": Resp(CAD_FI)}
    c = CvmFundosCadConnector(st, client=FakeClient(rotas), hoje=HOJE)
    recursos = c.discover("2024-01")
    assert sorted(r.nome_arquivo for r in recursos) == ["legado__cad_fi.csv", "registro__registro_fundo_classe.zip"]  # sem hist
    for r in recursos:
        c.fetch(r)
    c.normalize("202611")
    return c


def test_cadastro_de_fundos_filtra_por_administrador_ou_gestor_e_segue_classes(settings_factory):
    st = _settings(settings_factory)
    c = _cad_fundos_pronto(st)
    fundo = pl.read_parquet(c.silver_path("registro_fundo", "202611"))
    assert dict(zip(fundo["cnpj_fundo"], fundo["grupo"])) == {
        "11111111000111": "safra",   # administrador do grupo
        "22222222000122": "safra",   # gestor do grupo
        "33333333000133": "pares",
    }
    assert fundo.schema["patrimonio_liquido"] == pl.Float64
    classe = pl.read_parquet(c.silver_path("registro_classe", "202611"))
    assert classe["cnpj_classe"].to_list() == ["11111111000111"] and classe["is_safra"].to_list() == [True]
    sub = pl.read_parquet(c.silver_path("registro_subclasse", "202611"))
    assert sub["id_subclasse"].to_list() == ["SUBA"] and sub["cnpj_classe"].to_list() == ["11111111000111"]
    legado = pl.read_parquet(c.silver_path("cad_fi_legado", "202611"))
    assert legado["cnpj_fundo"].to_list() == ["55555555000155"] and legado["grupo"].to_list() == ["safra"]

    universo = carregar_universo_fundos(st)
    assert dict(zip(universo["cnpj"], universo["grupo"])) == {
        "11111111000111": "safra", "22222222000122": "safra", "33333333000133": "pares", "55555555000155": "safra"}


# ---------------------------------------------------------------- fundos: informe e CDA

INFORME = csv_latin1([
    "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST",
    "CLASSES - FIF;11.111.111/0001-11;;2026-10-01;10.5;1.5;100.25;5;2;7",
    "CLASSES - FIF;44.444.444/0001-44;;2026-10-01;1;1;1;1;1;1",
    "CLASSES - FIF;33.333.333/0001-33;;2026-10-02;1;1;300;0;9;3",
])
INFORME_LEGADO = csv_latin1([
    "TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST",
    "FI;55.555.555/0001-55;2024-01-02;1;1;10;0;0;1",
])


def _inf_url(anomes):
    return f"{DADOS}/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{anomes}.zip"


def test_informe_exige_universo_e_discover_deduz_meses_antigos_do_template(settings_factory):
    st = _settings(settings_factory)
    rotas = {"package_show?id=fi-doc-inf_diario": pacote(
        f"{DADOS}/FI/DOC/INF_DIARIO/META/meta_inf_diario_fi.txt", _inf_url("202610"), _inf_url("202611"))}
    sem_cadastro = CvmFundosInformeConnector(st, client=FakeClient(rotas), hoje=HOJE)
    with pytest.raises(RuntimeError, match="cvm.fundos_cad"):
        sem_cadastro.discover("2024-01")

    _cad_fundos_pronto(st)
    cli = FakeClient(rotas, existentes={_inf_url("202401"), _inf_url("202402")})
    c = CvmFundosInformeConnector(st, client=cli, hoje=HOJE)
    recursos = c.discover("2024-01", "2024-03")
    assert [r.data_base for r in recursos] == ["202401", "202402"]  # 202403 nao existe (HEAD falso)
    recursos = c.discover("2026-10")
    assert [(r.data_base, r.url) for r in recursos] == [("202610", _inf_url("202610")), ("202611", _inf_url("202611"))]


def test_informe_normalize_filtra_fundos_do_universo_e_tipa(settings_factory):
    st = _settings(settings_factory)
    _cad_fundos_pronto(st)
    c = CvmFundosInformeConnector(st, client=FakeClient(), hoje=HOJE)
    _bronze(st, "cvm_fundos_informe", "202610", "inf_diario_fi_202610.zip",
            zip_de({"inf_diario_fi_202610.csv": INFORME}))
    df = pl.read_parquet(c.normalize("202610"))
    assert sorted(df["cnpj_fundo_classe"]) == ["11111111000111", "33333333000133"]  # 4444... fora do universo
    linha = df.filter(pl.col("cnpj_fundo_classe") == "11111111000111").row(0, named=True)
    assert linha["vl_patrim_liq"] == 100.25 and linha["captc_dia"] == 5.0 and linha["nr_cotst"] == 7
    assert linha["dt_comptc"] == date(2026, 10, 1) and linha["grupo"] == "safra" and linha["is_safra"] is True
    assert df.filter(pl.col("cnpj_fundo_classe") == "33333333000133")["grupo"].to_list() == ["pares"]
    assert c.normalize("209901") is None


def test_informe_aceita_layout_legado_com_cnpj_fundo(settings_factory):
    st = _settings(settings_factory)
    _cad_fundos_pronto(st)
    c = CvmFundosInformeConnector(st, client=FakeClient(), hoje=HOJE)
    _bronze(st, "cvm_fundos_informe", "202401", "inf_diario_fi_202401.zip", zip_de({"inf_diario_fi_202401.csv": INFORME_LEGADO}))
    df = pl.read_parquet(c.normalize("202401"))
    assert df["cnpj_fundo_classe"].to_list() == ["55555555000155"] and df["grupo"].to_list() == ["safra"]


def test_cda_junta_blocos_e_nomeia_o_bloco(settings_factory):
    st = _settings(settings_factory)
    _cad_fundos_pronto(st)
    blc4 = csv_latin1([
        "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC;TP_APLIC;VL_MERC_POS_FINAL;QT_POS_FINAL;CD_ATIVO",
        "CLASSES - FIF;11.111.111/0001-11;F1;2026-10-31;Ações;1234.5;10;PETR4",
        "CLASSES - FIF;44.444.444/0001-44;F4;2026-10-31;Ações;9;1;VALE3",
    ])
    pl_ = csv_latin1([
        "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC;VL_PATRIM_LIQ",
        "CLASSES - FIF;11.111.111/0001-11;F1;2026-10-31;5000.5",
    ])
    fie = csv_latin1([
        "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DENOM_SOCIAL;DT_COMPTC;ID_DOC;VL_PATRIM_LIQ",
        "CLASSES - FIDC;22.222.222/0001-22;F2;2026-10-31;77;1.5",
    ])
    _bronze(st, "cvm_fundos_cda", "202610", "cda_fi_202610.zip", zip_de({
        "cda_fi_BLC_4_202610.csv": blc4, "cda_fi_PL_202610.csv": pl_, "cda_fie_202610.csv": fie, "LEIAME.txt": b"x"}))
    c = CvmFundosCdaConnector(st, client=FakeClient(), hoje=HOJE)
    df = pl.read_parquet(c.normalize("202610"))
    assert sorted(df["bloco"].to_list()) == ["fi_BLC_4", "fi_PL", "fie"]
    blc = df.filter(pl.col("bloco") == "fi_BLC_4").row(0, named=True)
    assert blc["vl_merc_pos_final"] == 1234.5 and blc["cd_ativo"] == "PETR4" and blc["is_safra"] is True
    assert df.schema["dt_comptc"] == pl.Date and df.height == 3


# ---------------------------------------------------------------- ofertas

OFERTA_OLD = csv_latin1([
    "Numero_Processo;CNPJ_Emissor;Nome_Emissor;CNPJ_Lider;Nome_Lider;Data_Registro_Oferta;Valor_Total",
    "P1;58.160.789/0001-28;BANCO SAFRA;;;2020-01-02;100.5",             # emissor do grupo
    "P2;11.111.111/0001-11;OUTRA;58.160.789/0001-28;BANCO SAFRA;2020-02-03;200",   # coordenador do grupo
    "P3;;SAFRA LEASING SA ARRENDAMENTO MERCANTIL;;;2000-01-01;5",      # so nome: nao marca
    "P4;00.000.000/0001-91;BB;11.111.111/0001-11;X;2021-01-01;7",       # emissor par
    "P5;22.222.222/0001-22;TERCEIRO;33.333.333/0001-33;Y;2021-01-01;9",
])
OFERTA_160 = csv_latin1([
    "Numero_Requerimento;CNPJ_Emissor;Nome_Emissor;CNPJ_Lider;Grupo_Coordenador;Data_Registro;Valor_Total_Registrado;Data_deliberacao_aprovou_oferta",
    "1;12.130.744/0001-00;TRUE SEC;58.160.789/0001-28;BANCO SAFRA S/A, ITAU;2023-01-04;1000.00;02/01/2023",
    "2;12.130.744/0001-00;TRUE SEC;99.999.999/0001-99;SAFRA, BTG;2023-01-04;1.00;02/01/2023",
])


def test_ofertas_emissor_ou_lider_do_mapa_e_nunca_por_nome(settings_factory):
    st = _settings(settings_factory)
    rotas = {"package_show?id=oferta-distrib": pacote(
        f"{DADOS}/OFERTA/DISTRIB/META/meta_oferta_distribuicao.zip", f"{DADOS}/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip")}
    c = CvmOfertasConnector(st, client=FakeClient(rotas), hoje=HOJE)
    recursos = c.discover("2024-01")
    assert [r.url for r in recursos] == [f"{DADOS}/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"]
    _bronze(st, "cvm_ofertas", "202611", recursos[0].nome_arquivo, zip_de({
        "oferta_distribuicao.csv": OFERTA_OLD, "oferta_resolucao_160.csv": OFERTA_160}))
    c.normalize("202611")
    old = pl.read_parquet(c.silver_path("ofertas_distribuicao", "202611"))
    assert old["numero_processo"].to_list() == ["P1", "P2", "P4"]
    assert old["grupo_emissor"].to_list() == ["safra", None, "pares"]
    assert old["grupo_coordenador"].to_list() == [None, "safra", None]
    assert old["grupo"].to_list() == ["safra", "safra", "pares"] and old["is_safra"].to_list() == [True, True, False]
    assert old["valor_total"].to_list() == [100.5, 200.0, 7.0]
    novo = pl.read_parquet(c.silver_path("ofertas_res160", "202611"))
    assert novo["numero_requerimento"].to_list() == ["1"] and novo["grupo_coordenador"].to_list() == ["safra"]
    assert novo.schema["data_deliberacao_aprovou_oferta"] == pl.Date


# ---------------------------------------------------------------- DFP / IPE

def _dfp_url(ano):
    return f"{DADOS}/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip"


DFP_INDICE = csv_latin1([
    "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;CATEG_DOC;ID_DOC;DT_RECEB;LINK_DOC",
    "00.000.000/0001-91;2025-12-31;1;BCO BRASIL S.A.;001023;DFP;1;2026-02-11;http://x",
    "11.111.111/0001-11;2025-12-31;1;OUTRA;000001;DFP;2;2026-02-11;http://y",
])
DFP_BPA_CON = csv_latin1([
    "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;ORDEM_EXERC;DT_FIM_EXERC;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA",
    "00.000.000/0001-91;2025-12-31;1;BCO BRASIL S.A.;001023;DF Consolidado - BPA;REAL;MIL;ÚLTIMO;2025-12-31;1;Ativo Total;2398719197.0000000000;S",
    "11.111.111/0001-11;2025-12-31;1;OUTRA;000001;DF Consolidado - BPA;REAL;UNIDADE;ÚLTIMO;2025-12-31;1;Ativo Total;5;S",
])
DFP_DRE_IND = csv_latin1([
    "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;ORDEM_EXERC;DT_INI_EXERC;DT_FIM_EXERC;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA",
    "00.000.000/0001-91;2025-12-31;1;BCO BRASIL S.A.;001023;DF Individual - DRE;REAL;UNIDADE;ÚLTIMO;2025-01-01;2025-12-31;3.01;Receitas;10.5;S",
])
DFP_ZIP = zip_de({
    "dfp_cia_aberta_2025.csv": DFP_INDICE,
    "dfp_cia_aberta_BPA_con_2025.csv": DFP_BPA_CON,
    "dfp_cia_aberta_DRE_ind_2025.csv": DFP_DRE_IND,
    "dfp_cia_aberta_parecer_2025.csv": b"nao deve ser lido\n",
    "dfp_cia_aberta_composicao_capital_2025.csv": csv_latin1([
        "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;QT_ACAO_ORDINARIA_CAP_INTEGR",
        "00.000.000/0001-91;2025-12-31;1;BCO BRASIL S.A.;5000000",
    ]),
})


def test_dfp_discover_por_ano_e_normalize_formato_longo(settings_factory):
    st = _settings(settings_factory)
    rotas = {"package_show?id=cia_aberta-doc-dfp": pacote(
        f"{DADOS}/CIA_ABERTA/DOC/DFP/META/meta_dfp_cia_aberta_txt.zip", _dfp_url(2023), _dfp_url(2024), _dfp_url(2025))}
    c = CvmDfpConnector(st, client=FakeClient(rotas), hoje=HOJE)
    assert [r.data_base for r in c.discover("2024-01")] == ["2024", "2025"]
    assert [r.data_base for r in c.discover("2021-01", "2023-12")] == ["2023"]

    _bronze(st, "cvm_dfp", "2025", "dfp_cia_aberta_2025.zip", DFP_ZIP)
    c.normalize("2025")
    docs = pl.read_parquet(c.silver_path("documentos", "2025"))
    assert docs["cnpj_cia"].to_list() == ["00000000000191"] and docs["grupo"].to_list() == ["pares"]
    assert docs.schema["dt_receb"] == pl.Date and docs.schema["versao"] == pl.Int64
    dem = pl.read_parquet(c.silver_path("demonstracoes", "2025"))
    assert dem.height == 2  # a CIA fora do mapa e o parecer ficam de fora
    ativo = dem.filter(pl.col("demonstracao") == "BPA").row(0, named=True)
    assert ativo["consolidacao"] == "consolidado" and ativo["vl_conta"] == 2398719197.0
    assert ativo["vl_conta_brl"] == 2398719197.0 * 1000  # escala MIL
    dre = dem.filter(pl.col("demonstracao") == "DRE").row(0, named=True)
    assert dre["consolidacao"] == "individual" and dre["vl_conta_brl"] == 10.5 and dre["dt_ini_exerc"] == date(2025, 1, 1)
    assert pl.read_parquet(c.silver_path("composicao_capital", "2025")).height == 1


def test_ipe_ignora_cnpj_placeholder_de_empresa_estrangeira(settings_factory):
    st = _settings(settings_factory)
    ipe = csv_latin1([
        "CNPJ_Companhia;Nome_Companhia;Codigo_CVM;Data_Referencia;Categoria;Tipo;Especie;Assunto;Data_Entrega;Tipo_Apresentacao;Protocolo_Entrega;Versao;Link_Download",
        "00.000.000/0000-00;JBS FOODS INTERNATIONAL;27030;2026-01-29;Assembleia;AGE;Ata;X;2026-02-05;AP;P1;1;http://a",
        "00.000.000/0001-91;BANCO DO BRASIL S.A.;1023;2026-01-30;Fato Relevante;;;Y;2026-02-06;AP;P2;1;http://b",
        "58.160.789/0001-28;BANCO SAFRA S/A;1406;2026-01-31;Comunicado;;;Z;2026-02-07;AP;P3;2;http://c",
    ])
    _bronze(st, "cvm_ipe", "2026", "ipe_cia_aberta_2026.zip", zip_de({"ipe_cia_aberta_2026.csv": ipe}))
    c = CvmIpeConnector(st, client=FakeClient(), hoje=HOJE)
    c.normalize("2026")
    docs = pl.read_parquet(c.silver_path("documentos", "2026"))
    assert docs["protocolo_entrega"].to_list() == ["P2", "P3"]
    assert docs["grupo"].to_list() == ["pares", "safra"] and docs["data_entrega"].to_list()[0] == date(2026, 2, 6)
    assert not c.com_demonstracoes and not c.silver_path("demonstracoes", "2026").exists()


# ---------------------------------------------------------------- resolvedor: sugestoes CVM

LINHAS_CVM = [
    {"cadastro": "intermed", "cnpj": "01638542000157", "cnpj_raiz": "01638542", "nome": "SAFRA DISTRIBUIDORA DE TÍTULOS", "nome_comercial": None, "situacao": "EM FUNCIONAMENTO NORMAL", "categoria": "DISTRIBUIDORAS"},
    {"cadastro": "adm_cart_pj", "cnpj": "01638542000157", "cnpj_raiz": "01638542", "nome": "SAFRA DISTRIBUIDORA DE TÍTULOS", "nome_comercial": None, "situacao": "EM FUNCIONAMENTO NORMAL", "categoria": "Gestor"},
    {"cadastro": "adm_cart_pj", "cnpj": "11120589000188", "cnpj_raiz": "11120589", "nome": "SAFRA GESTÃO DE RECURSOS LTDA", "nome_comercial": None, "situacao": "EM FUNCIONAMENTO NORMAL", "categoria": "Gestor"},
    {"cadastro": "cia_aberta", "cnpj": "10807374000177", "cnpj_raiz": "10807374", "nome": "BOA SAFRA SEMENTES S.A", "nome_comercial": None, "situacao": "ATIVO", "categoria": None},
    {"cadastro": "cia_aberta", "cnpj": "00000000000191", "cnpj_raiz": "00000000", "nome": "BANCO DO BRASIL S.A.", "nome_comercial": "BB", "situacao": "ATIVO", "categoria": "Categoria A"},
]


def test_sugerir_cvm_por_identificador_e_por_nome():
    # por identificador (conglomerado ja resolvido): so as raizes relacionadas
    entrada = {"status": "confirmado", "cnpj": "58160789", "cnpjs_relacionados": ["58160789", "01638542"]}
    itens = sugerir_cvm({"cnpj_raiz": "58160789"}, entrada, LINHAS_CVM, ids_no_mapa={"01638542", "58160789"})
    assert [i["cnpj_raiz"] for i in itens] == ["01638542"]
    assert itens[0]["origem"] == "identificador" and itens[0]["no_mapa"] and not itens[0]["aprovado"]
    assert {r["cadastro"] for r in itens[0]["registros"]} == {"intermed", "adm_cart_pj"}

    # por nome (busca_nome_cvm): "safra" generico traria Boa Safra; o termo especifico nao
    itens = sugerir_cvm({"busca_nome_cvm": ["safra gestao"]}, {"status": "sugestao"}, LINHAS_CVM, ids_no_mapa=set())
    assert [(i["cnpj_raiz"], i["origem"], i["no_mapa"]) for i in itens] == [("11120589", "nome", False)]
    itens = sugerir_cvm({"busca_nome": ["banco do brasil s.a."]}, {"status": "sugestao"}, LINHAS_CVM, ids_no_mapa=set())
    assert [i["cnpj_raiz"] for i in itens] == ["00000000"]


def test_resolver_grava_secao_cvm_preserva_aprovado_e_so_aprovado_marca(settings_factory):
    ent = {"grupos": {"safra": {"cnpj_raiz": "58160789", "entidades": {
        "gestora": {"cnpj_raiz": None, "busca_nome_cvm": ["safra gestao"]}}}}}
    st = settings_factory(ent, entidades_resolvidas=None, fontes=FONTES)
    res = resolver_entidades(st, anomes="202606", cadastro=[], cadastro_cvm=LINHAS_CVM)
    assert res["cvm"]["status"] == "ok"
    item = res["cvm"]["sugestoes"]["safra"]["gestora"][0]
    assert item["cnpj_raiz"] == "11120589" and item["aprovado"] is False
    assert "11120589" not in carregar_mapa_entidades(st).por_identificador  # sugestao nao marca

    # revisao humana: aprova no YAML
    import yaml
    caminho = st.entidades_resolvidas_path()
    dados = yaml.safe_load(caminho.read_text(encoding="utf-8"))
    dados["cvm"]["sugestoes"]["safra"]["gestora"][0]["aprovado"] = True
    caminho.write_text(yaml.safe_dump(dados, allow_unicode=True), encoding="utf-8")
    assert carregar_mapa_entidades(st).por_identificador["11120589"] == "safra"

    # nova execucao preserva a aprovacao
    res2 = resolver_entidades(st, anomes="202606", cadastro=[], cadastro_cvm=LINHAS_CVM)
    assert res2["cvm"]["sugestoes"]["safra"]["gestora"][0]["aprovado"] is True
    # --sem-cvm preserva a secao anterior
    res3 = resolver_entidades(st, anomes="202606", cadastro=[], incluir_cvm=False)
    assert res3["cvm"]["sugestoes"]["safra"]["gestora"][0]["aprovado"] is True


def test_mapa_de_dicts_ignora_sugestao_cvm_nao_aprovada():
    cfg = {"grupos": {"safra": {"entidades": {}}}}
    res = {"cvm": {"sugestoes": {"safra": {"e": [
        {"cnpj_raiz": "11120589", "aprovado": False}, {"cnpj_raiz": "22222222", "aprovado": True}]}}}}
    assert mapa_de_dicts(cfg, res).por_identificador == {"22222222": "safra"}
