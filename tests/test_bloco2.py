"""Testes do Bloco 2 (BCB complementar) com fixtures, sem rede."""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date

import polars as pl

from safra_dados.connectors.bcb_estban import BcbEstbanConnector, nomes_arquivo, parse_estban
from safra_dados.connectors.bcb_reclamacoes import (
    BcbReclamacoesConnector, data_base_do_periodo, parse_ranking_csv, periodos_disponiveis,
)
from safra_dados.connectors.bcb_sgs import BcbSgsConnector
from safra_dados.connectors.bcb_tarifas import BcbTarifasConnector
from safra_dados.connectors.bcb_taxas_juros import BcbTaxasJurosConnector, ultima_janela_por_mes
from safra_dados.connectors.bcb_unicad import BcbUnicadConnector
from safra_dados.manifest import read_entries
from safra_dados.periodos import meses_entre, ultimo_dia

ENTIDADES = {
    "grupos": {
        "safra": {"cnpj_raiz": "58160789", "entidades": {"banco_safra": {"cnpj_raiz": "58160789"}}},
        "pares": {"entidades": {"bb": {"cnpj_raiz": "00000000"}}},
    }
}
OLINDA = "https://olinda.bcb.gov.br/olinda/servico"
FONTES = {
    "bcb_taxas_juros": {"base_url": f"{OLINDA}/taxaJuros/versao/v2/odata"},
    "bcb_tarifas": {"base_url": f"{OLINDA}/Informes_Tarifas/versao/v1/odata", "grupo_consolidado": "15"},
    "bcb_unicad": {"base_url": f"{OLINDA}/BcBase/versao/v2/odata"},
    "bcb_reclamacoes": {"base_url": "https://www3.bcb.gov.br/rdrweb/rest/ext/ranking", "tipos": ["Bancos e financeiras"]},
    "bcb_estban": {"base_url": "https://www.bcb.gov.br/content/estatisticas/estatistica_bancaria_estban",
                   "niveis": ["municipio", "agencia"]},
    "bcb_sgs": {"base_url": "https://api.bcb.gov.br/dados/serie", "series": {432: "selic_meta", 12: "cdi"}},
}


class Resp:
    def __init__(self, corpo, status=200):
        self.status_code = status
        self.content = corpo if isinstance(corpo, bytes) else json.dumps(corpo).encode()
        self.text = self.content.decode("utf-8", "replace")

    def json(self):
        return json.loads(self.content)


class FakeClient:
    """GET por substring da URL; registra as URLs chamadas."""

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


def _settings(settings_factory):
    return settings_factory(ENTIDADES, fontes=FONTES)


def _bronze(settings, fonte, data_base, nome, conteudo):
    d = settings.bronze_fonte_dir(fonte, data_base)
    d.mkdir(parents=True, exist_ok=True)
    (d / nome).write_bytes(conteudo if isinstance(conteudo, bytes) else json.dumps(conteudo).encode())


# ---------------- periodos ----------------

def test_meses_entre_e_ultimo_dia():
    assert meses_entre("2026-09", None, date(2026, 11, 19)) == ["202609", "202610"]
    assert meses_entre("2026-09", None, date(2026, 11, 19), incluir_mes_corrente=True)[-1] == "202611"
    assert meses_entre("2025-11", "2026-02") == ["202511", "202512", "202601", "202602"]
    assert ultimo_dia("202610", date(2026, 11, 19)) == date(2026, 10, 31)
    assert ultimo_dia("202611", date(2026, 11, 19)) == date(2026, 11, 19)


# ---------------- juros ----------------

def test_juros_urls_discover_e_normalize(settings_factory):
    st = _settings(settings_factory)
    datas = {"value": [
        {"inicioPeriodo": "2026-10-01", "fimPeriodo": "2026-10-07", "tipoModalidade": "D"},
        {"inicioPeriodo": "2026-10-29", "fimPeriodo": "2026-11-05", "tipoModalidade": "D"},
        {"inicioPeriodo": "2026-10-01", "fimPeriodo": "2026-10-31", "tipoModalidade": "M"},
    ]}
    cli = FakeClient({"ConsultaDatas": datas})
    c = BcbTaxasJurosConnector(st, client=cli)
    assert ultima_janela_por_mes(datas["value"]) == {"202610": "2026-10-29"}

    recursos = c.discover("2026-10", "2026-10")
    base = FONTES["bcb_taxas_juros"]["base_url"]
    assert [r.url for r in recursos] == [
        f"{base}/TaxasJurosMensalPorMes?$filter=anoMes%20eq%20'2026-10'&$format=json",
        f"{base}/TaxasJurosDiariaPorInicioPeriodo?$filter=InicioPeriodo%20eq%20'2026-10-29'&$format=json",
    ]
    assert all("%24" not in r.url and "%40" not in r.url for r in recursos)

    mensal = {"value": [
        {"Mes": "Out-2026", "Modalidade": "Consignado", "Posicao": 1, "InstituicaoFinanceira": "BCO DO BRASIL S.A.",
         "TaxaJurosAoMes": 1.5, "TaxaJurosAoAno": 19.6, "cnpj8": "00000000", "anoMes": "2026-10"},
        {"Mes": "Out-2026", "Modalidade": "Consignado", "Posicao": 2, "InstituicaoFinanceira": "OUTRO",
         "TaxaJurosAoMes": 1.7, "TaxaJurosAoAno": 22.0, "cnpj8": "11111111", "anoMes": "2026-10"},
    ]}
    diaria = {"value": [{"InicioPeriodo": "2026-10-29", "FimPeriodo": "2026-11-05", "Segmento": "PESSOA FISICA",
                         "Modalidade": "X", "Posicao": 1, "InstituicaoFinanceira": "BCO SAFRA S.A.",
                         "TaxaJurosAoMes": 2.0, "TaxaJurosAoAno": 26.8, "cnpj8": "58160789"}]}
    _bronze(st, "bcb_taxas_juros", "202610", "mensal.json", mensal)
    _bronze(st, "bcb_taxas_juros", "202610", "diaria_2026-10-29.json", diaria)
    df = pl.read_parquet(c.normalize("202610"))
    assert df.height == 3
    assert dict(zip(df["cnpj8"], df["grupo"])) == {"00000000": "pares", "11111111": None, "58160789": "safra"}
    assert set(df["periodicidade"]) == {"mensal", "diaria"}
    assert c.normalize("209901") is None


# ---------------- tarifas ----------------

def test_tarifas_discover_cruza_com_mapa_e_normalize(settings_factory):
    st = _settings(settings_factory)
    base = FONTES["bcb_tarifas"]["base_url"]
    lista = {"value": [{"Cnpj": "58160789", "Nome": "BANCO SAFRA S.A."}, {"Cnpj": "11111111", "Nome": "OUTRO"}]}
    cli = FakeClient({"ListaInstituicoesDeGrupoConsolidado": lista})
    c = BcbTarifasConnector(st, client=cli, hoje=date(2026, 11, 19))

    recursos = c.discover("2026-01", None)
    urls = [r.url for r in recursos]
    assert urls[0] == f"{base}/ListaInstituicoesDeGrupoConsolidado(CodigoGrupoConsolidado=@CodigoGrupoConsolidado)" \
                      "?@CodigoGrupoConsolidado='15'&$format=json"
    assert urls[1] == f"{base}/ListaTarifasPorInstituicaoFinanceira(PessoaFisicaOuJuridica=@PessoaFisicaOuJuridica,CNPJ=@CNPJ)" \
                      "?@PessoaFisicaOuJuridica='F'&@CNPJ='58160789'&$format=json"
    assert len(recursos) == 3 and all(r.data_base == "202611" for r in recursos)
    assert c.discover("2026-12", None) == []  # so ha instantaneo do mes corrente

    tarifa = {"value": [{"CodigoServico": "1101", "Servico": "Cadastro", "Unidade": "Por cliente",
                         "DataVigencia": "2025-02-10", "ValorMaximo": 80.0, "TipoValor": "Real", "Periodicidade": "Por evento"}]}
    _bronze(st, "bcb_tarifas", "202611", "tarifas_F_58160789.json", tarifa)
    _bronze(st, "bcb_tarifas", "202611", "tarifas_J_58160789.json", {"value": []})
    d = pl.read_parquet(c.normalize("202611")).to_dicts()
    assert len(d) == 1 and d[0]["grupo"] == "safra" and d[0]["tipo_pessoa"] == "F" and d[0]["valor_maximo"] == 80.0
    assert d[0]["nome_instituicao"] == "BANCO SAFRA S.A."  # vem do instituicoes.json gravado pelo discover


# ---------------- unicad ----------------

def test_unicad_url_mm_dd_yyyy_e_normalize(settings_factory):
    st = _settings(settings_factory)
    c = BcbUnicadConnector(st, client=FakeClient(), hoje=date(2026, 11, 19))
    recursos = c.discover("2026-10", None)
    assert [r.data_base for r in recursos] == ["202610"]
    assert recursos[0].url == (
        f"{FONTES['bcb_unicad']['base_url']}/EntidadesSupervisionadas(dataBase=@dataBase)"
        "?@dataBase='10-31-2026'&$format=json"
    )
    linhas = {"value": [
        {"database": "2026-10-31", "codigoCNPJ8": "58160789", "codigoCNPJ14": "58160789000128",
         "nomeEntidadeInteresse": "BANCO SAFRA S.A.", "descricaoTipoEntidadeSupervisionada": "Banco Múltiplo",
         "descricaoTipoSituacaoPessoaJuridica": "Autorizada em Atividade", "nomeDoMunicipio": "São Paulo"},
        {"database": "2026-10-31", "codigoCNPJ8": "99999999", "codigoCNPJ14": "99999999000100",
         "nomeEntidadeInteresse": "OUTRA"},
    ]}
    _bronze(st, "bcb_unicad", "202610", "entidades.json", linhas)
    df = pl.read_parquet(c.normalize("202610"))
    assert df["grupo"].to_list() == ["safra", None] and df["situacao"][0] == "Autorizada em Atividade"


# ---------------- reclamacoes ----------------

CSV_RANKING = (
    "Ano;Trimestre;Categoria;Tipo;CNPJ IF;Instituição financeira;Índice;Quantidade total de reclamações respondidas;"
    "Quantidade de reclamações procedentes;Quantidade de reclamações procedentes extrapoladas;"
    "Quantidade total de reclamações analisadas;Quantidade total de clientes – CCS e SCR;"
    "Quantidade de clientes – CCS;Quantidade de clientes – SCR;\n"
    "2026;3º;Top 15;Conglomerado; ;99PAY IP (conglomerado);5,98;1391;74;176;582;29407596;29407432;8288;\n"
    "2026;3º;Demais;Banco/financeira;00000000;BANCO DO BRASIL S.A.; ;10;0;0;4;44525;44525;0;\n"
)
INDICE_RANKING = {"anos": [{"ano": "2026", "periodicidades": [
    {"periodicidade": "TRIMESTRAL", "periodos": [
        {"periodo": 2, "tipos": [{"tipo": "Bancos e financeiras"}]},
        {"periodo": 3, "tipos": [{"tipo": "Bancos e financeiras"}]}]},
    {"periodicidade": "SEMESTRAL", "periodos": [{"periodo": 1, "tipos": [{"tipo": "Consorcios"}]}]}]}]}


def test_reclamacoes_parse_discover_normalize(settings_factory):
    st = _settings(settings_factory)
    df = parse_ranking_csv(CSV_RANKING)
    assert df.height == 2
    assert df["indice"].to_list() == [5.98, None]
    assert df["cnpj_if"].to_list() == [None, "00000000"]
    assert df["periodo"].to_list() == [3, 3] and df["qtd_analisadas"].to_list() == [582, 4]

    assert data_base_do_periodo(2026, "TRIMESTRAL", 3) == "202609"
    assert data_base_do_periodo(2026, "SEMESTRAL", 1) == "202606"
    assert len(periodos_disponiveis(INDICE_RANKING, ["Bancos e financeiras"])) == 2

    c = BcbReclamacoesConnector(st, client=FakeClient({"rdrweb/rest/ext/ranking": INDICE_RANKING}))
    recursos = c.discover("2026-09", None)
    assert [r.data_base for r in recursos] == ["202609"]
    assert recursos[0].url.endswith("/arquivo?ano=2026&periodicidade=TRIMESTRAL&periodo=3&tipo=Bancos+e+financeiras")

    _bronze(st, "bcb_reclamacoes", "202609", "ranking_2026_TRIMESTRAL_3.csv", CSV_RANKING.encode("cp1252"))
    out = pl.read_parquet(c.normalize("202609"))
    assert out["grupo"].to_list() == [None, "pares"]  # conglomerado sem CNPJ nunca e marcado por nome


# ---------------- estban ----------------

CSV_ESTBAN_AG = (
    "ESTBAN (Documento 4500) por agencia\nData de geracao dos dados: 2026-11-03\n"
    "#DATA_BASE;UF;CODMUN;MUNICIPIO;CNPJ;NOME_INSTITUICAO;AGENCIA;VERBETE_110_DISPONIBILIDADES;"
    "VERBETE_141_CORRESPONDENTES_NO_EXTERIOR + VERBETE_142_CORRESPONDENTES_NO_PAIS\n"
    "202606;AC;50830;ACRELANDIA;00000000;BCO DO BRASIL S.A.;'00000000517569;1747142;35840685\n"
    "202606;SP;3550308;SAO PAULO;58160789;BANCO SAFRA S.A.;'58160789000001;10;-5\n"
)
CSV_ESTBAN_MUN = (
    "ESTBAN (Documento 4500) por municipio \nData de geracao dos dados: 2026-11-03\n"
    "#DATA_BASE;UF;CODMUN;MUNICIPIO;CNPJ;NOME_INSTITUICAO;AGEN_ESPERADAS;AGEN_PROCESSADAS;VERBETE_110_DISPONIBILIDADES\n"
    "202606;SP;3550308;SAO PAULO;58160789;BANCO SAFRA S.A.;3;3;100\n"
)


def test_estban_parse_longo_e_nomes():
    ag = parse_estban(CSV_ESTBAN_AG, "agencia")
    assert ag.height == 4  # 2 linhas x 2 verbetes
    assert set(ag["verbete"]) == {"110", "141"}
    assert ag.filter(pl.col("cnpj") == "58160789").filter(pl.col("verbete") == "141")["saldo"][0] == -5
    assert ag["agencia"][0] == "00000000517569"  # aspa inicial removida
    assert nomes_arquivo("202606", "agencia")[0] == "202606_ESTBAN_AG.csv.zip"
    assert nomes_arquivo("202606", "municipio")[0] == "202606_ESTBAN.csv.zip"


def test_estban_discover_e_normalize(settings_factory):
    st = _settings(settings_factory)
    base = FONTES["bcb_estban"]["base_url"]
    cli = FakeClient(existentes={f"{base}/municipio/202606_ESTBAN.csv.zip", f"{base}/agencia/202606_ESTBAN_AG.ZIP"})
    c = BcbEstbanConnector(st, client=cli)
    recursos = c.discover("2026-06", "2026-06")
    assert [(r.extra["nivel"], r.url.rsplit("/", 1)[1]) for r in recursos] == [
        ("municipio", "202606_ESTBAN.csv.zip"), ("agencia", "202606_ESTBAN_AG.ZIP")]

    def _zip(nome, texto):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr(nome, texto.encode("latin-1"))
        return buf.getvalue()

    _bronze(st, "bcb_estban", "202606", "municipio_202606_ESTBAN.csv.zip", _zip("202606_ESTBAN.CSV", CSV_ESTBAN_MUN))
    _bronze(st, "bcb_estban", "202606", "agencia_202606_ESTBAN_AG.ZIP", CSV_ESTBAN_AG.encode("latin-1"))  # CSV puro
    df = pl.read_parquet(c.normalize("202606"))
    assert set(df["nivel"]) == {"municipio", "agencia"}
    assert df.filter(pl.col("grupo") == "safra").height == 3 and df.filter(pl.col("grupo") == "pares").height == 2


# ---------------- sgs ----------------

def test_sgs_url_discover_normalize(settings_factory):
    st = _settings(settings_factory)
    c = BcbSgsConnector(st, client=FakeClient(), hoje=date(2026, 11, 19))
    recursos = c.discover("2026-10", None)
    assert [(r.data_base, r.extra["serie"]) for r in recursos] == [
        ("202610", 432), ("202610", 12), ("202611", 432), ("202611", 12)]
    assert recursos[0].url == ("https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados"
                               "?formato=json&dataInicial=01/10/2026&dataFinal=31/10/2026")
    assert recursos[2].url.endswith("dataInicial=01/11/2026&dataFinal=19/11/2026")

    _bronze(st, "bcb_sgs", "202610", "sgs_432.json", [{"data": "01/10/2026", "valor": "13.75"}])
    _bronze(st, "bcb_sgs", "202610", "sgs_12.json", [{"data": "01/10/2026", "valor": "0.050788"}])
    df = pl.read_parquet(c.normalize("202610")).sort("serie")
    assert df["nome_serie"].to_list() == ["cdi", "selic_meta"]
    assert df["data"].dtype == pl.Date and df["valor"].to_list() == [0.050788, 13.75]


# ---------------- fetch (HttpConnector): bronze + manifest + sha256 ----------------

def test_fetch_idempotente_por_sha256_e_registra_falha(settings_factory):
    st = _settings(settings_factory)
    base = FONTES["bcb_sgs"]["base_url"]
    cli = FakeClient({"sgs.432": [{"data": "01/10/2026", "valor": "13.75"}], "sgs.12": Resp({"x": 1}, 500)})
    c = BcbSgsConnector(st, client=cli, hoje=date(2026, 11, 19))
    r432, r12 = c.discover("2026-10", "2026-10")
    c.fetch(r432)
    c.fetch(r432)  # mesmo conteudo -> nao regrava
    c.fetch(r12)
    entradas = read_entries(st.manifest_path("bcb_sgs", "202610"))
    assert [e.status for e in entradas] == ["ok", "ja_existe", "falha"]
    assert entradas[0].sha256 == entradas[1].sha256 and "HTTP 500" in entradas[2].erro
    assert (st.bronze_fonte_dir("bcb_sgs", "202610") / "sgs_432.json").is_file()
    assert not (st.bronze_fonte_dir("bcb_sgs", "202610") / "sgs_12.json").exists()
    assert base  # silencia linter


def test_sgs_404_sem_valores_nao_e_falha(settings_factory):
    st = _settings(settings_factory)
    erro = Resp({"erro": {"detail": "SGSNegocioException: Value(s) not found"}}, 404)
    c = BcbSgsConnector(st, client=FakeClient({"sgs.432": erro, "sgs.12": Resp({"x": 1}, 500)}), hoje=date(2026, 11, 19))
    r432, r12 = c.discover("2026-11", "2026-11")
    c.fetch(r432)
    c.fetch(r12)
    assert [e.status for e in read_entries(st.manifest_path("bcb_sgs", "202611"))] == ["ok", "falha"]
