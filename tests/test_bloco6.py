"""Testes do Bloco 6 (app Streamlit, somente leitura), sem rede, sobre fixtures parquet do Bloco 5."""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pytest

from safra_dados import app_core
from safra_dados.app_core import (
    Filtro, SqlNaoPermitido, contar, criar_conexao_memoria, exportar_consulta, formatar_df, montar_consulta, pagina,
    relacoes_disponiveis, status_fontes, tipo_formato, total_paginas, validar_sql_somente_leitura,
)
from safra_dados.gold import VIEWS_GOLD, aplicar_views, criar_gold
from test_bloco5 import (  # fixtures de parquet e Settings de teste (mesmo diretorio de testes)
    _settings, silver_cosif, silver_estban, silver_fundos, silver_ifdata, silver_juros, silver_reclamacoes, silver_ses,
)


@pytest.fixture
def st_app(tmp_path):
    st = _settings(tmp_path)
    for f in (silver_ifdata, silver_cosif, silver_juros, silver_reclamacoes, silver_estban, silver_fundos, silver_ses):
        f(st)
    return st


# ---------------------------------------------------------------- views em memoria

def test_aplicar_views_em_conexao_qualquer_e_igual_ao_gold_em_arquivo(st_app):
    """A mesma funcao serve ao arquivo (`criar_gold`) e a um banco em memoria; o resultado das views e identico."""
    mem = duckdb.connect(":memory:")
    r = aplicar_views(mem, st_app)
    assert not r.erros, r.erros
    assert set(r.criadas) == set(VIEWS_GOLD) and r.caminho is None
    criar_gold(st_app)
    arq = duckdb.connect(str(st_app.gold_db_path), read_only=True)
    for view in ("capital", "balanco_dre", "carteira", "juros", "reclamacoes", "agencias", "seguros"):
        a = mem.execute(f"select * from gold.{view} order by all").fetchall()
        b = arq.execute(f"select * from gold.{view} order by all").fetchall()
        assert a == b and a, view
    arq.close()
    mem.close()


def test_conexao_do_app_e_em_memoria_e_nao_toca_no_gold_em_arquivo(st_app):
    assert not st_app.gold_db_path.exists()
    con, r = criar_conexao_memoria(st_app)
    assert not r.erros, r.erros
    assert not st_app.gold_db_path.exists()  # o app nao cria nem abre data/gold/safra.duckdb
    rels = relacoes_disponiveis(con)
    assert {"capital", "fundos", "fundos_conciliacao", "dim_entidade"} <= set(rels["gold"])
    assert {"ifdata", "informe"} <= set(rels["silver"])
    # views pesadas viram tabela em memoria (resultado agregado), com o mesmo conteudo
    tipo = con.execute("select table_type from information_schema.tables where table_schema = 'gold' and table_name = 'fundos'").fetchone()
    assert tipo[0] == "BASE TABLE"
    assert con.execute("select count(*) from gold.fundos").fetchone()[0] > 0
    con.close()


def test_conexao_do_app_inclui_fontes_silver_sem_view_gold(st_app):
    caminho = st_app.silver_dir / "bcb_sgs" / "202610.parquet"
    caminho.parent.mkdir(parents=True)
    pl.DataFrame({"data": ["2026-10-01"], "valor": [10.5]}).write_parquet(caminho)
    sub = st_app.silver_dir / "cvm_dfp" / "documentos" / "2025.parquet"
    sub.parent.mkdir(parents=True)
    pl.DataFrame({"x": [1]}).write_parquet(sub)
    con, _ = criar_conexao_memoria(st_app, materializar=())
    rels = relacoes_disponiveis(con)["silver"]
    assert "bcb_sgs" in rels and "cvm_dfp__documentos" in rels
    assert con.execute("select valor from silver.bcb_sgs").fetchone()[0] == 10.5
    con.close()


# ---------------------------------------------------------------- SQL somente leitura

@pytest.mark.parametrize("sql", [
    "select 1", "SELECT * from gold.capital where entidade = 'a;b'", "with x as (select 1 a) select * from x;",
    "  \n select 'drop table x' as texto", "select \"update\" from (select 1 as \"update\")", "-- comentario\nselect 1",
    "select 1 /* delete */", "(select 1) union all (select 2)",
])
def test_sql_permitido(sql):
    assert validar_sql_somente_leitura(sql)


@pytest.mark.parametrize("sql", [
    "", "   ", "insert into t values (1)", "update gold.capital set x = 1", "delete from t", "drop view gold.capital",
    "create table x as select 1", "alter table t add column c int", "attach 'x.duckdb' as x", "copy (select 1) to 'f.csv'",
    "pragma database_list", "set memory_limit = '1GB'", "call pragma_version()", "select 1; select 2", "select 1; drop table t",
    "with x as (select 1) delete from t", "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
    "select * from read_csv('c:/dados.csv')", "select * from read_parquet('x.parquet')", "select * from glob('*')",
    "explain select 1", "install httpfs", "load httpfs", "export database 'x'", "vacuum", "show tables",
    "select 1 /* aberto", "select 'aberto",
])
def test_sql_bloqueado(sql):
    with pytest.raises(SqlNaoPermitido):
        validar_sql_somente_leitura(sql)


def test_sql_valido_executa_no_duckdb_em_memoria(st_app):
    con, _ = criar_conexao_memoria(st_app, materializar=())
    sql = validar_sql_somente_leitura("select entidade, indice_basileia from gold.capital where grupo = 'safra';")
    assert not sql.endswith(";")
    assert contar(con, sql) == 1
    con.close()


# ---------------------------------------------------------------- formatacao

def test_formatacao_tres_casas_virgula_decimal():
    assert app_core.fmt_bi(29_117_400_000) == "R$ 29,117 bi"
    assert app_core.fmt_bi(1_234_567_891_234) == "R$ 1.234,568 bi"
    assert app_core.fmt_bi(None) == "n/d"
    assert app_core.fmt_pct(18.14) == "18,140%"
    assert app_core.fmt_pct(1234.5) == "1.234,500%"
    assert app_core.fmt_num(207.93) == "207,930"
    assert app_core.fmt_int(1234567) == "1.234.567"


def test_tipo_formato_por_coluna():
    assert tipo_formato("patrimonio_liquido", "DOUBLE") == "bi"
    assert tipo_formato("pl_deduplicado_principal", "DOUBLE") == "bi"
    assert tipo_formato("indice_basileia", "DOUBLE") == "pct"
    assert tipo_formato("roe_anualizado_mediana_pares", "DOUBLE") == "pct"
    assert tipo_formato("diferenca_pct", "DOUBLE") == "pct"
    assert tipo_formato("diferenca", "DOUBLE") == "bi"
    assert tipo_formato("taxa_ao_ano", "DOUBLE") == "pct"
    assert tipo_formato("indice", "DOUBLE") == "num"  # reclamacoes: por milhao de clientes, nao e %
    assert tipo_formato("n_agencias", "BIGINT") == "int"
    assert tipo_formato("entidade", "VARCHAR") == "txt"


def test_formatar_df_nao_altera_original_e_trata_nulos():
    df = pd.DataFrame({"entidade": ["a", "b"], "patrimonio_liquido": [29_117_000_000.0, None], "indice_basileia": [18.1411, 15.0],
                       "indice": [207.93, 1.0], "n_agencias": [1500, 2]})
    out = formatar_df(df)
    assert list(out["patrimonio_liquido"]) == ["R$ 29,117 bi", "n/d"]
    assert list(out["indice_basileia"]) == ["18,141%", "15,000%"]
    assert list(out["indice"]) == ["207,930", "1,000"]
    assert list(out["n_agencias"]) == ["1.500", "2"]
    assert df["patrimonio_liquido"].iloc[0] == 29_117_000_000.0


def test_valores_do_gold_formatados_em_3_casas(st_app):
    con, _ = criar_conexao_memoria(st_app, materializar=())
    df = con.execute("select indice_basileia, ativo_total from gold.capital c join gold.balanco_dre b using (data_base, entidade) "
                     "where c.grupo = 'safra'").df()
    out = formatar_df(df)
    assert out["indice_basileia"].iloc[0].endswith("%") and "," in out["indice_basileia"].iloc[0]
    assert out["ativo_total"].iloc[0].startswith("R$ ") and out["ativo_total"].iloc[0].endswith(" bi")
    con.close()


# ---------------------------------------------------------------- paginacao e filtros

@pytest.fixture
def con_num():
    con = duckdb.connect(":memory:")
    con.execute("create schema gold")
    con.execute("create table gold.t as select i as n, 'grupo_' || (i % 3) as g, i * 1.5 as v, "
                "date '2026-01-01' + cast(i as integer) as d from range(1, 251) r(i)")
    return con


def test_paginacao_limit_offset(con_num):
    sql, params = montar_consulta("gold.t", ["n", "g", "v", "d"], ordem=[("n", True)])
    assert contar(con_num, sql, params) == 250
    assert total_paginas(250, 100) == 3 and total_paginas(0, 100) == 1 and total_paginas(100, 100) == 1 and total_paginas(101, 100) == 2
    p1, p3 = pagina(con_num, sql, params, 1, 100), pagina(con_num, sql, params, 3, 100)
    assert len(p1) == 100 and list(p1["n"][:2]) == [1, 2] and p1["n"].iloc[-1] == 100
    assert len(p3) == 50 and p3["n"].iloc[0] == 201 and p3["n"].iloc[-1] == 250
    assert pagina(con_num, sql, params, 4, 100).empty


def test_montar_consulta_filtros_dinamicos(con_num):
    cols = ["n", "g", "v", "d"]
    sql, p = montar_consulta("gold.t", cols, [Filtro("g", "contem", "UPO_1")])
    assert contar(con_num, sql, p) == 84
    sql, p = montar_consulta("gold.t", cols, [Filtro("n", "intervalo", (10, 20))])
    assert contar(con_num, sql, p) == 11
    sql, p = montar_consulta("gold.t", cols, [Filtro("n", "intervalo", (240, None)), Filtro("g", "lista", ["grupo_0", "grupo_1"])])
    assert contar(con_num, sql, p) == 8  # 240..250 (11) menos os de resto 2: 242, 245, 248
    sql, p = montar_consulta("gold.t", cols, [Filtro("d", "intervalo", ("2026-01-10", "2026-01-12"))])
    assert contar(con_num, sql, p) == 3
    sql, p = montar_consulta("gold.t", cols, [Filtro("g", "contem", ""), Filtro("g", "lista", [])])  # filtros vazios nao filtram
    assert contar(con_num, sql, p) == 250
    sql, p = montar_consulta("gold.t", cols, colunas=["n"], ordem=[("n", False)], limite=3, offset=1)
    assert con_num.execute(sql, p).fetchall() == [(249,), (248,), (247,)]


def test_montar_consulta_rejeita_coluna_ou_filtro_invalido():
    with pytest.raises(ValueError):
        montar_consulta("gold.t", ["n"], [Filtro("n; drop table t", "contem", "x")])
    with pytest.raises(ValueError):
        montar_consulta("gold.t", ["n"], colunas=["inexistente"])
    with pytest.raises(ValueError):
        montar_consulta("gold.t", ["n"], ordem=[("x", True)])
    with pytest.raises(ValueError):
        montar_consulta("gold.t", ["n"], [Filtro("n", "regex", "x")])


def test_filtro_contem_nao_permite_injecao(con_num):
    sql, p = montar_consulta("gold.t", ["n", "g"], [Filtro("g", "contem", "x'; drop table gold.t; --")])
    assert contar(con_num, sql, p) == 0
    assert con_num.execute("select count(*) from gold.t").fetchone()[0] == 250


# ---------------------------------------------------------------- exportacao

def test_exportacao_ate_o_limite_devolve_bytes(con_num, st_app):
    sql, p = montar_consulta("gold.t", ["n", "g"], [Filtro("n", "intervalo", (1, 10))])
    r = exportar_consulta(con_num, st_app, sql, p, "csv", "teste")
    assert r.dados is not None and r.caminho is None and r.n_linhas == 10 and r.nome == "teste.csv"
    assert r.dados.decode().splitlines()[0] == "n,g" and len(r.dados.decode().splitlines()) == 11
    x = exportar_consulta(con_num, st_app, sql, p, "xlsx", "teste")
    assert x.dados[:2] == b"PK" and x.nome == "teste.xlsx"


def test_exportacao_acima_do_limite_vai_para_exports(con_num, st_app, monkeypatch):
    monkeypatch.setattr(app_core, "LIMITE_DOWNLOAD", 100)
    sql, p = montar_consulta("gold.t", ["n", "g"], [Filtro("g", "contem", "grupo")])
    r = exportar_consulta(con_num, st_app, sql, p, "csv", "grande consulta")
    assert r.dados is None and r.n_linhas == 250
    assert r.caminho == st_app.exports_dir / "grande_consulta.csv" and r.caminho.is_file()
    assert len(r.caminho.read_text(encoding="utf-8").splitlines()) == 251
    x = exportar_consulta(con_num, st_app, sql, p, "xlsx", "grande")
    assert x.dados is None and x.caminho.suffix == ".xlsx" and x.caminho.is_file()
    with pytest.raises(ValueError):
        exportar_consulta(con_num, st_app, sql, p, "parquet", "x")


def test_limite_de_download_e_50_mil():
    assert app_core.LIMITE_DOWNLOAD == 50_000


# ---------------------------------------------------------------- status das fontes

def test_status_fontes_usa_checkpoint_e_calendario(st_app):
    from datetime import date

    from safra_dados.manifest import Checkpoint, save_checkpoint

    cp = Checkpoint(fonte="bcb_ifdata")
    cp.marcar("202606", arquivos_ok=3, arquivos_falha=0)
    save_checkpoint(st_app.checkpoint_path("bcb_ifdata"), cp)
    linhas = {r["fonte"]: r for r in status_fontes(st_app, date(2026, 11, 19))}
    assert set(linhas) == set(app_core.CALENDARIOS)
    r = linhas["bcb_ifdata"]
    assert r["ultima_data_base_ok"] == "202606" and r["arquivos_ok"] == 3 and r["n_data_bases"] == 1
    assert r["proxima_data_base_esperada"] and isinstance(r["vencida"], bool)
    assert linhas["bcb_sgs"]["ultima_data_base_ok"] is None and linhas["bcb_sgs"]["vencida"] is True


# ---------------------------------------------------------------- app (AppTest, sem rede)

APP = Path(__file__).resolve().parents[1] / "app" / "app.py"
PAGINAS = [
    "pages/2_Capital.py", "pages/3_Balanco_e_DRE.py", "pages/4_Carteira.py", "pages/5_Juros.py", "pages/6_Fundos.py",
    "pages/7_Seguros.py", "pages/8_Reclamacoes_e_agencias.py", "pages/9_Explorador.py", "pages/10_Fontes_e_status.py",
]


def test_app_paginas_carregam_sem_excecao_sobre_fixtures(st_app, monkeypatch):
    pytest.importorskip("streamlit")
    pytest.importorskip("plotly")
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("SAFRA_DADOS_CONFIG_DIR", str(st_app.config_dir))
    monkeypatch.setenv("SAFRA_DADOS_HOME", str(st_app.data_root))
    at = AppTest.from_file(str(APP), default_timeout=300).run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Basileia" in m.label for m in at.metric)  # pagina 1 (Visao geral)
    graficos = {}
    for pagina_ in PAGINAS:
        at.switch_page(pagina_).run()
        assert not at.exception, (pagina_, [e.value for e in at.exception])
        assert not at.error, (pagina_, [e.value for e in at.error])
        graficos[pagina_] = len(at.get("plotly_chart"))
    assert graficos["pages/2_Capital.py"] >= 2 and graficos["pages/6_Fundos.py"] >= 1
    # explorador: SQL que escreve e bloqueado, SELECT roda
    at.switch_page("pages/9_Explorador.py").run()
    at.text_area(key="exp_sql").set_value("drop view gold.capital").run()
    next(b for b in at.button if b.key == "exp_sql_run").click().run()
    assert any("Bloqueado" in e.value for e in at.error)
    at.text_area(key="exp_sql").set_value("select entidade from gold.capital").run()
    next(b for b in at.button if b.key == "exp_sql_run").click().run()
    assert not at.exception and not [e for e in at.error if "Bloqueado" in e.value]
