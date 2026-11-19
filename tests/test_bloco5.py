"""Testes do Bloco 5 (gold com DuckDB, exportacao, calendario/atualizar), sem rede, sobre fixtures parquet."""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import polars as pl
import pytest
import yaml
from typer.testing import CliRunner

from safra_dados.calendario import CALENDARIOS, desde_para_atualizar, vencida
from safra_dados.config import HttpSettings, Settings
from safra_dados.gold import (
    VIEWS_GOLD, conciliacao_fundos, conectar_gold, construir_dim_entidade, criar_gold, exportar_view, resumo_safra,
)
from safra_dados.manifest import Checkpoint

ENTIDADES = {"grupos": {
    "safra": {"cnpj_raiz": "58160789", "entidades": {
        "banco_safra": {"cnpj_raiz": "58160789", "codigo_conglomerado_prudencial": "C0080109"},
        "asset_safra": {"cnpj_raiz": "65913436"},
        "seguradora_safra": {"cnpj_raiz": None},
    }},
    "pares": {"entidades": {
        "itau_unibanco": {"cnpj_raiz": "60872504", "codigo_conglomerado_prudencial": "C0080099"},
        "bradesco": {"cnpj_raiz": "60746948", "codigo_conglomerado_prudencial": "C0080075"},
    }},
}}
RESOLVIDAS = {
    "grupos": {},
    "susep": {"sugestoes": {
        "safra": {"seguradora_safra": [{"codigo_fip": "01627", "cnpj": "06109373000181", "cnpj_raiz": "06109373", "aprovado": True}]},
        "pares": {
            "bradesco": [{"codigo_fip": "05444", "cnpj": "33055146000193", "cnpj_raiz": "33055146", "aprovado": True}],
            "itau_unibanco": [
                {"codigo_fip": "05321", "cnpj": "61557039000107", "cnpj_raiz": "61557039", "aprovado": True},
                {"codigo_fip": "03182", "cnpj": "08816067000100", "cnpj_raiz": "08816067", "aprovado": False},
            ],
        },
    }},
}
REFERENCIAS = {
    "referencias": {
        "fundos_administrados": {"data_referencia": "2026-09-30", "valor_mil_reais": 150000, "fonte": "nota9-fundos"},
        "previdencia_consolidada": {"data_referencia": "2026-09-30", "valor_mil_reais": 10000, "fonte": "nota9-prev"},
    },
    "conciliacoes_fundos": [
        {"id": "fundos_administrados", "descricao": "a", "referencia": ["fundos_administrados"], "visao": "administrado",
         "inclui_previdencia_ses": False},
        {"id": "total", "descricao": "b", "referencia": ["fundos_administrados", "previdencia_consolidada"],
         "visao": "administrado", "inclui_previdencia_ses": True},
    ],
    "reclamacoes_nomes": [
        {"instituicao": "SAFRA (conglomerado)", "grupo": "safra", "entidade": "banco_safra"},
        {"instituicao": "ITAU (conglomerado)", "grupo": "pares", "entidade": "itau_unibanco"},
        {"instituicao": "BRADESCO (conglomerado)", "grupo": "pares", "entidade": "bradesco"},
    ],
}


def _settings(tmp_path) -> Settings:
    config = tmp_path / "config"
    config.mkdir(parents=True, exist_ok=True)
    (config / "entidades.yaml").write_text(yaml.safe_dump(ENTIDADES), encoding="utf-8")
    (config / "entidades_resolvidas.yaml").write_text(yaml.safe_dump(RESOLVIDAS), encoding="utf-8")
    (config / "referencias.yaml").write_text(yaml.safe_dump(REFERENCIAS), encoding="utf-8")
    (config / "settings.yaml").write_text(yaml.safe_dump({"data_root": str(tmp_path / "data"), "fontes": {}}), encoding="utf-8")
    return Settings(config_dir=config, data_root=tmp_path / "data", http=HttpSettings(), fontes={})


def _silver(st: Settings, relativo: str, linhas: list[dict], schema: dict | None = None) -> None:
    caminho = st.silver_dir / relativo
    caminho.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(linhas, schema=schema, infer_schema_length=None).write_parquet(caminho)


def _q(st: Settings, sql: str) -> list[dict]:
    con = conectar_gold(st)
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()


# ---------------------------------------------------------------- fixtures por fonte

def _ifd(db, cod, rel, conta, valor, coluna="x", grupo_conta=None, nome_rel="rel", grupo=None):
    return {
        "data_base": db, "cod_inst": cod, "tipo_inst": 1, "relatorio": rel, "nome_relatorio": nome_rel,
        "grupo_conta": grupo_conta, "conta": conta, "nome_coluna": coluna, "descricao_coluna": "", "valor": float(valor),
        "nome_instituicao": f"INST {cod}", "grupo": grupo, "is_safra": grupo == "safra",
    }


def silver_ifdata(st: Settings) -> None:
    linhas = []
    # (cod, grupo, basileia, nivel1, capital principal)
    for cod, grupo, bas in (("C0080109", "safra", 0.18), ("C0080099", "pares", 0.15), ("C0080075", "pares", 0.16)):
        linhas += [
            _ifd("202606", cod, "5", "79664", bas, grupo=grupo), _ifd("202606", cod, "5", "79660", bas - 0.03, grupo=grupo),
            _ifd("202606", cod, "5", "79659", bas - 0.07, grupo=grupo), _ifd("202606", cod, "5", "79649", 29e9, grupo=grupo),
            _ifd("202606", cod, "5", "79665", 160e9, grupo=grupo), _ifd("202606", cod, "5", "79651", 11e9, grupo=grupo),
        ]
    # Avenue: marcado `pares` por CNPJ aprovado no CVM, mas nao e conglomerado de config -> fora da gold
    linhas += [_ifd("202606", "C0088008", "5", "79664", 0.99, grupo="pares")]
    # resumo (relatorio 1) do Safra: 202512, 202603, 202606; pares: so 202606
    for db, ativo, pl_, lucro in (("202512", 9000, 800, 400), ("202603", 10000, 1000, 100), ("202606", 12000, 1200, 250)):
        for conta, v in (("140220", ativo), ("141873", ativo / 2), ("140239", ativo * 0.8), ("140246", pl_), ("141870", lucro)):
            linhas.append(_ifd(db, "C0080109", "1", conta, v, grupo="safra"))
    for cod in ("C0080099", "C0080075"):
        for conta, v in (("140220", 5000), ("141873", 2000), ("140239", 4000), ("140246", 500), ("141870", 50)):
            linhas.append(_ifd("202606", cod, "1", conta, v, grupo="pares"))
    # carteira: indexador (relatorio 7, total geral 24454), porte (14) e modalidade (13, so coluna Total)
    linhas += [
        _ifd("202606", "C0080109", "7", "24454", 1000, coluna="Total Geral", grupo="safra"),
        _ifd("202606", "C0080109", "7", "23367", 600, coluna="Prefixado", grupo="safra"),
        _ifd("202606", "C0080109", "14", "23366", 250, coluna="Grande", grupo="safra"),
        _ifd("202606", "C0080109", "13", "23283", 175, coluna="Total", grupo_conta="Capital de Giro", grupo="safra"),
        _ifd("202606", "C0080109", "13", "23277", 99, coluna="Vencido a Partir de 15 Dias", grupo_conta="Capital de Giro", grupo="safra"),
    ]
    _silver(st, "bcb_ifdata/202606.parquet", linhas)


def silver_cosif(st: Settings) -> None:
    linhas = []
    for db, cong, ativo_a, ativo_p, pl_, cred, dev in (
        ("202603", "C0080109", 9500, 500, 900, 130, -30), ("202606", "C0080109", 11500, 500, 950, 330, -80),
    ):
        for conta, saldo in (("1000000009", ativo_a), ("2000000008", ativo_p), ("6000000004", pl_), ("7000000003", cred), ("8000000002", dev)):
            linhas.append({"data_base": db, "cnpj": "58160789", "documento": "4060", "conta": conta, "saldo": float(saldo),
                           "cod_conglomerado": cong, "grupo": "safra", "is_safra": True})
    _silver(st, "bcb_cosif/202606.parquet", linhas)


def silver_juros(st: Settings) -> None:
    def j(cnpj8, grupo, ao_ano, modalidade="Cartao", db="202610", per="mensal", ipos=1):
        return {"data_base": db, "periodicidade": per, "inicio_periodo": f"{db}01", "fim_periodo": f"{db}30", "segmento": "PF",
                "modalidade": modalidade, "posicao": ipos, "instituicao_financeira": cnpj8, "cnpj8": cnpj8,
                "taxa_ao_mes": ao_ano / 12, "taxa_ao_ano": float(ao_ano), "grupo": grupo, "is_safra": grupo == "safra"}
    linhas = [j("58160789", "safra", 60), j("58160789", "safra", 80), j("60872504", "pares", 40), j("60746948", "pares", 50),
              j("99999999", None, 10)]
    _silver(st, "bcb_taxas_juros/202610.parquet", linhas)


def silver_reclamacoes(st: Settings) -> None:
    def r(inst, indice, grupo=None, tipo="Conglomerado", cnpj=None):
        return {"ano": 2026, "periodo": 3, "categoria": "Top 15", "tipo_instituicao": tipo,
                "qtd_procedentes_extrapoladas": int(indice), "qtd_analisadas": 10, "instituicao": inst, "cnpj_if": cnpj, "indice": indice,
                "qtd_reclamacoes_respondidas": 100, "qtd_procedentes": 50, "qtd_clientes_total": 1_000_000, "data_base": "202609",
                "grupo": grupo, "is_safra": grupo == "safra"}
    _silver(st, "bcb_reclamacoes/202609.parquet", [
        r("SAFRA (conglomerado)", 200.0), r("ITAU (conglomerado)", 30.0), r("BRADESCO (conglomerado)", 50.0),
        r("OUTRO BANCO (conglomerado)", 5.0),
        r("BCO SAFRA S.A.", 20.0, "safra", "Banco/financeira", "58160789"),
    ])


def silver_estban(st: Settings) -> None:
    linhas = []
    for cnpj, grupo, ags in (("58160789", "safra", ["a1", "a2"]), ("60872504", "pares", ["b1", "b2", "b3"]), ("60746948", "pares", ["c1"])):
        for ag in ags:
            linhas.append({"data_base": "202606", "uf": "SP", "cod_municipio": f"m{ag}", "cnpj": cnpj, "agencia": ag, "saldo": 1.0,
                           "verbete": "160", "nivel": "agencia", "grupo": grupo, "is_safra": grupo == "safra"})
        linhas.append({"data_base": "202606", "uf": "SP", "cod_municipio": "m1", "cnpj": cnpj, "agencia": None, "saldo": 500.0,
                       "verbete": "160", "nivel": "municipio", "grupo": grupo, "is_safra": grupo == "safra"})
    _silver(st, "bcb_estban/202606.parquet", linhas)


# --- fundos: ver comentarios do teste de fundos para os valores esperados
F1, F2, F3, F4, F5, F6, F8 = (
    "11111111000111", "22222222000122", "33333333000133", "44444444000144", "55555555000155", "66666666000166",
    "88888888000188")
SAFRA_ASSET, TERCEIRO = "65913436000100", "99999999000199"


def silver_fundos(st: Settings) -> None:
    _silver(st, "cvm_fundos_cad/registro_classe/202611.parquet", [
        {"cnpj_classe": F1, "cnpj_administrador": SAFRA_ASSET, "cpf_cnpj_gestor": SAFRA_ASSET},   # adm + gestor
        {"cnpj_classe": F2, "cnpj_administrador": SAFRA_ASSET, "cpf_cnpj_gestor": TERCEIRO},      # so adm
        {"cnpj_classe": F3, "cnpj_administrador": TERCEIRO, "cpf_cnpj_gestor": SAFRA_ASSET},      # so gestor
        {"cnpj_classe": F4, "cnpj_administrador": TERCEIRO, "cpf_cnpj_gestor": TERCEIRO},         # fora do grupo
        {"cnpj_classe": F5, "cnpj_administrador": SAFRA_ASSET, "cpf_cnpj_gestor": TERCEIRO},      # so subclasses no informe
    ])
    # F8: so existe no cadastro de FUNDO (nao tem registro de classe com o mesmo CNPJ)
    _silver(st, "cvm_fundos_cad/registro_fundo/202611.parquet", [
        {"cnpj_fundo": F8, "cnpj_administrador": SAFRA_ASSET, "cpf_cnpj_gestor": TERCEIRO},
    ])
    _silver(st, "cvm_fundos_cad/cad_fi_legado/202611.parquet", [
        {"cnpj_fundo": F1, "cnpj_admin": TERCEIRO, "cpf_cnpj_gestor": TERCEIRO},                  # defasado: o cadastro novo prevalece
        {"cnpj_fundo": F6, "cnpj_admin": SAFRA_ASSET, "cpf_cnpj_gestor": TERCEIRO},               # so no legado
    ])

    def inf(d, cnpj, pl_, sub=None):
        return {"tp_fundo_classe": "CLASSES - FIF", "cnpj_fundo_classe": cnpj, "id_subclasse": sub, "dt_comptc": d, "vl_patrim_liq": float(pl_)}
    dias_setembro = [date(2026, 9, 29), date(2026, 9, 30)]
    linhas = []
    for d in dias_setembro:
        linhas += [inf(d, F1, 100e6), inf(d, F1, 100e6, "S1"),          # classe + subclasse: conta so a classe
                   inf(d, F2, 50e6), inf(d, F3, 30e6), inf(d, F4, 999e6),
                   inf(d, F5, 20e6, "S1"), inf(d, F5, 10e6, "S2"),       # sem linha de classe: soma das subclasses
                   inf(d, F6, 5e6), inf(d, F8, 8e6)]
    # outubro: dia 29 cheio, dia 30 parcial (so 1 de 7 fundos) -> o ultimo dia considerado e 29
    linhas += [inf(date(2026, 10, 29), c, 1e6) for c in (F1, F2, F3, F4, F5, F6, F8)]
    linhas += [inf(date(2026, 10, 30), F1, 1e6)]
    _silver(st, "cvm_fundos_informe/202609.parquet", linhas)

    def cda(holder, held, valor, d=date(2026, 9, 30)):
        return {"cnpj_fundo_classe": holder, "dt_comptc": d, "cnpj_fundo_classe_cota": held, "vl_merc_pos_final": float(valor),
                "tp_aplic": "Cotas de Fundos", "bloco": "fi_BLC_2"}
    _silver(st, "cvm_fundos_cda/202609.parquet", [
        cda(F1, F2, 20e6),    # F1 e F2 sao administrados: sai da visao administrado, e da ambos
        cda(F1, F3, 7e6),     # F1 e F3 sao geridos: sai da visao gerido, e da ambos; F3 nao e administrado
        cda(F4, F1, 5e6),     # detentor fora do grupo: ignorado
        cda(F1, F4, 3e6),     # cotizado fora do grupo: ignorado
        cda(F1, F8, 3e6),     # F8 so e conhecido pelo cadastro de FUNDO: o cruzamento usa fundos E classes
        # posicoes confidenciais (arquivo CONFID pos-RCVM 175): "Cotas de Fundos" SEM CNPJ do ativo
        {"cnpj_fundo_classe": F2, "dt_comptc": date(2026, 9, 30), "cnpj_fundo_classe_cota": None, "vl_merc_pos_final": 11e6,
         "tp_aplic": "Cotas de Fundos", "bloco": "fi_CONFID"},
        {"cnpj_fundo_classe": F1, "dt_comptc": date(2026, 9, 30), "cnpj_fundo_classe_cota": None, "vl_merc_pos_final": 9e6,
         "tp_aplic": "Cotas de Fundos", "bloco": "fi_CONFID"},
        {"cnpj_fundo_classe": F4, "dt_comptc": date(2026, 9, 30), "cnpj_fundo_classe_cota": None, "vl_merc_pos_final": 77e6,
         "tp_aplic": "Cotas de Fundos", "bloco": "fi_CONFID"},      # detentor fora do grupo: ignorado
        {"cnpj_fundo_classe": F1, "dt_comptc": date(2026, 9, 30), "cnpj_fundo_classe_cota": None, "vl_merc_pos_final": 55e6,
         "tp_aplic": "Debêntures", "bloco": "fi_CONFID"},           # nao e cota: ignorado
    ])


def silver_ses(st: Settings) -> None:
    def seg(db, fip, grupo, pd, pg, si, so):
        return {"damesano": db, "codigo_fip": fip, "premio_direto": pd, "premio_ganho": pg, "sinistro_direto": si,
                "sinistro_ocorrido": so, "grupo": grupo}
    _silver(st, "susep_ses/seguros/202609.parquet", [
        seg("202601", "01627", "safra", 10.0, 10.0, 1.0, 1.0), seg("202602", "01627", "safra", 12.0, 10.0, 2.0, 2.0),
        seg("202602", "01627", "safra", 3.0, 0.0, 0.0, 0.0),  # outro ramo da mesma empresa/mes: soma
        seg("202602", "05444", "pares", 50.0, 40.0, 10.0, 8.0), seg("202602", "05321", "pares", 50.0, 40.0, 20.0, 16.0),
        seg("202602", "03182", None, 999.0, 999.0, 999.0, 999.0),  # candidato nao aprovado: sem grupo, fora
    ])
    _silver(st, "susep_ses/pl_margem/202609.parquet", [
        {"codigo_fip": "01627", "damesano": "202602", "pl": 200.0, "plajustado": 180.0, "margem": 10.0, "grupo": "safra"},
        {"codigo_fip": "01627", "damesano": "202609", "pl": 4e6, "plajustado": 3e6, "margem": 1e6, "grupo": "safra"},
        {"codigo_fip": "05444", "damesano": "202609", "pl": 999e6, "plajustado": 1.0, "margem": 1.0, "grupo": "pares"}])
    _silver(st, "susep_ses/prov/202609.parquet", [
        {"codigo_fip": "01627", "damesano": "202602", "valor": 300.0}, {"codigo_fip": "01627", "damesano": "202609", "valor": 30e6}])
    _silver(st, "susep_ses/cadastro/202609.parquet", [{"codigo_fip": "01627", "nome": "SAFRA SEGUROS GERAIS S.A."}])


@pytest.fixture
def gold_st(tmp_path):
    st = _settings(tmp_path)
    for f in (silver_ifdata, silver_cosif, silver_juros, silver_reclamacoes, silver_estban, silver_fundos, silver_ses):
        f(st)
    r = criar_gold(st)
    assert not r.erros, r.erros
    assert set(r.criadas) == set(VIEWS_GOLD) and not r.ignoradas
    return st


# ---------------------------------------------------------------- dimensao e estrutura

def test_dim_entidade_prefere_safra_e_so_aprovados(tmp_path):
    st = _settings(tmp_path)
    dim = {(t, i): (g, e, o) for t, i, g, e, o in construir_dim_entidade(st)}
    assert dim[("conglomerado", "C0080109")][:2] == ("safra", "banco_safra")
    assert dim[("cnpj_raiz", "65913436")][:2] == ("safra", "asset_safra")
    assert dim[("fip", "05444")][:2] == ("pares", "bradesco")
    assert ("fip", "03182") not in dim  # sugestao nao aprovada


def test_views_sao_views_sobre_parquet_sem_copiar_dados(gold_st):
    con = conectar_gold(gold_st)
    try:
        sql = con.execute("select sql from duckdb_views() where schema_name = 'silver' and view_name = 'ifdata'").fetchone()[0]
        assert "read_parquet" in sql
        tabelas = {r[0] for r in con.execute("select table_name from duckdb_tables()").fetchall()}
    finally:
        con.close()
    assert tabelas == {"dim_entidade", "dim_reclamacao_nome", "referencias_fundos"}  # so configuracao
    assert gold_st.gold_db_path.stat().st_size < 5_000_000


def test_views_com_silver_ausente_sao_ignoradas(tmp_path):
    st = _settings(tmp_path)
    silver_ifdata(st)
    r = criar_gold(st)
    assert {"capital", "carteira"} <= set(r.criadas)
    assert "faltam fontes silver: cosif" in r.ignoradas["balanco_dre"]
    assert "juros" in r.ignoradas and "fundos" in r.ignoradas and "seguros" in r.ignoradas
    assert resumo_safra(conectar_gold(st))  # resumo funciona so com o que existe


def test_todas_as_views_gold_tem_data_base_grupo_entidade(gold_st):
    con = conectar_gold(gold_st)
    try:
        for v in ("capital", "balanco_dre", "carteira", "juros", "reclamacoes", "agencias", "fundos", "fundos_conciliacao", "seguros"):
            cols = [d[0] for d in con.execute(f"select * from gold.{v} limit 0").description]
            assert {"data_base", "grupo", "entidade"} <= set(cols), v
            assert con.execute(f"select typeof(data_base) from gold.{v} limit 1").fetchone()[0] == "DATE"
    finally:
        con.close()


# ---------------------------------------------------------------- capital / balanco / carteira

def test_capital_safra_vs_pares_exclui_nao_configurados(gold_st):
    linhas = _q(gold_st, "select * from gold.capital order by entidade")
    assert [l["entidade"] for l in linhas] == ["banco_safra", "bradesco", "itau_unibanco"]  # Avenue de fora
    s = linhas[0]
    assert s["data_base"] == date(2026, 6, 30) and s["grupo"] == "safra"
    assert s["indice_basileia"] == 18.0 and s["indice_basileia_mediana_pares"] == 15.5
    assert s["indice_basileia_dif_pares"] == 2.5
    assert s["patrimonio_referencia"] == 29e9 and s["rwa_total"] == 160e9 and s["rwa_mercado"] == 11e9
    assert s["indice_capital_principal"] == 11.0 and s["indice_nivel_1"] == 15.0


def test_balanco_dre_roe_roa_anualizados_e_lucro_trimestral(gold_st):
    por_db = {l["data_base"]: l for l in _q(gold_st, "select * from gold.balanco_dre where entidade = 'banco_safra'")}
    t1, t2 = por_db[date(2026, 3, 31)], por_db[date(2026, 6, 30)]
    assert t1["lucro_liquido_trimestre"] == 100 and t2["lucro_liquido_trimestre"] == 150
    # 202606: lucro acum. 250 em 6 meses -> 500 anualizado; PL base = media(1200, 800 de dez/2025) = 1000
    assert t2["pl_base_roe"] == 1000 and t2["base_media"] is True and t2["roe_anualizado"] == 50.0
    assert t2["roa_anualizado"] == round(100 * 250 * 2 / ((12000 + 9000) / 2), 3)
    assert por_db[date(2025, 12, 31)]["base_media"] is False and por_db[date(2025, 12, 31)]["lucro_liquido_trimestre"] is None
    # Cosif: ativo = 1000000009 + 2000000008; lucro = credor + devedor
    assert t1["ativo_total_cosif"] == 10000 and t1["dif_ativo_cosif_vs_ifdata"] == 0
    assert t2["lucro_liquido_acum_cosif"] == 250 and t2["patrimonio_liquido_cosif"] == 950 + 330 - 80
    # pares: ROE = 50*2/500 = 0.2 (sem PL de dez/2025 usa o PL do periodo); mediana dos pares
    assert _q(gold_st, "select roe_anualizado_mediana_pares m from gold.balanco_dre where entidade = 'banco_safra' "
                       "and data_base = '2026-06-30'")[0]["m"] == 20.0


def test_carteira_cortes_e_participacao(gold_st):
    linhas = {(l["corte"], l["categoria"]): l for l in _q(gold_st, "select * from gold.carteira where grupo = 'safra'")}
    assert linhas[("indexador", "Prefixado")]["participacao_na_carteira"] == 60.0
    assert linhas[("porte", "Grande")]["valor"] == 250 and linhas[("porte", "Grande")]["participacao_na_carteira"] == 25.0
    assert linhas[("modalidade", "Capital de Giro")]["valor"] == 175  # so a coluna Total
    assert not any(c == "nivel_risco" for c, _ in linhas)  # sem relatorio de nivel de risco no silver


# ---------------------------------------------------------------- juros / reclamacoes / agencias

def test_juros_mediana_dos_pares_por_entidade(gold_st):
    linhas = {l["entidade"]: l for l in _q(gold_st, "select * from gold.juros")}
    assert set(linhas) == {"banco_safra", "itau_unibanco", "bradesco"}  # 99999999 (sem grupo) fora
    s = linhas["banco_safra"]
    assert s["taxa_ao_ano"] == 70 and s["n_instituicoes"] == 2  # media das duas linhas da entidade
    assert s["mediana_pares_ao_ano"] == 45 and s["n_pares"] == 2 and s["dif_ao_ano_vs_mediana_pares"] == 25
    assert s["data_base"] == date(2026, 10, 31)


def test_reclamacoes_rotulo_por_nome_exato_e_por_cnpj(gold_st):
    linhas = _q(gold_st, "select * from gold.reclamacoes order by tipo_instituicao, entidade")
    assert {(l["tipo_instituicao"], l["entidade"]) for l in linhas} == {
        ("Conglomerado", "banco_safra"), ("Conglomerado", "itau_unibanco"), ("Conglomerado", "bradesco"),
        ("Banco/financeira", "banco_safra"),
    }  # "OUTRO BANCO" fica sem grupo
    cong = {l["entidade"]: l for l in linhas if l["tipo_instituicao"] == "Conglomerado"}
    assert cong["banco_safra"]["indice_mediana_pares"] == 40 and cong["banco_safra"]["indice_dif_pares"] == 160
    # o indice vem do ranking do BCB (nao e recalculado); `indice_recalculado` so confere: extrapoladas por milhao de clientes
    assert cong["banco_safra"]["indice"] == 200 and cong["banco_safra"]["indice_recalculado"] == 200
    assert cong["banco_safra"]["categoria"] == "Top 15" and cong["banco_safra"]["qtd_procedentes_extrapoladas"] == 200


def test_agencias_contagem_e_mediana_dos_pares(gold_st):
    linhas = {l["entidade"]: l for l in _q(gold_st, "select * from gold.agencias")}
    assert linhas["banco_safra"]["n_agencias"] == 2 and linhas["itau_unibanco"]["n_agencias"] == 3
    assert linhas["banco_safra"]["n_agencias_mediana_pares"] == 2  # mediana de (3, 1)
    assert linhas["banco_safra"]["estban_operacoes_credito"] == 500


# ---------------------------------------------------------------- fundos

def test_fundos_pl_deduplicado_por_visao(gold_st):
    """Setembro (30/09): classe+subclasse conta so a classe (F1=100); F5 so tem subclasses (20+10=30).
    administrado = F1+F2+F5+F6(legado)+F8(so cadastro de fundo) = 100+50+30+5+8 = 193, menos cotas F1->F2 (20) e F1->F8 (3) = 170
    gerido       = F1+F3 = 130, menos cotas F1->F3 (7) = 123
    ambos        = F1+F2+F3+F5+F6+F8 = 223, menos 20+7+3 = 193. F4 (terceiros) e posicoes fora do conjunto ficam de fora.
    Cotas confidenciais (sem CNPJ do ativo) dos detentores da visao: adm 11+9, gerido 9, ambos 20."""
    f = {(l["data_base"], l["visao"]): l for l in _q(gold_st, "select * from gold.fundos")}
    adm, ger, amb = (f[(date(2026, 9, 30), v)] for v in ("administrado", "gerido", "ambos"))
    assert (adm["n_fundos"], adm["pl_bruto"], adm["posicoes_em_cotas_do_grupo"], adm["pl_deduplicado"]) == (5, 193e6, 23e6, 170e6)
    assert (ger["n_fundos"], ger["pl_bruto"], ger["posicoes_em_cotas_do_grupo"], ger["pl_deduplicado"]) == (2, 130e6, 7e6, 123e6)
    assert (amb["n_fundos"], amb["pl_bruto"], amb["posicoes_em_cotas_do_grupo"], amb["pl_deduplicado"]) == (6, 223e6, 30e6, 193e6)
    assert (adm["cotas_confidenciais_sem_cnpj"], ger["cotas_confidenciais_sem_cnpj"], amb["cotas_confidenciais_sem_cnpj"]) == (20e6, 9e6, 20e6)
    assert adm["pl_deduplicado_minimo"] == 150e6 and amb["pl_deduplicado_minimo"] == 173e6
    assert adm["data_cda"] == date(2026, 9, 30) and adm["mes_encerrado"] is True
    assert adm["n_fundos_com_cda"] == 2  # F1 e F2 tem linhas no CDA


def test_fundos_cruza_cotas_com_cnpj_de_fundo_e_de_classe(gold_st):
    papel = {l["cnpj"]: l for l in _q(gold_st, "select * from gold.fundos_papel")}
    assert papel[F8]["administrado"] is True            # so em registro_fundo
    assert papel[F1]["administrado"] and papel[F1]["gerido"]  # cadastro de classe vence o legado defasado
    assert papel[F6]["administrado"] and F4 not in papel


def test_fundos_ultimo_dia_do_mes_ignora_dia_parcial(gold_st):
    datas = {l["data_base"] for l in _q(gold_st, "select data_base from gold.fundos")}
    assert datas == {date(2026, 9, 30), date(2026, 10, 29)}  # 30/10 tinha so 1 de 8 fundos
    out = _q(gold_st, "select * from gold.fundos where visao = 'ambos' and data_base = '2026-10-29'")[0]
    assert out["pl_bruto"] == 6e6 and out["posicoes_em_cotas_do_grupo"] == 0 and out["data_cda"] is None


def test_fundos_decomposicao_por_papel(gold_st):
    d = {l["papel"]: l for l in _q(gold_st, "select * from gold.fundos_decomposicao where data_base = '2026-09-30'")}
    assert d["so_administrado"]["pl_bruto"] == 93e6 and d["so_administrado"]["n_fundos"] == 4  # F2+F5+F6+F8
    assert d["so_gerido"]["pl_bruto"] == 30e6 and d["administra_e_gere"]["pl_bruto"] == 100e6  # F3 / F1
    assert sum(x["pl_bruto"] for x in d.values()) == 223e6  # = visao `ambos`


def test_fundos_conciliacao_a_e_b_com_decomposicao(gold_st):
    c = {x["id"]: x for x in conciliacao_fundos(conectar_gold(gold_st))}
    assert set(c) == {"fundos_administrados", "total"}
    a, t = c["fundos_administrados"], c["total"]
    # (a) administrado dedup 170 vs 150 (mil reais -> reais)
    assert a["valor_referencia"] == 150e6 and a["valor_gold"] == 170e6 and a["diferenca"] == 20e6
    assert a["diferenca_pct"] == 13.333 and a["valor_gold_minimo"] == 150e6 and a["diferenca_minimo_pct"] == 0
    # (b) administrado dedup + PL SES das seguradoras/previdencia Safra (so grupo safra: 4e6) vs 150+10
    assert t["componentes"] == "fundos_administrados + previdencia_consolidada" and t["valor_referencia"] == 160e6
    assert t["pl_ses_safra"] == 4e6 and t["valor_gold"] == 174e6 and t["diferenca"] == 14e6 and t["diferenca_pct"] == 8.75
    assert t["valor_gold_minimo"] == 154e6 and t["diferenca_minimo_pct"] == -3.75
    assert t["provisoes_ses_safra"] == 30e6 and t["diferenca_com_provisoes"] == 170e6 + 30e6 - 160e6
    # decomposicao e administracao fiduciaria para terceiros (administrador Safra, gestor de fora)
    assert (a["pl_so_administrado"], a["pl_so_gerido"], a["pl_administra_e_gere"]) == (93e6, 30e6, 100e6)
    assert a["pl_adm_safra_gestor_externo"] == a["pl_so_administrado"]
    # mudar o yaml e recriar a gold muda a conciliacao
    ref = gold_st.referencias_path()
    dados = yaml.safe_load(ref.read_text(encoding="utf-8"))
    dados["referencias"]["fundos_administrados"]["valor_mil_reais"] = 170000
    ref.write_text(yaml.safe_dump(dados), encoding="utf-8")
    criar_gold(gold_st)
    c2 = {x["id"]: x for x in conciliacao_fundos(conectar_gold(gold_st))}
    assert c2["fundos_administrados"]["diferenca"] == 0 and c2["fundos_administrados"]["diferenca_pct"] == 0


def test_referencias_invalidas_sao_rejeitadas():
    from safra_dados.gold import referencias_fundos
    base = {"referencias": {"a": {"data_referencia": "2026-09-30", "valor_mil_reais": 1},
                            "b": {"data_referencia": "2026-06-30", "valor_mil_reais": 2}}}
    with pytest.raises(ValueError, match="inexistentes"):
        referencias_fundos({**base, "conciliacoes_fundos": [{"id": "x", "referencia": ["z"], "visao": "ambos"}]})
    with pytest.raises(ValueError, match="mistura datas"):
        referencias_fundos({**base, "conciliacoes_fundos": [{"id": "x", "referencia": ["a", "b"], "visao": "ambos"}]})
    with pytest.raises(ValueError, match="visao invalida"):
        referencias_fundos({**base, "conciliacoes_fundos": [{"id": "x", "referencia": ["a"], "visao": "tudo"}]})
    (linha,) = referencias_fundos({**base, "conciliacoes_fundos": [{"id": "x", "referencia": ["a"], "visao": "gerido"}]})
    assert linha[4] == 1000.0  # mil reais -> reais


# ---------------------------------------------------------------- formatacao

def test_formatacao_3_casas_ptbr():
    from safra_dados.gold import _bi, _num, _pct
    assert _pct(18.141) == "18,141%" and _pct(5) == "5,000%" and _pct(None) == "n/d"
    assert _bi(29.117e9) == "R$ 29,117 bi" and _bi(1234.5e9) == "R$ 1.234,500 bi" and _bi(None) == "n/d"
    assert _num(207.93) == "207,930"


def test_resumos_da_cli_usam_3_casas(gold_st):
    linhas = resumo_safra(conectar_gold(gold_st))
    import re
    for linha in linhas:
        for m in re.findall(r"R\$ [\d.]+,(\d+) bi", linha):
            assert len(m) == 3, linha
        for m in re.findall(r"[\d.]+,(\d+)%", linha):
            assert len(m) == 3, linha
    texto = "\n".join(linhas)
    assert "[fundos]" in texto and "minimo" in texto


# ---------------------------------------------------------------- seguros

def test_seguros_soma_ramos_acumula_no_ano_e_so_aprovados(gold_st):
    linhas = _q(gold_st, "select * from gold.seguros order by data_base, entidade")
    assert {l["codigo_fip"] for l in linhas} == {"01627", "05444", "05321"}  # 03182 (nao aprovado) fora
    fev = next(l for l in linhas if l["codigo_fip"] == "01627" and l["data_base"] == date(2026, 2, 28))
    assert fev["entidade"] == "seguradora_safra" and fev["seguradora"] == "SAFRA SEGUROS GERAIS S.A."
    assert fev["premio_direto"] == 15 and fev["premio_direto_ano"] == 25  # 12+3 em fev, +10 de jan
    assert fev["sinistralidade"] == 20.0 and fev["sinistralidade_ano"] == 15.0  # (1+2)/(10+10)
    assert fev["provisoes_tecnicas"] == 300 and fev["patrimonio_liquido"] == 200 and fev["pl_ajustado"] == 180
    # pares em fev: bradesco 8/40=0.2, itau 16/40=0.4 -> mediana 0.3
    assert fev["sinistralidade_ano_mediana_pares"] == 30.0 and fev["sinistralidade_ano_dif_pares"] == -15.0


# ---------------------------------------------------------------- resumo / exportacao

def test_resumo_do_safra_cobre_as_views(gold_st):
    texto = "\n".join(resumo_safra(conectar_gold(gold_st)))
    for marca in ("[capital]", "[balanco_dre]", "[carteira]", "[juros]", "[reclamacoes]", "[agencias]", "[fundos]", "[seguros]"):
        assert marca in texto, marca
    assert "18,000%" in texto  # Basileia do Safra, 3 casas


def test_exportar_csv_e_xlsx(gold_st):
    csv = exportar_view(gold_st, "capital", "csv")
    assert csv == gold_st.exports_dir / "capital.csv"
    cab = csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert cab[:3] == ["data_base", "grupo", "entidade"]
    assert len(csv.read_text(encoding="utf-8").splitlines()) == 4  # cabecalho + 3 entidades
    xlsx = exportar_view(gold_st, "fundos", "xlsx")
    assert xlsx.read_bytes()[:2] == b"PK"  # arquivo zip (xlsx)
    with pytest.raises(ValueError):
        exportar_view(gold_st, "inexistente", "csv")
    with pytest.raises(ValueError):
        exportar_view(gold_st, "capital", "pdf")


def test_gold_e_idempotente(gold_st):
    antes = _q(gold_st, "select count(*) n from gold.fundos")[0]["n"]
    criar_gold(gold_st)
    criar_gold(gold_st)
    assert _q(gold_st, "select count(*) n from gold.fundos")[0]["n"] == antes


# ---------------------------------------------------------------- calendario / atualizar

def _cp(fonte, data_bases: dict, ultima=None):
    cp = Checkpoint(fonte=fonte, ultima_data_base_ok=ultima, data_bases=data_bases)
    return cp


def _db(falhas=0, quando="2026-11-19T10:00:00+00:00"):
    return {"arquivos_ok": 1, "arquivos_falha": falhas, "atualizado_em": quando}


def test_calendarios_cobrem_todas_as_fontes():
    from safra_dados.cli import _FONTE_ALIASES
    assert set(_FONTE_ALIASES.values()) == set(CALENDARIOS)


def test_vencida_pelo_calendario_de_publicacao():
    hoje = date(2026, 11, 19)
    # IF.data: em 19/11/2026 a esperada e 202606 (202609 so sai em 29/11)
    assert vencida("bcb_ifdata", _cp("bcb_ifdata", {"202606": _db()}), hoje)[0] is False
    assert vencida("bcb_ifdata", _cp("bcb_ifdata", {"202603": _db()}), hoje)[0] is True
    assert vencida("bcb_ifdata", _cp("bcb_ifdata", {"202606": _db()}), date(2026, 12, 1))[0] is True  # 202609 saiu
    # coleta com falha continua vencida
    assert vencida("bcb_ifdata", _cp("bcb_ifdata", {"202606": _db(falhas=1)}), hoje)[0] is True
    # sem checkpoint: vencida
    assert vencida("bcb_cosif", _cp("bcb_cosif", {}), hoje)[0] is True
    # DFP anual: ano anterior so depois de 30/04
    assert vencida("cvm_dfp", _cp("cvm_dfp", {"2025": _db()}), hoje)[0] is False
    assert vencida("cvm_dfp", _cp("cvm_dfp", {"2024": _db()}), hoje)[0] is True
    assert vencida("cvm_dfp", _cp("cvm_dfp", {"2024": _db()}), date(2026, 3, 1))[0] is False


def test_vencida_por_refresh():
    hoje = date(2026, 11, 19)
    agora = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
    recente = _cp("cvm_fundos_informe", {"202611": _db(quando="2026-11-19T08:00:00+00:00")})
    antigo = _cp("cvm_fundos_informe", {"202611": _db(quando="2026-11-17T08:00:00+00:00")})
    assert vencida("cvm_fundos_informe", recente, hoje, agora)[0] is False   # refresh diario, coletado hoje
    assert vencida("cvm_fundos_informe", antigo, hoje, agora)[0] is True
    # sem refresh: coleta antiga da data-base esperada nao vence
    assert vencida("bcb_unicad", _cp("bcb_unicad", {"202610": _db(quando="2026-11-01T08:00:00+00:00")}), hoje, agora)[0] is False


def test_desde_para_atualizar():
    cp = _cp("bcb_ifdata", {}, ultima="202606")
    assert desde_para_atualizar("bcb_ifdata", cp, "2015-03") == "2026-06"
    assert desde_para_atualizar("bcb_ifdata", _cp("bcb_ifdata", {}), "2015-03") == "2015-03"
    assert desde_para_atualizar("cvm_dfp", _cp("cvm_dfp", {}, ultima="2025"), "2021-01") == "2025-01"
    # `desde` filtra o conteudo do SES e do RI: nunca encurta o historico
    assert desde_para_atualizar("susep_ses", _cp("susep_ses", {}, ultima="202609"), "2020-01") == "2020-01"
    assert desde_para_atualizar("safra_ri", _cp("safra_ri", {}, ultima="202611"), "2023-01") == "2023-01"


def test_cli_atualizar_dry_run_gold_e_exportar(tmp_path, monkeypatch):
    from safra_dados.cli import app

    st = _settings(tmp_path)
    for f in (silver_ifdata, silver_cosif, silver_fundos, silver_ses):
        f(st)
    monkeypatch.setenv("SAFRA_DADOS_CONFIG_DIR", str(st.config_dir))
    monkeypatch.setenv("SAFRA_DADOS_HOME", str(st.data_root))
    runner = CliRunner()

    r = runner.invoke(app, ["atualizar", "--dry-run"])
    assert r.exit_code == 0, r.output
    assert "VENCIDA bcb_ifdata" in r.output and "dry-run:" in r.output
    logs = list(st.logs_dir.glob("atualizar_*.log"))
    assert len(logs) == 1 and "dry-run:" in logs[0].read_text(encoding="utf-8")
    assert not st.gold_db_path.exists()  # dry-run nao cria a gold

    r = runner.invoke(app, ["gold"])
    assert r.exit_code == 0, r.output
    assert "[capital]" in r.output and "[conciliacao] fundos_administrados" in r.output and "ignorada juros" in r.output

    r = runner.invoke(app, ["exportar", "--view", "capital", "--formato", "csv"])
    assert r.exit_code == 0 and (st.exports_dir / "capital.csv").is_file()
    r = runner.invoke(app, ["exportar", "--view", "juros", "--formato", "csv"])
    assert r.exit_code != 0  # view ausente (silver de juros nao existe)
