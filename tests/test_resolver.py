from __future__ import annotations

import yaml

from safra_dados.resolver import resolver_entidade, resolver_entidades


def _row(cod, nome, pru, fin, lider, sit="A"):
    return {"CodInst": cod, "NomeInstituicao": nome, "CodConglomeradoPrudencial": pru,
            "CodConglomeradoFinanceiro": fin, "CnpjInstituicaoLider": lider, "Situacao": sit}


CAD = [
    _row("58160789", "BANCO SAFRA S.A.", "C0080109", "C0010083", "58160789"),
    _row("03017677", "BANCO J. SAFRA S.A.", "C0080109", "C0010083", "58160789"),
    _row("65913436", "SAFRA ASSET CTVM", "C0080109", "C0010083", "58160789"),
    _row("62180047", "SAFRA ASSET MANAGEMENT LTDA", None, None, None, "I"),
    _row("00000000", "BANCO DO BRASIL S.A.", "C0080329", None, "00000000"),
    _row("00000001", "BANCO DO BRASIL S.A.", "C0099999", None, "00000001"),
    _row("90400888", "BANCO SANTANDER (BRASIL) S.A.", "C0080185", None, "90400888"),
    _row("47193149", "SANTANDER LEASING S.A.", "C0080185", None, "90400888"),
]


def test_identificador_confirma_e_coleta_membros_do_conglomerado():
    r = resolver_entidade({"cnpj_raiz": "03017677", "codigo_conglomerado_prudencial": "C0080109"}, CAD, None)
    assert r["status"] == "confirmado"
    assert r["cnpj"] == "03017677"
    assert r["codigo_conglomerado_prudencial"] == "C0080109"
    assert r["codigo_conglomerado_financeiro"] == "C0010083"
    assert {"58160789", "03017677", "65913436"} <= set(r["cnpjs_relacionados"])


def test_sem_identificador_so_sugere_por_nome():
    r = resolver_entidade({"busca_nome": ["safra asset"]}, CAD, None)
    assert r["status"] == "sugestao"
    assert "cnpj" not in r and "cnpjs_relacionados" not in r
    assert len(r["candidatos"]) == 2


def test_pares_resolve_lider_unica_e_membros():
    cfg = {"busca_nome": ["banco santander (brasil) s.a."], "resolver_por_nome_lider": True}
    r = resolver_entidade(cfg, CAD, None)
    assert r["status"] == "resolvido"
    assert r["cnpj"] == "90400888" and r["codigo_conglomerado_prudencial"] == "C0080185"
    assert r["cnpjs_relacionados"] == ["47193149", "90400888"]


def test_pares_ambiguo_nao_preenche():
    r = resolver_entidade({"busca_nome": ["banco do brasil s.a."], "resolver_por_nome_lider": True}, CAD, None)
    assert r["status"] == "ambiguo" and "cnpj" not in r


def test_nao_encontrado_e_falha_api():
    assert resolver_entidade({"busca_nome": ["xyz"]}, CAD, None)["status"] == "nao_encontrado"
    assert resolver_entidade({"cnpj_raiz": "11111111"}, CAD, None)["status"] == "nao_encontrado"
    assert resolver_entidade({"cnpj_raiz": "58160789"}, [], "HTTP 500")["status"] == "falha_api"


def test_resolver_entidades_grava_yaml_preserva_aprovado_e_outros_grupos(settings_factory):
    entidades = {"grupos": {"safra": {"entidades": {"alfa": {"busca_nome": ["safra asset"]}}}}}
    previa = {"grupos": {"safra": {"entidades": {"alfa": {"status": "sugestao", "aprovado": True}}},
                         "outro": {"entidades": {}}}}
    settings = settings_factory(entidades, previa)
    res = resolver_entidades(settings, grupo="safra", anomes="202606", cadastro=CAD, incluir_cvm=False, incluir_susep=False)
    assert res["grupos"]["safra"]["entidades"]["alfa"]["aprovado"] is True
    assert "outro" in res["grupos"]
    gravado = yaml.safe_load(settings.entidades_resolvidas_path().read_text(encoding="utf-8"))
    assert gravado["grupos"]["safra"]["entidades"]["alfa"]["status"] == "sugestao"
