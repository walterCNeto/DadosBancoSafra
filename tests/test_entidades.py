from __future__ import annotations

import polars as pl

from safra_dados.entidades import (
    carregar_mapa_entidades, marcar_grupos, normalizar_identificador, normalizar_nome,
)

ENTIDADES = {
    "grupos": {
        "safra": {
            "cnpj_raiz": "58160789",
            "entidades": {
                "banco_safra": {"cnpj_raiz": "58160789", "codigo_conglomerado_prudencial": "C0080109"},
                "banco_j_safra": {"cnpj_raiz": "03017677"},
                "banco_alfa": {"cnpj_raiz": None, "busca_nome": ["alfa"]},
            },
        },
        "pares": {"entidades": {"bb": {"cnpj_raiz": None, "busca_nome": ["banco do brasil s.a."]}}},
    }
}

RESOLVIDAS = {
    "grupos": {
        "safra": {
            "entidades": {
                "banco_safra": {"status": "confirmado", "cnpjs_relacionados": ["65913436"]},
                # sugestao por nome: NAO pode marcar
                "banco_alfa": {"status": "sugestao", "cnpj": "60770336"},
            }
        },
        "pares": {
            "entidades": {
                "bb": {"status": "resolvido", "cnpj": "00000000", "codigo_conglomerado_prudencial": "C0080329"},
                "itau": {"status": "ambiguo", "cnpj": "60872504"},
            }
        },
    }
}


def test_mapa_usa_config_e_somente_resolvidas_utilizaveis(settings_factory):
    mapa = carregar_mapa_entidades(settings_factory(ENTIDADES, RESOLVIDAS))
    assert mapa.por_identificador["58160789"] == "safra"
    assert mapa.por_identificador["03017677"] == "safra"
    assert mapa.por_identificador["C0080109"] == "safra"
    assert mapa.por_identificador["65913436"] == "safra"
    assert mapa.por_identificador["C0080329"] == "pares"
    assert "60770336" not in mapa.identificadores  # sugestao
    assert "60872504" not in mapa.identificadores  # ambiguo


def test_aprovado_manual_libera_sugestao(settings_factory):
    resolvidas = {"grupos": {"safra": {"entidades": {"x": {"status": "sugestao", "cnpj": "11111111", "aprovado": True}}}}}
    mapa = carregar_mapa_entidades(settings_factory(ENTIDADES, resolvidas))
    assert mapa.por_identificador["11111111"] == "safra"


def test_marcar_grupos_so_por_identificador_nunca_por_nome(settings_factory):
    mapa = carregar_mapa_entidades(settings_factory(ENTIDADES, RESOLVIDAS))
    df = pl.DataFrame(
        {
            "cnpj": ["58160789", "03017677", "00000000", "99999999", "58.160.789/0001-78"],
            "cod_conglomerado": [None, None, None, None, None],
            "nome": ["BCO SAFRA S.A.", "BANCO J. SAFRA", "BANCO DO BRASIL S.A.", "SAFRA FALSO LTDA", "X"],
        },
        schema_overrides={"cod_conglomerado": pl.String},
    )
    out = marcar_grupos(df, mapa, ["cnpj", "cod_conglomerado"])
    assert out["grupo"].to_list() == ["safra", "safra", "pares", None, "safra"]
    assert out["is_safra"].to_list() == [True, True, False, False, True]


def test_marcar_grupos_por_conglomerado_e_prioridade_safra(settings_factory):
    mapa = carregar_mapa_entidades(settings_factory(ENTIDADES, RESOLVIDAS))
    df = pl.DataFrame({"a": ["C0080329", "C0080329"], "b": ["C0080109", None]})
    out = marcar_grupos(df, mapa, ["a", "b"])
    assert out["grupo"].to_list() == ["safra", "pares"]


def test_marcar_grupos_sem_colunas_de_identificador(settings_factory):
    mapa = carregar_mapa_entidades(settings_factory(ENTIDADES, RESOLVIDAS))
    out = marcar_grupos(pl.DataFrame({"x": [1]}), mapa, ["cnpj"])
    assert out["grupo"].to_list() == [None] and out["is_safra"].to_list() == [False]


def test_normalizacoes():
    assert normalizar_nome("Ativo Realizável") == "ATIVO REALIZAVEL"
    assert normalizar_identificador("58.160.789/0001-78") == "58160789"
    assert normalizar_identificador("c0080109") == "C0080109"
