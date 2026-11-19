from __future__ import annotations

import polars as pl

from safra_dados.entidades import carregar_entidades_safra, marcar_is_safra, normalizar_nome


ENTIDADES = {
    "grupos": {
        "safra": {
            "cnpj_raiz": "58160789",
            "entidades": {
                "banco_safra": {"cnpj_raiz": "58160789", "busca_nome": ["safra"]},
                "banco_alfa": {"cnpj_raiz": None, "busca_nome": ["alfa"]},
            },
        }
    }
}

RESOLVIDAS = {
    "grupos": {
        "safra": {
            "entidades": {
                "banco_safra": {"codigo_conglomerado_prudencial": "C0080109"},
                "banco_alfa": {"cnpj": "60770336"},
            }
        }
    }
}


def test_carregar_entidades_safra_combina_config_e_resolvidas(settings_factory):
    settings = settings_factory(ENTIDADES, RESOLVIDAS)
    entidades = carregar_entidades_safra(settings)
    assert "58160789" in entidades.identificadores
    assert "C0080109" in entidades.identificadores
    assert "60770336" in entidades.identificadores
    assert "SAFRA" in entidades.termos_nome
    assert "ALFA" in entidades.termos_nome


def test_marcar_is_safra_por_identificador_e_nome(settings_factory):
    settings = settings_factory(ENTIDADES, RESOLVIDAS)
    entidades = carregar_entidades_safra(settings)

    df = pl.DataFrame(
        {
            "cnpj": ["58160789", "60770336", "00000000"],
            "cod_conglomerado": ["C0080109", None, None],
            "nome": ["BCO SAFRA S.A.", "BCO ALFA S.A.", "BCO DO BRASIL S.A."],
        }
    )
    marcado = marcar_is_safra(df, entidades, ["cnpj", "cod_conglomerado"], coluna_nome="nome")
    assert marcado["is_safra"].to_list() == [True, True, False]


def test_normalizar_nome_remove_acentos():
    assert normalizar_nome("Ativo Realizável") == "ATIVO REALIZAVEL"
