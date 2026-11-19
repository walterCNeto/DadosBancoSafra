from __future__ import annotations

import json

import polars as pl

from safra_dados.connectors.bcb_ifdata import BcbIfDataConnector

ENTIDADES = {
    "grupos": {
        "safra": {
            "cnpj_raiz": "58160789",
            "entidades": {"banco_safra": {"cnpj_raiz": "58160789", "busca_nome": ["safra"]}},
        }
    }
}

FONTES = {"bcb_ifdata": {"base_url": "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata", "tipos_instituicao": [1]}}

CADASTRO = {
    "value": [
        {
            "CodInst": "58160789", "Data": "202306", "NomeInstituicao": "BCO SAFRA S.A.",
            "CodConglomeradoPrudencial": "C0080109", "CodConglomeradoFinanceiro": None,
            "CnpjInstituicaoLider": "58160789", "Situacao": "A",
        },
        {
            "CodInst": "00000000", "Data": "202306", "NomeInstituicao": "BCO DO BRASIL S.A.",
            "CodConglomeradoPrudencial": "C0080329", "CodConglomeradoFinanceiro": None,
            "CnpjInstituicaoLider": "00000000", "Situacao": "A",
        },
    ]
}

VALORES = {
    "value": [
        {
            "TipoInstituicao": 1, "CodInst": "58160789", "AnoMes": "202306",
            "NomeRelatorio": "Resumo", "NumeroRelatorio": "1", "Grupo": "Ativo",
            "Conta": "1.0", "NomeColuna": "saldo", "DescricaoColuna": "Saldo", "Saldo": 270050140145.01,
        },
        {
            "TipoInstituicao": 1, "CodInst": "00000000", "AnoMes": "202306",
            "NomeRelatorio": "Resumo", "NumeroRelatorio": "1", "Grupo": "Ativo",
            "Conta": "1.0", "NomeColuna": "saldo", "DescricaoColuna": "Saldo", "Saldo": 2527637659526.53,
        },
    ]
}


def test_normalize_gera_silver_com_is_safra(settings_factory):
    settings = settings_factory(ENTIDADES, fontes=FONTES)
    connector = BcbIfDataConnector(settings)

    bronze_dir = settings.bronze_fonte_dir("bcb_ifdata", "202306")
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "cadastro.json").write_text(json.dumps(CADASTRO), encoding="utf-8")
    (bronze_dir / "valores_tipo1_rel1.json").write_text(json.dumps(VALORES), encoding="utf-8")

    out_path = connector.normalize("202306")
    assert out_path is not None and out_path.is_file()

    df = pl.read_parquet(out_path)
    assert df.height == 2
    assert set(df.columns) >= {
        "data_base", "cod_inst", "tipo_inst", "relatorio", "grupo", "conta",
        "nome_coluna", "valor", "is_safra",
    }

    safra_row = df.filter(df["cod_inst"] == "58160789").to_dicts()[0]
    assert safra_row["is_safra"] is True
    assert safra_row["nome_instituicao"] == "BCO SAFRA S.A."
    assert safra_row["valor"] == 270050140145.01

    bb_row = df.filter(df["cod_inst"] == "00000000").to_dicts()[0]
    assert bb_row["is_safra"] is False


def test_normalize_sem_bronze_retorna_none(settings_factory):
    settings = settings_factory(ENTIDADES, fontes=FONTES)
    connector = BcbIfDataConnector(settings)
    assert connector.normalize("209912") is None
