from __future__ import annotations

import json

import polars as pl

from safra_dados.connectors.bcb_ifdata import BcbIfDataConnector

ENTIDADES = {
    "grupos": {
        "safra": {
            "cnpj_raiz": "58160789",
            "entidades": {"banco_safra": {"cnpj_raiz": "58160789", "codigo_conglomerado_prudencial": "C0080109"}},
        },
        "pares": {"entidades": {"bb": {"cnpj_raiz": "00000000"}}},
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
        "data_base", "cod_inst", "tipo_inst", "relatorio", "grupo", "grupo_conta", "conta",
        "nome_coluna", "valor", "is_safra",
    }

    safra_row = df.filter(df["cod_inst"] == "58160789").to_dicts()[0]
    assert safra_row["is_safra"] is True
    assert safra_row["grupo"] == "safra"
    assert safra_row["grupo_conta"] == "Ativo"
    assert safra_row["nome_instituicao"] == "BCO SAFRA S.A."
    assert safra_row["valor"] == 270050140145.01

    bb_row = df.filter(df["cod_inst"] == "00000000").to_dicts()[0]
    assert bb_row["is_safra"] is False
    assert bb_row["grupo"] == "pares"


def test_normalize_sem_bronze_retorna_none(settings_factory):
    settings = settings_factory(ENTIDADES, fontes=FONTES)
    connector = BcbIfDataConnector(settings)
    assert connector.normalize("209912") is None


def test_normalize_tipo1_resolve_codigo_de_conglomerado_pelo_lider(settings_factory):
    """No tipo 1, CodInst e o codigo do conglomerado (C...), que nao e linha do cadastro."""
    settings = settings_factory(ENTIDADES, fontes=FONTES)
    bronze_dir = settings.bronze_fonte_dir("bcb_ifdata", "202606")
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "cadastro.json").write_text(json.dumps({"value": [CADASTRO["value"][0]]}), encoding="utf-8")
    valores = {"value": [dict(VALORES["value"][0], CodInst="C0080109", AnoMes="202606")]}
    (bronze_dir / "valores_tipo1_rel1.json").write_text(json.dumps(valores), encoding="utf-8")

    row = pl.read_parquet(BcbIfDataConnector(settings).normalize("202606")).to_dicts()[0]
    assert row["nome_instituicao"] == "BCO SAFRA S.A."
    assert row["grupo"] == "safra"
