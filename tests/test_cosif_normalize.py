from __future__ import annotations

from safra_dados.connectors.base import decode_bytes
from safra_dados.connectors.bcb_cosif_balancetes import parse_cosif_csv

SAMPLE_CSV = (
    "Balancete/Balanco Geral (Documentos: 4010)\n"
    "Data de geracao dos dados: 2026-11-16\n"
    "Fonte: Instituicoes financeiras\n"
    "#DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO\n"
    "202606;4010;58160789;;BCO SAFRA S.A.;;;BANCOS MULTIPLOS;1000000009;Ativo Realizável;245370733134,58\n"
    "202606;4010;00000000;;BCO DO BRASIL S.A.;;;BANCO DO BRASIL - BANCO MULTIPLO;1000000009;Ativo Realizável;2380735804252,21\n"
)


def test_parse_cosif_csv_colunas_e_valores():
    df = parse_cosif_csv(SAMPLE_CSV, segmento="Bancos")
    assert df.height == 2
    assert set(df.columns) >= {
        "data_base", "cnpj", "nome", "atributo", "documento", "conta", "nome_conta", "saldo", "segmento",
    }

    safra = df.filter(df["cnpj"] == "58160789").to_dicts()[0]
    assert safra["nome"] == "BCO SAFRA S.A."
    assert safra["documento"] == "4010"
    assert safra["data_base"] == "202606"
    assert safra["saldo"] == 245370733134.58
    assert safra["segmento"] == "Bancos"


def test_parse_cosif_csv_ignora_linhas_de_metadado():
    df = parse_cosif_csv(SAMPLE_CSV)
    # as 3 primeiras linhas (titulo, data de geracao, fonte) e o cabecalho "#..."
    # nao devem virar registros
    assert df.height == 2


def test_parse_cosif_csv_vazio_quando_sem_dados():
    texto = "titulo\ndata\nfonte\n#DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO\n"
    df = parse_cosif_csv(texto)
    assert df.is_empty()


def test_decode_bytes_cp1252():
    raw = "Ativo Realizável".encode("cp1252")
    assert decode_bytes(raw) == "Ativo Realizável"


def test_decode_bytes_latin1_fallback():
    # byte 0x81 e invalido em cp1252 mas valido em latin-1 (controle C1)
    raw = b"teste\x81fim"
    texto = decode_bytes(raw)
    assert "teste" in texto and "fim" in texto
