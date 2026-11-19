# safra-dados

Servico de coleta de dados **publicos** (BCB, CVM, SUSEP) sobre o conglomerado
Banco Safra e, por configuracao, outras instituicoes (pares).

Bloco 1 (este repositorio, no estado atual) cobre:

- Esqueleto do pacote `safra_dados` e da CLI `safra-dados`.
- Conector `bcb_ifdata` (Olinda IFDATA v1: `IfDataCadastro` e `IfDataValores`).
- Conector `bcb_cosif_balancetes` (dataset CKAN `ifs-balancetes`).
- Camadas bronze (arquivo bruto + `manifest.jsonl`) e silver (parquet), com
  checkpoint incremental por fonte.

CVM, SUSEP e a camada gold (`duckdb`) ficam para um bloco seguinte.

## Requisitos

- Windows 11 com PowerShell.
- Ambiente conda `dadossafra` (Python 3.12) ja criado e ativo.

## Instalacao (PowerShell)

```powershell
conda activate dadossafra
cd C:\Users\User\Documents\Walter\DadosBancoSafra
pip install -e ".[dev]"
```

## Configuracao

- `config\settings.yaml`: URLs base, parametros de HTTP (timeout, retries,
  limite de taxa) e a raiz dos dados (`data_root`, padrao `.\data`).
- A raiz dos dados pode ser sobrescrita pela variavel de ambiente
  `SAFRA_DADOS_HOME`:

  ```powershell
  $env:SAFRA_DADOS_HOME = "D:\dados-safra"
  ```

- `config\entidades.yaml`: grupo `safra` (CNPJ raiz `58160789`) com os campos
  de seguradora, previdencia, asset, corretora e Banco Alfa a resolver.
- `config\entidades_resolvidas.yaml` (gerado por `resolver-entidades`, nao
  versionado como fonte de verdade — revise manualmente antes de confiar nele).

Os dados ficam fora do controle de versao (`data\` esta no `.gitignore`):

```
data\
  bronze\<fonte>\<data_base>\arquivo... + manifest.jsonl
  silver\<fonte>\<data_base>.parquet
  gold\safra.duckdb        (bloco futuro)
```

## Uso da CLI

```powershell
# ingestao completa de uma fonte, desde uma data-base
safra-dados ingest --fonte bcb.ifdata --desde 2015-03
safra-dados ingest --fonte bcb.cosif --desde 2015-03
safra-dados ingest --fonte all --desde 2015-03

# resolve os CNPJs/codigos de conglomerado do grupo Safra via IfDataCadastro
safra-dados resolver-entidades

# status por fonte: ultima data-base, numero de arquivos, falhas
safra-dados status
```

A ingestao e **idempotente**: se o sha256 de um recurso ja coletado com
sucesso bater, o arquivo nao e baixado de novo (ver `manifest.jsonl` e
`checkpoint.json` em cada `data\bronze\<fonte>\`).

## Testes

```powershell
pytest -m "not rede"
```

Os testes marcados `@pytest.mark.rede` fazem chamadas reais aos servicos do
BCB e sao pulados por padrao; para roda-los: `pytest -m rede`.

## Notas e limitacoes conhecidas (bloco 1)

- Durante o desenvolvimento, os endpoints parametrizados do Olinda IFDATA
  (`IfDataCadastro` e `IfDataValores`) estavam retornando HTTP 500 ("Erro
  desconhecido") para qualquer parametro testado, enquanto `ListaDeRelatorio()`
  funcionava normalmente — um indicio de instabilidade do lado do BCB, nao de
  uso incorreto da API (a assinatura foi conferida contra o `$metadata` OData
  e o `swagger-ui3`). O conector foi implementado conforme a especificacao
  oficial; se o BCB estiver fora do ar, os recursos ficam marcados como
  `falha` no manifest para nova tentativa.
- Os balancetes Cosif consolidados por segmento (Bancos, Conglomerados
  prudenciais) so existem, no formato atual, a partir da data-base 2023-03.
  Para datas anteriores, o conector usa o endpoint individualizado por
  CNPJ/codigo de conglomerado (documentos 4010/4040), limitado aos codigos
  configurados em `entidades.yaml`/`entidades_resolvidas.yaml` — sem o
  `IfDataCadastro` funcionando, nao ha como enumerar todo o universo de
  instituicoes para esses periodos.
- O documento do conglomerado prudencial nos arquivos atuais e `4060` (nao
  `4040`); o conector nao assume um numero fixo, apenas registra o que vem na
  coluna `DOCUMENTO` de cada arquivo.
