# safra-dados

Servico de coleta de dados **publicos** (BCB, CVM, SUSEP) sobre o conglomerado
Banco Safra e, por configuracao, outras instituicoes (pares).

Bloco 1 (+ correcoes do 1.1) cobre:

- Esqueleto do pacote `safra_dados` e da CLI `safra-dados`.
- Conector `bcb_ifdata` (Olinda IFDATA v1: `IfDataCadastro` e `IfDataValores`).
- Conector `bcb_cosif_balancetes` (dataset CKAN `ifs-balancetes`).
- Camadas bronze (arquivo bruto + `manifest.jsonl`) e silver (parquet), com
  checkpoint incremental por fonte.

O Bloco 2 (BCB complementar) acrescenta os conectores abaixo. CVM, SUSEP e a camada gold (`duckdb`) ficam para um bloco seguinte.

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

- `config\entidades.yaml`: grupos `safra` (Banco Safra `58160789`, Banco J. Safra
  `03017677`, conglomerado prudencial `C0080109`, mais seguradora/previdencia/
  asset/corretora/Alfa a resolver) e `pares` (Itau Unibanco, Bradesco, Santander
  Brasil, BTG Pactual, Banco do Brasil).
- `config\entidades_resolvidas.yaml` (gerado por `resolver-entidades`; revise
  antes de confiar nele).

### Marcacao `grupo` / `is_safra` (somente por identificador)

As tabelas silver ganham `grupo` (`safra`, `pares` ou nulo) e `is_safra`. A
marcacao usa **apenas identificadores** (CNPJ raiz, CNPJ completo, codigo de
conglomerado), nunca o nome. A busca por nome existe so no `resolver-entidades`:

| status no resolvido | significado | marca linhas? |
|---|---|---|
| `confirmado` | identificador do YAML confirmado no IfDataCadastro; CNPJs do conglomerado coletados | sim |
| `resolvido` | (so pares, `resolver_por_nome_lider: true`) nome casou com **uma unica** lider ativa | sim |
| `sugestao` | entidade sem identificador; candidatos por nome para revisao humana | **nao**, ate copiar o CNPJ para `entidades.yaml` ou pôr `aprovado: true` |
| `ambiguo` / `nao_encontrado` / `falha_api` | — | nao |

No IFDATA a coluna de conta contabil chama-se `grupo_conta` (para nao colidir com `grupo`).

Os dados ficam fora do controle de versao (`data\` esta no `.gitignore`):

```
data\
  bronze\<fonte>\<data_base>\arquivo... + manifest.jsonl
  silver\<fonte>\<data_base>.parquet
  gold\safra.duckdb        (bloco futuro)
```

## Fontes

| `--fonte` | Origem | Observacao |
|---|---|---|
| `bcb.ifdata` | Olinda IFDATA | trimestral |
| `bcb.cosif` | CKAN `ifs-balancetes` | trimestral |
| `bcb.juros` | Olinda `taxaJuros` v2 | ranking por instituicao/modalidade; mensal + ultima janela diaria de cada mes |
| `bcb.tarifas` | Olinda `Informes_ListaTarifasPorInstituicaoFinanceira` | **instantaneo** (so tarifas vigentes); data-base = mes da coleta; apenas instituicoes de safra/pares |
| `bcb.unicad` | Olinda `BcBase` v2 (`EntidadesSupervisionadas`) | `dataBase` em `MM-DD-AAAA` |
| `bcb.reclamacoes` | REST `rdrweb/rest/ext/ranking` (**nao e Olinda**) | CSV por periodo; conglomerados vem sem CNPJ (ver limitacoes) |
| `bcb.estban` | arquivos em `bcb.gov.br/content/estatisticas/estatistica_bancaria_estban` (**nem CKAN nem Olinda**) | formato longo (verbete/saldo), niveis municipio e agencia |
| `bcb.sgs` | `api.bcb.gov.br` (**nao e Olinda**) | Selic meta 432, IPCA 433, dolar 1, CDI 12 |

Todas as chamadas Olinda usam `olinda_url`. As tabelas silver de fontes por instituicao
trazem `grupo`/`is_safra`.

## Uso da CLI

```powershell
# ingestao completa de uma fonte, desde uma data-base
safra-dados ingest --fonte bcb.ifdata --desde 2015-03
safra-dados ingest --fonte bcb.cosif --desde 2015-03
safra-dados ingest --fonte all --desde 2015-03
# ultimo periodo de uma fonte do Bloco 2 (cada fonte tem seu proprio calendario)
safra-dados ingest --fonte bcb.juros --desde 2026-10
safra-dados ingest --fonte bcb.sgs --desde 2026-10

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

## Notas e limitacoes conhecidas

- **Olinda e as chaves da query**: o Olinda responde HTTP 500 se `@` e `$` nas
  chaves da query vierem codificados (`%40`, `%24`), que e o que o httpx faz com
  `params=`. Toda URL Olinda e montada por `safra_dados.olinda.olinda_url` (chaves
  literais, valores com `quote(v, safe="'")`) e passada inteira ao httpx. Nunca use
  `params=` com Olinda. Se a resposta vier truncada (`@odata.nextLink`), o conector
  pagina com `$top/$skip`.
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
- **Bloco 2 - limitacoes**: (a) o ranking de reclamacoes traz os conglomerados (ex.:
  "SAFRA (conglomerado)") sem CNPJ, entao ficam com `grupo` nulo; so as linhas
  "Banco/financeira" tem CNPJ. (b) Tarifas nao tem historico no servico. (c) `taxaJuros`
  so publica as instituicoes que entram no ranking de cada modalidade. (d) O Estban de
  um mes so aparece apos a publicacao; meses ausentes sao simplesmente ignorados no
  `discover`. (e) SGS 404 "Value(s) not found" (mes corrente sem valor, ex.: IPCA) conta
  como serie vazia, nao como falha.
