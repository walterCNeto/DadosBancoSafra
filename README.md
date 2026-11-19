# safra-dados

Servico de coleta de dados **publicos** (BCB, CVM, SUSEP) sobre o conglomerado
Banco Safra e, por configuracao, outras instituicoes (pares).

Bloco 1 (+ correcoes do 1.1) cobre:

- Esqueleto do pacote `safra_dados` e da CLI `safra-dados`.
- Conector `bcb_ifdata` (Olinda IFDATA v1: `IfDataCadastro` e `IfDataValores`).
- Conector `bcb_cosif_balancetes` (dataset CKAN `ifs-balancetes`).
- Camadas bronze (arquivo bruto + `manifest.jsonl`) e silver (parquet), com
  checkpoint incremental por fonte.

O Bloco 2 (BCB complementar) e o Bloco 3 (CVM) acrescentam os conectores abaixo. O Bloco 4 acrescenta SUSEP (SES) e o RI do Banco Safra. A camada gold (`duckdb`) fica para um bloco seguinte.

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

| `cvm.cadastros` | CKAN CVM: `cia_aberta-cad`, `intermed-cad`, `adm_cart-cad`, `coord_oferta-cad` | instantaneo (data-base = mes da coleta); silver completo |
| `cvm.fundos_cad` | CKAN `fi-cad` (`registro_fundo_classe.zip` pos-RCVM 175 + `cad_fi.csv` legado) | instantaneo; so fundos cujo administrador ou gestor e do mapa (safra/pares) |
| `cvm.fundos_informe` | CKAN `fi-doc-inf_diario` | mensal; so fundos do universo acima; exige `cvm.fundos_cad` antes |
| `cvm.fundos_cda` | CKAN `fi-doc-cda` | mensal; idem; coluna `bloco` (fi_BLC_1..8, fi_PL, fie, ...) |
| `cvm.ofertas` | CKAN `oferta-distrib` | instantaneo; ofertas em que o grupo e emissor ou coordenador lider (por CNPJ) |
| `cvm.dfp`, `cvm.itr` | CKAN `cia_aberta-doc-dfp` / `-itr` | anual (data-base `AAAA`); `documentos`, `demonstracoes` (formato longo) e `composicao_capital` |
| `cvm.fre`, `cvm.ipe` | CKAN `cia_aberta-doc-fre` / `-ipe` | anual; so `documentos` (metadados/links), sem tabelas de detalhe do FRE |

| `susep.ses` | pagina do SES (`www2.susep.gov.br/menuestatistica/ses/principal.aspx`) -> `BaseCompleta.zip` + `LISTAEMPRESAS.csv` | data-base = mes de carga (`AAAAMM`) lido da pagina; zip de ~580 MB; tabelas desde 2020 |
| `safra.ri` | `www.safra.com.br/sobre/relacoes-com-investidores...` (HTML) | catalogo de documentos de RI; ver bloco 4 (robots.txt) |

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

# Bloco 3 (CVM): a ordem de `all` ja respeita a dependencia fundos_cad -> informe/cda
safra-dados ingest --fonte cvm.cadastros
safra-dados ingest --fonte cvm.fundos_cad
safra-dados ingest --fonte cvm.fundos_informe --desde 2024-01
safra-dados ingest --fonte cvm.fundos_cda --desde 2024-01

# Bloco 4: SUSEP (SES) e RI Safra
safra-dados resolver-entidades          # agora tambem grava `susep.sugestoes` (FIP + CNPJ, aprovado: false)
safra-dados ingest --fonte susep.ses --desde 2020-01
safra-dados ingest --fonte safra.ri --desde 2023-01

# Bloco 4.1: refaz a silver a partir do bronze, sem rede, com as entidades aprovadas atuais
safra-dados reprocessar --fonte susep.ses        # ou cvm.fundos_cad, cvm.fundos_informe, cvm.fundos_cda, ... | all
# Bloco 4.1: arquivos de RI baixados pelo navegador (fonte bloqueada por robots) -> data\manual\safra_ri\
safra-dados importar-manual --fonte safra_ri
safra-dados importar-manual --fonte safra_ri --tipo demonstracoes_ifrs --data-base 2026-09   # arquivo sem correspondencia

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

## Bloco 3 - CVM (dados.cvm.gov.br)

- **Descoberta por CKAN**: todo recurso vem de `package_show` (`ckan_base_url` em
  `settings.yaml`), sem URL fixa de arquivo. Unica deducao: o CKAN so lista os ultimos ~12
  meses de informe diario/CDA; os meses anteriores (ate 2024-01 e antes) saem do template do
  nome de um recurso listado (`...AAAAMM.zip`) e sao confirmados por HEAD.
- **Silver por tabela**: `silver\<fonte>\<tabela>\<data_base>.parquet` (informe e CDA:
  `silver\<fonte>\<AAAAMM>.parquet`). Colunas em minusculas, CNPJ so com digitos, datas `Date`,
  numericos `Float64` onde o layout e conhecido (cast que perderia valor e abandonado).
  `grupo`/`is_safra` por CNPJ, com a regra de sempre (nome nunca marca) e **CNPJ placeholder**
  (filial `0000`, ex. `00.000.000/0000-00` de estrangeiras no IPE) ignorado, para nao virar BB.
- **Resolvedor + CVM**: `safra-dados resolver-entidades` tambem consulta os cadastros da CVM e grava
  em `entidades_resolvidas.yaml` a secao `cvm.sugestoes.<grupo>.<entidade>`: registros por CNPJ raiz
  (conglomerado ja resolvido) ou por nome (`busca_nome_cvm` em `entidades.yaml`). Cada item traz
  `ativo`, `no_mapa` (ja marcado via IF.data/config) e `aprovado: false`. **Revise e ponha
  `aprovado: true`** nos que devem entrar no mapa (ex.: Safra Gestao de Recursos, `11120589`); so
  esses passam a marcar linhas e a puxar fundos. `--sem-cvm` pula a consulta e preserva a secao.
- **Fundos**: o cadastro e filtrado por administrador **ou** gestor no mapa; classes e subclasses
  seguem o fundo (`grupo` = `safra` se qualquer dos dois e Safra). Informe e CDA so guardam esses
  fundos (universo lido do silver de `cvm_fundos_cad`).
- `ingest` agora continua para as demais fontes se uma falhar na descoberta e termina com codigo 1.

### Limitacoes (Bloco 3)

- O pacote CKAN `emissores` so tem planilhas `.ods`; o cadastro de emissores aqui e o de companhias
  abertas. Nao ha entidade Safra ativa como companhia aberta: Banco Safra, Banco Alfa de Investimento,
  Safra Leasing etc. constam como `CANCELADA`, entao DFP/ITR/FRE/IPE ficam so com os **pares**.
- Ofertas: coordenadores que nao sejam o lider (`Grupo_Coordenador`) e emissores sem CNPJ (ofertas antigas,
  so nome) nao marcam nada. Fundos do grupo como emissores (FIDC etc.) nao entram.
- DFP/ITR: `parecer` do auditor nao e carregado; FRE so tem o indice de documentos.
- O arquivo e baixado inteiro a cada execucao para calcular o sha256 (padrao dos conectores).

## Bloco 4 - SUSEP (SES) e RI do Banco Safra

### `susep.ses`

- **Descoberta**: so a pagina do SES esta em `settings.yaml` (`pagina_url`). Dela saem os links de
  `BaseCompleta.zip` (via `redarq.asp`, que redireciona) e `LISTAEMPRESAS.csv`, e o mes de carga
  (`lblMesAnoCarga`), que vira a data-base. Se a pagina mudar, a falha e registrada, nunca adivinhada.
  `robots.txt` do host e consultado (o da SUSEP nao existe: tudo liberado).
- **Arquivos**: CSVs `;` em cp1252, decimal com virgula, FIP (`coenti`) as vezes com espacos, nomes de
  membro com caixa variada. O zip tem ~2,9 GB descompactado; as tabelas grandes sao extraidas uma a uma
  para um diretorio temporario em `data\` e lidas em streaming (`scan_csv`) ja filtradas.
- **Silver** (`silver\susep_ses\<tabela>\<AAAAMM>.parquet`):
  - universo completo: `cadastro` (Ses_cias + LISTAEMPRESAS + ultimo grupo economico SUSEP), `ramos`,
    `grupos_ramos`, `campos` (dicionario de itens do balanco) e `grupos_economicos` (desde `desde`);
  - so safra/pares e periodo >= `desde`: `seguros` (premios/sinistros por empresa e ramo), `provramos`
    (provisoes tecnicas por ramo), `prov`, `balanco` (com `noitem` vindo de `campos`: `cmpid` = `nuitem`),
    `pl_margem` (PL, margem de solvencia), `valores_mov_ramos` e `uf`.
  - Nao carregadas (existem no zip): `Ses_rmovram` (layout largo e instavel), `ses_contatos`,
    `Ses_Administradores`, previdencia/capitalizacao detalhadas (`ses_pgbl_*`, `ses_prev_*`, `ses_cap_*`),
    `Ses_limite_ret`, `Ses_part_acion`. Adicionar uma tabela = uma linha em `TABELAS_GRANDES`.
- **Resolucao de entidades**: `resolver-entidades` cruza `LISTAEMPRESAS.csv` (FIP, nome, CNPJ) com
  `busca_nome_susep` (e CNPJ raiz conhecido) de `entidades.yaml` e grava `susep.sugestoes.<grupo>.<entidade>`
  em `entidades_resolvidas.yaml` com `aprovado: false`. **Revise e ponha `aprovado: true`**: so esses
  marcam `grupo`/`is_safra` (por CNPJ no mapa e por FIP nas tabelas do SES). O `LISTAEMPRESAS` so lista
  empresas ativas; entidades extintas aparecem so no `cadastro` do zip (sem CNPJ), marcaveis por FIP
  se alguem as adicionar a mao em `susep.sugestoes`.
- **Universo das tabelas grandes**: FIPs com `grupo` (aprovados) **mais os candidatos** (sugestoes ainda
  `aprovado: false`). Candidatos ficam com `grupo` nulo e `grupo_sugerido` preenchido; assim, aprovar
  uma sugestao so exige reprocessar a silver (`safra-dados reprocessar --fonte susep.ses`, sem rede),
  e `grupo` passa a valer. Filtre por `grupo` (nao por `grupo_sugerido`) em analises.
- Cuidado com sugestoes por nome: ex. `ITAU SEGUROS DE AUTO E RESIDENCIA` (FIP 03182) hoje pertence ao
  grupo Porto Seguro segundo o proprio SES (`cogrupo` 00051); revise antes de aprovar.
- **Incremental (bloco 4.1)**: a pagina e lida primeiro (mes de carga = data-base); se o manifest ja tem bronze `ok` para esse mes e o zip esta em disco, o zip **nao e baixado de novo** (registra `ja_existe`). O zip fica no bronze; para refazer so a silver use `safra-dados reprocessar --fonte susep.ses`.

### `safra.ri` (RI do Banco Safra)

- **Descoberta**: so HTML (nao precisa de JavaScript). Parte de `pagina_inicial`, segue links internos sob
  `prefixo_paginas` (`.htm`) e le os blocos `div.doc AAAA[data-month=MM]` com `.titulo` + `span.badge`
  + link. A secao (acordeao) e o titulo classificam o `tipo`: `demonstracoes_ifrs`, `demonstracoes_brgaap`,
  `demonstracoes_brgaap_resumo` (o "Resumo Consolidado e Principais Indicadores"/balanco promocional, unico
  BRGAAP publicado desde 2023 na pagina), `pilar3_gir` (relatorio + anexo xlsx), `relatorio_anual` e `outros`
  (DCA da LIG, CVM008, etc., so catalogados). A data-base (`AAAAMM`) vem do titulo ("1T23", "set 2026",
  "Relatorio Anual 2025"), depois do nome do arquivo, depois do mes de publicacao (`data_base_origem`).
- **robots.txt e limite de taxa**: o robots.txt e avaliado por regra de maior correspondencia (RFC 9309;
  `safra_dados.robots`, nao o `urllib.robotparser`, que liberaria tudo por causa do `Allow: /` inicial).
  O site tem `Disallow: /data/` e **todos os arquivos de RI estao em `/data/files/...`**; por isso o conector
  **nao baixa nenhum arquivo**: cataloga titulo, tipo, data-base, URL e formato com `status = bloqueado_robots`,
  `baixado = false`, sem sha256. As paginas de listagem (permitidas) vao para a bronze, com hash. Se o robots.txt
  mudar para permitir, o mesmo codigo baixa (`doc__<tipo>__<nome>`), grava sha256 e passa a ser incremental. Se o
  robots.txt der erro 5xx/rede, nada e coletado. Intervalo minimo entre requisicoes: `intervalo_min_segundos` (2 s).
- Silver `silver\safra_ri\<AAAAMM>.parquet`: um registro por documento (`tipo`, `titulo`, `url`, `formato`,
  `status`, `baixado`, `sha256`, `bytes`, `arquivo_local`, ...), `grupo = safra` por `cnpj_emissor` (58160789).
  `bronze\safra_ri\<AAAAMM>\catalogo.jsonl` guarda os metadados de todos os documentos vistos.
- Sem parsing de PDF/xlsx.

## Bloco 4.1 - aprovacoes SUSEP, SES incremental, reprocessar e importacao manual

- **Aprovacoes SUSEP** (`config\entidades_resolvidas.yaml`, `aprovado: true`): grupo `safra` = FIP 01627 (Safra Seguros Gerais) e
  09938 (Safra Vida e Previdencia); grupo `pares` = Itau 05321, 05096, 06912; Bradesco 05444, 06866, 05312, 23566; Santander 06564,
  05070, 01589, 29319; BTG 02305, 02682; BB 06785, 04707, 20141, 06211. **Nao aprovados** (continuam candidatos, `grupo` nulo):
  03182 (grupo Porto Seguro segundo o SES), 74934 (corretora de resseguros) e 37052 (resseguradora). Depois de mudar aprovacoes,
  rode `safra-dados reprocessar` nas fontes afetadas.
- **`safra-dados reprocessar --fonte <fonte>|all`**: chama o `normalize` de cada data-base que tem bronze, **sem nenhuma requisicao
  de rede**, aplicando as entidades aprovadas atuais. Uma data-base com erro nao impede as demais (codigo de saida 1 no fim).
  Para `all`, a ordem e a de `ingest` (`cvm.fundos_cad` antes de informe/CDA, que filtram pelo universo de fundos).
- **`safra-dados importar-manual --fonte safra_ri`**: o site do Safra proibe `/data/` no robots.txt, onde ficam todos os PDFs/xlsx
  de RI (o conector so cataloga; ver Bloco 4). Baixe os arquivos **pelo navegador** e coloque em `data\manual\safra_ri\`. O comando:
  1. calcula o sha256 de cada arquivo (`.pdf .xlsx .xls .xlsm .csv .zip .docx`);
  2. casa o arquivo com um item do catalogo (`bronze\safra_ri\*\catalogo.jsonl`) pelo **nome do arquivo** (sem extensao, acentos,
     pontuacao nem sufixo `(1)` de download repetido), depois pelo **titulo**, depois por similaridade de nome >= 0,92;
     empate entre itens = nao casa;
  3. se nao casar, o arquivo fica `PENDENTE` (codigo de saida 1) e pede `--tipo` (`demonstracoes_ifrs`, `demonstracoes_brgaap`,
     `demonstracoes_brgaap_resumo`, `pilar3_gir`, `relatorio_anual`, `outros`) e `--data-base AAAA-MM`; com as duas opcoes os arquivos sem
     correspondencia viram itens novos (`url = manual://safra_ri/<AAAAMM>/<sha16>`);
  4. copia para `bronze\safra_ri\<AAAAMM>\doc__<tipo>__<nome>`, grava no `manifest.jsonl` uma entrada com `status = importado_manual` e
     sha256, atualiza o catalogo e regrava a silver (`baixado = true`, `status = importado_manual`).
  E idempotente (mesmo sha256 = `ja_importado`) e **nunca acessa a rede nem `/data/` do site**; um `ingest --fonte safra.ri`
  posterior nao sobrescreve um item importado manualmente.
