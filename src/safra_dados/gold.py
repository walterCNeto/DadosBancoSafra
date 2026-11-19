"""Camada gold: `data/gold/safra.duckdb` com VIEWS sobre o silver (parquet via `read_parquet`,
sem copiar dados). Todas as views gold trazem `data_base` (DATE), `grupo` e `entidade`; valores em
reais; indices derivados arredondados a 3 casas decimais.

Estrutura do banco:
- schema `silver`: uma view por fonte (`read_parquet(<glob>, union_by_name=true)`);
- tabelas de configuracao pequenas em `gold`: `dim_entidade` (identificador -> grupo/entidade, montada
  a partir de config/entidades*.yaml), `dim_reclamacao_nome` e `referencias_fundos` (config/referencias.yaml);
- views `gold.*`: capital, balanco_dre, carteira, juros, reclamacoes, agencias, fundos_papel,
  fundos_dias, fundos, fundos_conciliacao, seguros.

Uma view gold so e criada se todas as views silver de que depende existem (fonte ainda nao ingerida
=> view ignorada e registrada no resultado).
"""

from __future__ import annotations

import glob
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import yaml

from .config import Settings, load_entidades, load_entidades_resolvidas
from .entidades import _utilizavel, normalizar_identificador

# nome da view silver -> glob (relativo a data/silver)
SILVER_VIEWS: dict[str, str] = {
    "ifdata": "bcb_ifdata/*.parquet",
    "cosif": "bcb_cosif/*.parquet",
    "juros": "bcb_taxas_juros/*.parquet",
    "reclamacoes": "bcb_reclamacoes/*.parquet",
    "estban": "bcb_estban/*.parquet",
    "informe": "cvm_fundos_informe/*.parquet",
    "cda": "cvm_fundos_cda/*.parquet",
    "fundos_classe": "cvm_fundos_cad/registro_classe/*.parquet",
    "fundos_legado": "cvm_fundos_cad/cad_fi_legado/*.parquet",
    "fundos_fundo": "cvm_fundos_cad/registro_fundo/*.parquet",
    "ses_seguros": "susep_ses/seguros/*.parquet",
    "ses_pl_margem": "susep_ses/pl_margem/*.parquet",
    "ses_prov": "susep_ses/prov/*.parquet",
    "ses_cadastro": "susep_ses/cadastro/*.parquet",
}

_DIM_IFDATA = (
    "d.identificador = i.cod_inst and d.tipo_id in ('conglomerado', 'cnpj_raiz') "
    "and d.origem in ('config', 'resolvido')"
)
_MES_FIM = "last_day(strptime({c} || '01', '%Y%m%d')::date)"


def _mes(coluna: str) -> str:
    return _MES_FIM.format(c=coluna)


# ---------------------------------------------------------------- dimensao de entidades

def construir_dim_entidade(settings: Settings) -> list[tuple[str, str, str, str, str]]:
    """Linhas (tipo_id, identificador, grupo, entidade, origem). `tipo_id`: cnpj_raiz | conglomerado | fip.
    Mesma fonte de verdade do mapa de marcacao (entidades.yaml + resolvidas), mas guardando a chave
    da entidade. Em conflito prevalece o primeiro (config > resolvido > cvm > susep), exceto que
    `safra` vence `pares`."""
    cfg = load_entidades(settings)
    res = load_entidades_resolvidas(settings) or {}
    linhas: dict[tuple[str, str], tuple[str, str, str]] = {}

    def add(grupo: str, entidade: str, origem: str, valores: list[Any], tipo: str | None = None) -> None:
        for valor in valores:
            if tipo == "fip":
                ident = str(valor).strip().zfill(5) if valor not in (None, "") else None
            else:
                ident = normalizar_identificador(valor)
            if not ident:
                continue
            t = tipo or ("cnpj_raiz" if ident.isdigit() else "conglomerado")
            atual = linhas.get((t, ident))
            if atual is None or (grupo == "safra" and atual[0] != "safra"):
                linhas[(t, ident)] = (grupo, entidade, origem)

    campos_cfg = ("cnpj_raiz", "cnpj", "codigo_conglomerado_prudencial", "codigo_conglomerado_financeiro")
    for grupo, gcfg in (cfg.get("grupos") or {}).items():
        for nome, ent in (gcfg.get("entidades") or {}).items():
            add(grupo, nome, "config", [ent.get(c) for c in campos_cfg])
    for grupo, gres in (res.get("grupos") or {}).items():
        for nome, ent in (gres.get("entidades") or {}).items():
            if _utilizavel(ent):
                add(grupo, nome, "resolvido", [
                    ent.get("cnpj"), ent.get("codigo_conglomerado_prudencial"),
                    ent.get("codigo_conglomerado_financeiro"), *(ent.get("cnpjs_relacionados") or []),
                ])
    for secao, origem in (("cvm", "cvm"), ("susep", "susep")):
        for grupo, por_ent in ((res.get(secao) or {}).get("sugestoes") or {}).items():
            for nome, itens in (por_ent or {}).items():
                for item in itens or []:
                    if not item.get("aprovado"):
                        continue
                    add(grupo, nome, origem, [item.get("cnpj_raiz"), item.get("cnpj")])
                    if secao == "susep":
                        add(grupo, nome, origem, [item.get("codigo_fip")], tipo="fip")
    return [(t, i, g, e, o) for (t, i), (g, e, o) in sorted(linhas.items())]


def carregar_referencias(settings: Settings) -> dict[str, Any]:
    caminho = settings.referencias_path()
    if not caminho.is_file():
        return {}
    return yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}


def referencias_fundos(refs: dict[str, Any]) -> list[tuple]:
    """Linhas de `gold.referencias_fundos` a partir de config/referencias.yaml: `referencias` (id -> valor em MIL de
    reais, data) e `conciliacoes_fundos` (comparacoes: soma de referencias x PL do gold). Valores saem em reais."""
    base = refs.get("referencias") or {}
    linhas = []
    for c in refs.get("conciliacoes_fundos") or []:
        if c.get("visao") not in ("administrado", "gerido", "ambos"):
            raise ValueError(f"referencias.yaml: visao invalida em {c.get('id')!r}: {c.get('visao')!r}")
        comps = c.get("referencia") or []
        faltam = [x for x in comps if x not in base]
        if not comps or faltam:
            raise ValueError(f"referencias.yaml: conciliacao {c.get('id')!r} usa referencias inexistentes: {faltam or comps}")
        datas = {str(base[x]["data_referencia"]) for x in comps}
        if len(datas) != 1:
            raise ValueError(f"referencias.yaml: conciliacao {c.get('id')!r} mistura datas de referencia: {sorted(datas)}")
        total = sum(float(base[x]["valor_mil_reais"]) for x in comps) * 1000.0
        linhas.append((
            c["id"], c.get("descricao"), datas.pop(), " + ".join(comps), total, c["visao"],
            bool(c.get("inclui_previdencia_ses", False)), "; ".join(str(base[x].get("fonte", "")) for x in comps),
        ))
    return linhas


# ---------------------------------------------------------------- SQL das views gold
# (nome, silver necessario, gold necessario, SQL)

_CAPITAL = f"""
with ident as (
  select i.*, d.grupo as g, d.entidade as e
  from silver.ifdata i join gold.dim_entidade d on {_DIM_IFDATA}
  where i.relatorio = '5'
),
base as (
  select {_mes('data_base')} as data_base, g as grupo, e as entidade, cod_inst,
    any_value(nome_instituicao) as instituicao,
    max(valor) filter (where relatorio = '5' and conta = '79649') as patrimonio_referencia,
    max(valor) filter (where relatorio = '5' and conta = '79647') as capital_nivel_1,
    max(valor) filter (where relatorio = '5' and conta = '79645') as capital_principal,
    max(valor) filter (where relatorio = '5' and conta = '79646') as capital_complementar,
    max(valor) filter (where relatorio = '5' and conta = '79648') as capital_nivel_2,
    max(valor) filter (where relatorio = '5' and conta = '79665') as rwa_total,
    max(valor) filter (where relatorio = '5' and conta = '138232') as rwa_credito,
    max(valor) filter (where relatorio = '5' and conta = '79651') as rwa_mercado,
    max(valor) filter (where relatorio = '5' and conta = '79656') as rwa_operacional,
    max(valor) filter (where relatorio = '5' and conta = '134461') as rwa_pagamentos,
    max(valor) filter (where relatorio = '5' and conta = '79658') as exposicao_total,
    max(valor) filter (where relatorio = '5' and conta = '92999') as adicional_capital_principal,
    max(valor) filter (where relatorio = '5' and conta = '79664') as indice_basileia,
    max(valor) filter (where relatorio = '5' and conta = '79660') as indice_nivel_1,
    max(valor) filter (where relatorio = '5' and conta = '79659') as indice_capital_principal,
    max(valor) filter (where relatorio = '5' and conta = '79661') as razao_alavancagem
  from ident group by all
),
cmp as (
  select *,
    median(indice_basileia) filter (where grupo = 'pares') over (partition by data_base) as m_bas,
    median(indice_nivel_1) filter (where grupo = 'pares') over (partition by data_base) as m_n1,
    median(indice_capital_principal) filter (where grupo = 'pares') over (partition by data_base) as m_cp,
    median(razao_alavancagem) filter (where grupo = 'pares') over (partition by data_base) as m_alav
  from base
)
select data_base, grupo, entidade, cod_inst, instituicao,
  patrimonio_referencia, capital_nivel_1, capital_principal, capital_complementar, capital_nivel_2,
  rwa_total, rwa_credito, rwa_mercado, rwa_operacional, rwa_pagamentos, exposicao_total, adicional_capital_principal,
  round(100 * indice_basileia, 3) as indice_basileia, round(100 * indice_nivel_1, 3) as indice_nivel_1,
  round(100 * indice_capital_principal, 3) as indice_capital_principal, round(100 * razao_alavancagem, 3) as razao_alavancagem,
  round(100 * m_bas, 3) as indice_basileia_mediana_pares, round(100 * (indice_basileia - m_bas), 3) as indice_basileia_dif_pares,
  round(100 * m_n1, 3) as indice_nivel_1_mediana_pares, round(100 * (indice_nivel_1 - m_n1), 3) as indice_nivel_1_dif_pares,
  round(100 * m_cp, 3) as indice_capital_principal_mediana_pares,
  round(100 * (indice_capital_principal - m_cp), 3) as indice_capital_principal_dif_pares,
  round(100 * m_alav, 3) as razao_alavancagem_mediana_pares, round(100 * (razao_alavancagem - m_alav), 3) as razao_alavancagem_dif_pares
from cmp
"""

_BALANCO_DRE = f"""
with r as (
  select {_mes('i.data_base')} as data_base, d.grupo, d.entidade, i.cod_inst,
    any_value(i.nome_instituicao) as instituicao,
    max(i.valor) filter (where i.conta = '140220') as ativo_total,
    max(i.valor) filter (where i.conta = '141873') as carteira_credito,
    max(i.valor) filter (where i.conta = '140239') as captacoes,
    max(i.valor) filter (where i.conta = '140246') as patrimonio_liquido,
    max(i.valor) filter (where i.conta = '141870') as lucro_liquido_acum
  from silver.ifdata i join gold.dim_entidade d on {_DIM_IFDATA}
  where i.relatorio = '1' group by all
),
cc as (
  select {_mes('s.data_base')} as data_base, d.entidade, s.conta, max(s.saldo) as saldo
  from silver.cosif s join gold.dim_entidade d on d.tipo_id = 'conglomerado' and d.identificador = s.cod_conglomerado
  where s.documento = '4060' and s.cod_conglomerado is not null
    and s.conta in ('1000000009', '2000000008', '6000000004', '7000000003', '8000000002')
  group by all
),
c as (
  select data_base, entidade,
    sum(saldo) filter (where conta in ('1000000009', '2000000008')) as ativo_total_cosif,
    sum(saldo) filter (where conta in ('6000000004', '7000000003', '8000000002')) as patrimonio_liquido_cosif,
    sum(saldo) filter (where conta in ('7000000003', '8000000002')) as lucro_liquido_acum_cosif
  from cc group by all
),
j as (
  select r.*, c.ativo_total_cosif, c.patrimonio_liquido_cosif, c.lucro_liquido_acum_cosif
  from r left join c using (data_base, entidade)
),
w as (
  select j.*,
    case when month(j.data_base) = 3 then j.lucro_liquido_acum
         when t.lucro_liquido_acum is not null then j.lucro_liquido_acum - t.lucro_liquido_acum end as lucro_liquido_trimestre,
    case when a.patrimonio_liquido is not null then (j.patrimonio_liquido + a.patrimonio_liquido) / 2
         else j.patrimonio_liquido end as pl_base_roe,
    case when a.ativo_total is not null then (j.ativo_total + a.ativo_total) / 2
         else j.ativo_total end as ativo_base_roa,
    case when a.patrimonio_liquido_cosif is not null then (j.patrimonio_liquido_cosif + a.patrimonio_liquido_cosif) / 2
         else j.patrimonio_liquido_cosif end as pl_base_roe_cosif,
    case when a.ativo_total_cosif is not null then (j.ativo_total_cosif + a.ativo_total_cosif) / 2
         else j.ativo_total_cosif end as ativo_base_roa_cosif,
    (a.patrimonio_liquido is not null) as base_media
  from j
  left join j a on a.entidade = j.entidade and a.data_base = make_date(year(j.data_base) - 1, 12, 31)
  left join j t on t.entidade = j.entidade and t.data_base = last_day((j.data_base - interval 3 month)::date)
),
x as (
  select *,
    round(100 * lucro_liquido_acum * 12.0 / month(data_base) / nullif(pl_base_roe, 0), 3) as roe_anualizado,
    round(100 * lucro_liquido_acum * 12.0 / month(data_base) / nullif(ativo_base_roa, 0), 3) as roa_anualizado,
    round(100 * lucro_liquido_acum_cosif * 12.0 / month(data_base) / nullif(pl_base_roe_cosif, 0), 3) as roe_anualizado_cosif,
    round(100 * lucro_liquido_acum_cosif * 12.0 / month(data_base) / nullif(ativo_base_roa_cosif, 0), 3) as roa_anualizado_cosif,
    round(100 * (ativo_total_cosif - ativo_total) / nullif(ativo_total, 0), 3) as dif_ativo_cosif_vs_ifdata
  from w
)
select data_base, grupo, entidade, cod_inst, instituicao,
  ativo_total, carteira_credito, captacoes, patrimonio_liquido,
  lucro_liquido_acum, lucro_liquido_trimestre, pl_base_roe, ativo_base_roa, base_media,
  roe_anualizado, roa_anualizado,
  round(median(roe_anualizado) filter (where grupo = 'pares') over (partition by data_base), 3) as roe_anualizado_mediana_pares,
  round(median(roa_anualizado) filter (where grupo = 'pares') over (partition by data_base), 3) as roa_anualizado_mediana_pares,
  ativo_total_cosif, patrimonio_liquido_cosif, lucro_liquido_acum_cosif,
  roe_anualizado_cosif, roa_anualizado_cosif, dif_ativo_cosif_vs_ifdata
from x
"""

_CARTEIRA = f"""
with b as (
  select {_mes('i.data_base')} as data_base, d.grupo, d.entidade, i.cod_inst, i.relatorio, i.nome_relatorio,
    i.grupo_conta, i.conta, i.nome_coluna, i.valor
  from silver.ifdata i join gold.dim_entidade d on {_DIM_IFDATA}
  where i.relatorio in ('7', '11', '13', '14', '16') or i.nome_relatorio ilike '%nível de risco%'
     or i.nome_relatorio ilike '%nivel de risco%'
),
tot as (
  select data_base, entidade, max(valor) as carteira_total
  from b where relatorio = '7' and conta = '24454' group by all
),
l as (
  select data_base, grupo, entidade, cod_inst,
    case when relatorio = '7' then 'indexador'
         when relatorio in ('11', '13') then 'modalidade'
         when relatorio = '14' then 'porte'
         when relatorio = '16' then 'carteira_res4966'
         else 'nivel_risco' end as corte,
    case when relatorio = '11' then 'PF' when relatorio in ('13', '14') then 'PJ' else 'PF+PJ' end as segmento,
    case when relatorio in ('11', '13') then grupo_conta else replace(nome_coluna, chr(10), ' ') end as categoria,
    valor
  from b
  where valor is not null
    and (relatorio not in ('11', '13') or nome_coluna = 'Total')
)
select l.data_base, l.grupo, l.entidade, l.cod_inst, l.corte, l.segmento, l.categoria, l.valor,
  t.carteira_total, round(100 * l.valor / nullif(t.carteira_total, 0), 3) as participacao_na_carteira
from l left join tot t using (data_base, entidade)
"""

_JUROS = f"""
with j as (
  select {_mes('s.data_base')} as data_base, s.periodicidade, s.inicio_periodo, s.fim_periodo, s.segmento, s.modalidade,
    s.grupo, coalesce(d.entidade, s.grupo || '_nao_mapeado') as entidade,
    avg(s.taxa_ao_mes) as taxa_ao_mes, avg(s.taxa_ao_ano) as taxa_ao_ano,
    count(*) as n_instituicoes, min(s.posicao) as melhor_posicao
  from silver.juros s
  left join gold.dim_entidade d on d.tipo_id = 'cnpj_raiz' and d.identificador = s.cnpj8
  where s.grupo in ('safra', 'pares') group by all
),
m as (
  select *,
    median(taxa_ao_mes) filter (where grupo = 'pares')
      over (partition by data_base, periodicidade, inicio_periodo, fim_periodo, segmento, modalidade) as m_mes,
    median(taxa_ao_ano) filter (where grupo = 'pares')
      over (partition by data_base, periodicidade, inicio_periodo, fim_periodo, segmento, modalidade) as m_ano,
    count(*) filter (where grupo = 'pares')
      over (partition by data_base, periodicidade, inicio_periodo, fim_periodo, segmento, modalidade) as n_pares
  from j
)
select data_base, periodicidade, inicio_periodo, fim_periodo, segmento, modalidade, grupo, entidade,
  round(taxa_ao_mes, 3) as taxa_ao_mes, round(taxa_ao_ano, 3) as taxa_ao_ano, n_instituicoes, melhor_posicao,
  round(m_mes, 3) as mediana_pares_ao_mes, round(m_ano, 3) as mediana_pares_ao_ano, n_pares,
  round(taxa_ao_ano - m_ano, 3) as dif_ao_ano_vs_mediana_pares
from m
"""

_RECLAMACOES = f"""
with r as (
  select {_mes('s.data_base')} as data_base, s.ano, s.periodo, s.categoria, s.tipo_instituicao, s.instituicao, s.cnpj_if,
    coalesce(n.grupo, s.grupo) as grupo, coalesce(n.entidade, dd.entidade) as entidade,
    s.indice, s.qtd_reclamacoes_respondidas, s.qtd_procedentes, s.qtd_procedentes_extrapoladas, s.qtd_analisadas,
    s.qtd_clientes_total
  from silver.reclamacoes s
  left join gold.dim_reclamacao_nome n on n.instituicao = s.instituicao
  left join gold.dim_entidade dd
    on dd.tipo_id = 'cnpj_raiz' and s.cnpj_if is not null and dd.identificador = left(s.cnpj_if, 8)
),
m as (
  -- a mediana da categoria usa TODAS as instituicoes do ranking naquela categoria (inclusive o proprio Safra)
  select *,
    median(indice) filter (where grupo = 'pares') over (partition by data_base, tipo_instituicao) as m_ind,
    median(indice) over (partition by data_base, categoria, tipo_instituicao) as m_cat,
    count(*) over (partition by data_base, categoria, tipo_instituicao) as n_cat
  from r
),
f as (select * from m where grupo is not null)
select data_base, grupo, entidade, instituicao, tipo_instituicao, categoria, ano, periodo,
  round(indice, 3) as indice, qtd_reclamacoes_respondidas, qtd_procedentes, qtd_procedentes_extrapoladas, qtd_analisadas,
  qtd_clientes_total,
  -- conferencia: o indice publicado e procedentes extrapoladas por milhao de clientes (CCS e SCR)
  round(qtd_procedentes_extrapoladas * 1000000.0 / nullif(qtd_clientes_total, 0), 3) as indice_recalculado,
  round(m_ind, 3) as indice_mediana_pares, round(indice - m_ind, 3) as indice_dif_pares,
  round(m_cat, 3) as indice_mediana_categoria, n_cat as n_categoria, round(indice - m_cat, 3) as indice_dif_categoria
from f
"""

_AGENCIAS = f"""
with e as (
  select {_mes('s.data_base')} as data_base, s.grupo, coalesce(d.entidade, s.grupo || '_nao_mapeado') as entidade,
    count(distinct s.agencia) filter (where s.nivel = 'agencia') as n_agencias,
    count(distinct s.cod_municipio) filter (where s.nivel = 'agencia') as n_municipios,
    sum(s.saldo) filter (where s.nivel = 'municipio' and s.verbete = '160') as estban_operacoes_credito,
    sum(s.saldo) filter (where s.nivel = 'municipio' and s.verbete = '432') as estban_depositos_a_prazo,
    sum(s.saldo) filter (where s.nivel = 'municipio' and s.verbete = '420') as estban_depositos_poupanca
  from silver.estban s
  left join gold.dim_entidade d on d.tipo_id = 'cnpj_raiz' and d.identificador = s.cnpj
  where s.grupo in ('safra', 'pares') group by all
)
select *,
  median(n_agencias) filter (where grupo = 'pares') over (partition by data_base) as n_agencias_mediana_pares,
  median(n_municipios) filter (where grupo = 'pares') over (partition by data_base) as n_municipios_mediana_pares
from e
"""

# papel de cada fundo E classe em relacao ao grupo Safra (administrador e/ou gestor). Preferencia:
# registro_classe > registro_fundo > legado (cad_fi); o de menor prioridade so vale para CNPJ ausentes dos anteriores.
_FUNDOS_PAPEL = """
with safra as (
  select identificador from gold.dim_entidade where grupo = 'safra' and tipo_id = 'cnpj_raiz'
),
x as (
  select cnpj_classe as cnpj,
    coalesce(left(cnpj_administrador, 8) in (select identificador from safra), false) as administrado,
    coalesce(left(cpf_cnpj_gestor, 8) in (select identificador from safra), false) as gerido, 1 as prio
  from silver.fundos_classe
  union all
  -- CNPJ do FUNDO (pos-RCVM 175 o CDA pode apontar para o fundo ou para a classe)
  select cnpj_fundo,
    coalesce(left(cnpj_administrador, 8) in (select identificador from safra), false),
    coalesce(left(cpf_cnpj_gestor, 8) in (select identificador from safra), false), 2
  from silver.fundos_fundo
  union all
  select cnpj_fundo,
    coalesce(left(cnpj_admin, 8) in (select identificador from safra), false),
    coalesce(left(cpf_cnpj_gestor, 8) in (select identificador from safra), false), 3
  from silver.fundos_legado
),
y as (select * from x qualify prio = min(prio) over (partition by cnpj))
select cnpj, bool_or(administrado) as administrado, bool_or(gerido) as gerido
from y where cnpj is not null group by cnpj
having bool_or(administrado) or bool_or(gerido)
"""

# ultimo dia (util) com informe de cada mes: o ultimo dia com pelo menos metade dos fundos do dia mais cheio
_FUNDOS_DIAS = """
with n as (
  select dt_comptc, count(distinct cnpj_fundo_classe) as n from silver.informe group by 1
),
m as (select *, max(n) over (partition by date_trunc('month', dt_comptc)) as mx from n)
select date_trunc('month', dt_comptc)::date as mes, max(dt_comptc) as data_base,
  max(dt_comptc) >= last_day(max(dt_comptc)) - interval 4 day as mes_encerrado
from m where n >= 0.5 * mx group by 1
"""

_FUNDOS_CDA_DIAS = """
select date_trunc('month', dt_comptc)::date as mes, max(dt_comptc) as data_cda from silver.cda group by 1
"""

_FUNDOS = """
with cls as (
  -- PL por classe: a linha da classe; classes que so tem linhas de subclasse (comum no informe pos-RCVM 175)
  -- usam a soma das subclasses. Nunca soma classe + subclasses.
  select i.dt_comptc, i.cnpj_fundo_classe,
    coalesce(max(i.vl_patrim_liq) filter (where i.id_subclasse is null),
             sum(i.vl_patrim_liq) filter (where i.id_subclasse is not null)) as pl
  from silver.informe i join gold.fundos_dias d on i.dt_comptc = d.data_base
  group by all
),
v(visao) as (values ('administrado'), ('gerido'), ('ambos')),
pl as (
  select c.dt_comptc as data_base, v.visao, count(*) as n_fundos, sum(c.pl) as pl_bruto
  from cls c join gold.fundos_papel p on p.cnpj = c.cnpj_fundo_classe
  join v on (v.visao = 'ambos' or (v.visao = 'administrado' and p.administrado) or (v.visao = 'gerido' and p.gerido))
  group by all
),
pos as (
  -- posicoes em cotas de fundos do proprio conjunto (a visao): detentor e cotizado no mesmo conjunto
  select d.data_base, v.visao, sum(c.vl_merc_pos_final) as cotas_do_grupo, max(k.data_cda) as data_cda
  from gold.fundos_dias d
  join gold.fundos_cda_dias k on k.mes = d.mes
  join silver.cda c on c.dt_comptc = k.data_cda and c.cnpj_fundo_classe_cota is not null
  join gold.fundos_papel ph on ph.cnpj = c.cnpj_fundo_classe
  join gold.fundos_papel pc on pc.cnpj = c.cnpj_fundo_classe_cota
  join v on (v.visao = 'ambos'
             or (v.visao = 'administrado' and ph.administrado and pc.administrado)
             or (v.visao = 'gerido' and ph.gerido and pc.gerido))
  group by all
),
conf as (
  -- apos a RCVM 175, quase todas as posicoes em "Cotas de Fundos" vao para o arquivo CONFID do CDA, sem o CNPJ do ativo
  -- (CNPJ_FUNDO_CLASSE_COTA vazio): nao ha como saber se sao do grupo. Soma por visao do detentor = teto da parte
  -- nao identificada
  select d.data_base, v.visao, sum(c.vl_merc_pos_final) as cotas_confidenciais_sem_cnpj
  from gold.fundos_dias d
  join gold.fundos_cda_dias k on k.mes = d.mes
  join silver.cda c on c.dt_comptc = k.data_cda and c.cnpj_fundo_classe_cota is null and c.tp_aplic ilike 'cotas de fundos%'
  join gold.fundos_papel ph on ph.cnpj = c.cnpj_fundo_classe
  join v on (v.visao = 'ambos' or (v.visao = 'administrado' and ph.administrado) or (v.visao = 'gerido' and ph.gerido))
  group by all
),
cob as (
  -- cobertura do CDA: fundos da visao com alguma linha de carteira no mes (o CDA do mes mais recente
  -- costuma vir incompleto, o que subestima as cotas do grupo)
  select d.data_base, v.visao, count(distinct c.cnpj_fundo_classe) as n_fundos_com_cda
  from gold.fundos_dias d
  join gold.fundos_cda_dias k on k.mes = d.mes
  join silver.cda c on c.dt_comptc = k.data_cda
  join (select distinct dt_comptc, cnpj_fundo_classe from silver.informe) i
    on i.dt_comptc = d.data_base and i.cnpj_fundo_classe = c.cnpj_fundo_classe
  join gold.fundos_papel ph on ph.cnpj = c.cnpj_fundo_classe
  join v on (v.visao = 'ambos' or (v.visao = 'administrado' and ph.administrado) or (v.visao = 'gerido' and ph.gerido))
  group by all
)
select pl.data_base, 'safra' as grupo, 'fundos_safra' as entidade, pl.visao, pl.n_fundos, pl.pl_bruto,
  coalesce(pos.cotas_do_grupo, 0) as posicoes_em_cotas_do_grupo,
  pl.pl_bruto - coalesce(pos.cotas_do_grupo, 0) as pl_deduplicado,
  coalesce(conf.cotas_confidenciais_sem_cnpj, 0) as cotas_confidenciais_sem_cnpj,
  pl.pl_bruto - coalesce(pos.cotas_do_grupo, 0) - coalesce(conf.cotas_confidenciais_sem_cnpj, 0) as pl_deduplicado_minimo,
  -- metrica principal: PL minimo a partir de @@DESDE@@ (cotas confidenciais sem CNPJ tratadas como intragrupo),
  -- identificado antes; `pl_deduplicado` (teto) fica como informativo
  case when pl.data_base >= date '@@DESDE@@'
       then pl.pl_bruto - coalesce(pos.cotas_do_grupo, 0) - coalesce(conf.cotas_confidenciais_sem_cnpj, 0)
       else pl.pl_bruto - coalesce(pos.cotas_do_grupo, 0) end as pl_deduplicado_principal,
  case when pl.data_base >= date '@@DESDE@@' then 'minimo' else 'identificado' end as metodo_principal,
  pos.data_cda, coalesce(cob.n_fundos_com_cda, 0) as n_fundos_com_cda, d.mes_encerrado
from pl
left join pos on pos.data_base = pl.data_base and pos.visao = pl.visao
left join cob on cob.data_base = pl.data_base and cob.visao = pl.visao
left join conf on conf.data_base = pl.data_base and conf.visao = pl.visao
join gold.fundos_dias d on d.data_base = pl.data_base
"""

_FUNDOS_DECOMPOSICAO = """
-- PL bruto (sem deduplicar) por papel do grupo, nas datas do gold.fundos. O papel `so_administrado` e exatamente o PL de
-- classes cujo administrador e Safra e o gestor e de fora do grupo (administracao fiduciaria para terceiros)
with cls as (
  select i.dt_comptc, i.cnpj_fundo_classe,
    coalesce(max(i.vl_patrim_liq) filter (where i.id_subclasse is null),
             sum(i.vl_patrim_liq) filter (where i.id_subclasse is not null)) as pl
  from silver.informe i join gold.fundos_dias d on i.dt_comptc = d.data_base
  group by all
)
select c.dt_comptc as data_base, 'safra' as grupo, 'fundos_safra' as entidade,
  case when p.administrado and p.gerido then 'administra_e_gere'
       when p.administrado then 'so_administrado' else 'so_gerido' end as papel,
  count(*) as n_fundos, sum(c.pl) as pl_bruto
from cls c join gold.fundos_papel p on p.cnpj = c.cnpj_fundo_classe
group by all
"""

_FUNDOS_CONCILIACAO = """
with ses as (
  select p.damesano, sum(p.pl) as pl_ses, sum(v.valor) as provisoes_ses
  from silver.ses_pl_margem p
  left join silver.ses_prov v on v.codigo_fip = p.codigo_fip and v.damesano = p.damesano
  where p.grupo = 'safra' group by 1
),
dec as (
  select data_base,
    sum(pl_bruto) filter (where papel = 'so_administrado') as pl_so_administrado,
    sum(pl_bruto) filter (where papel = 'so_gerido') as pl_so_gerido,
    sum(pl_bruto) filter (where papel = 'administra_e_gere') as pl_administra_e_gere
  from gold.fundos_decomposicao group by 1
),
base as (
  select r.id, r.descricao, r.data_referencia as data_base, r.componentes, r.visao, r.inclui_previdencia_ses,
    r.valor_referencia, f.data_base as data_base_gold, f.metodo_principal,
    f.pl_deduplicado_principal as pl_principal, f.pl_deduplicado as pl_teto,
    s.pl_ses as pl_ses_safra, s.provisoes_ses as provisoes_ses_safra,
    case when r.inclui_previdencia_ses then coalesce(s.provisoes_ses, 0) else 0 end as previdencia_incluida,
    r.fonte, f.pl_bruto as pl_bruto_visao, f.posicoes_em_cotas_do_grupo, f.cotas_confidenciais_sem_cnpj
  from gold.referencias_fundos r
  left join gold.fundos f on f.visao = r.visao and date_trunc('month', f.data_base) = date_trunc('month', r.data_referencia)
  left join ses s on s.damesano = strftime(r.data_referencia, '%Y%m')
)
select b.id, b.descricao, b.data_base, 'safra' as grupo, 'fundos_safra' as entidade, b.componentes, b.visao,
  b.inclui_previdencia_ses, b.valor_referencia, b.data_base_gold, b.metodo_principal,
  b.pl_bruto_visao, b.posicoes_em_cotas_do_grupo, b.cotas_confidenciais_sem_cnpj,
  b.pl_principal as pl_dedup_visao,
  -- (b): previdencia entra pelas PROVISOES TECNICAS das entidades Safra no SES (regra padrao)
  b.provisoes_ses_safra, b.pl_ses_safra,
  b.pl_principal + b.previdencia_incluida as valor_gold,
  b.pl_principal + b.previdencia_incluida - b.valor_referencia as diferenca,
  round(100 * (b.pl_principal + b.previdencia_incluida - b.valor_referencia) / nullif(b.valor_referencia, 0), 3) as diferenca_pct,
  -- informativos: teto (so cotas identificadas) e variante com o PL do SES no lugar das provisoes
  b.pl_teto + b.previdencia_incluida as valor_gold_teto,
  round(100 * (b.pl_teto + b.previdencia_incluida - b.valor_referencia) / nullif(b.valor_referencia, 0), 3) as diferenca_teto_pct,
  b.pl_principal + case when b.inclui_previdencia_ses then coalesce(b.pl_ses_safra, 0) else 0 end as valor_gold_com_pl_ses,
  b.pl_principal + case when b.inclui_previdencia_ses then coalesce(b.pl_ses_safra, 0) else 0 end - b.valor_referencia
    as diferenca_com_pl_ses,
  d.pl_so_administrado, d.pl_so_gerido, d.pl_administra_e_gere,
  d.pl_so_administrado as pl_adm_safra_gestor_externo,
  b.fonte
from base b left join dec d on d.data_base = b.data_base_gold
"""

_SEGUROS = f"""
with s as (
  select damesano, codigo_fip, any_value(grupo) as grupo,
    sum(premio_direto) as premio_direto, sum(premio_ganho) as premio_ganho,
    sum(sinistro_direto) as sinistro_direto, sum(sinistro_ocorrido) as sinistro_ocorrido
  from silver.ses_seguros where grupo is not null group by 1, 2
),
j as (
  select {_mes('s.damesano')} as data_base, s.grupo, coalesce(d.entidade, s.grupo || '_nao_mapeado') as entidade,
    s.codigo_fip, c.nome as seguradora,
    s.premio_direto, s.premio_ganho, s.sinistro_direto, s.sinistro_ocorrido,
    sum(s.premio_direto) over (partition by s.codigo_fip, left(s.damesano, 4) order by s.damesano) as premio_direto_ano,
    sum(s.premio_ganho) over (partition by s.codigo_fip, left(s.damesano, 4) order by s.damesano) as premio_ganho_ano,
    sum(s.sinistro_ocorrido) over (partition by s.codigo_fip, left(s.damesano, 4) order by s.damesano) as sinistro_ocorrido_ano,
    p.valor as provisoes_tecnicas, m.pl as patrimonio_liquido, m.plajustado as pl_ajustado, m.margem as margem_solvencia
  from s
  left join gold.dim_entidade d on d.tipo_id = 'fip' and d.identificador = s.codigo_fip
  left join silver.ses_cadastro c on c.codigo_fip = s.codigo_fip
  left join silver.ses_prov p on p.codigo_fip = s.codigo_fip and p.damesano = s.damesano
  left join silver.ses_pl_margem m on m.codigo_fip = s.codigo_fip and m.damesano = s.damesano
),
k as (
  select *,
    round(100 * sinistro_ocorrido / nullif(premio_ganho, 0), 3) as sinistralidade,
    round(100 * sinistro_ocorrido_ano / nullif(premio_ganho_ano, 0), 3) as sinistralidade_ano
  from j
)
select *,
  round(median(sinistralidade_ano) filter (where grupo = 'pares') over (partition by data_base), 3)
    as sinistralidade_ano_mediana_pares,
  round(sinistralidade_ano - median(sinistralidade_ano) filter (where grupo = 'pares') over (partition by data_base), 3)
    as sinistralidade_ano_dif_pares
from k
"""

# nome da view -> (silver necessarias, sql)
VIEWS_GOLD: dict[str, tuple[tuple[str, ...], str]] = {
    "capital": (("ifdata",), _CAPITAL),
    "balanco_dre": (("ifdata", "cosif"), _BALANCO_DRE),
    "carteira": (("ifdata",), _CARTEIRA),
    "juros": (("juros",), _JUROS),
    "reclamacoes": (("reclamacoes",), _RECLAMACOES),
    "agencias": (("estban",), _AGENCIAS),
    "fundos_papel": (("fundos_classe", "fundos_legado", "fundos_fundo"), _FUNDOS_PAPEL),
    "fundos_dias": (("informe",), _FUNDOS_DIAS),
    "fundos_cda_dias": (("cda",), _FUNDOS_CDA_DIAS),
    "fundos": (("informe", "cda", "fundos_classe", "fundos_legado", "fundos_fundo"), _FUNDOS),
    "fundos_decomposicao": (("informe", "fundos_classe", "fundos_legado", "fundos_fundo"), _FUNDOS_DECOMPOSICAO),
    "fundos_conciliacao": (
        ("informe", "cda", "fundos_classe", "fundos_legado", "fundos_fundo", "ses_pl_margem", "ses_prov"), _FUNDOS_CONCILIACAO),
    "seguros": (("ses_seguros", "ses_pl_margem", "ses_prov", "ses_cadastro"), _SEGUROS),
}
VIEWS_EXPORTAVEIS = (
    "capital", "balanco_dre", "carteira", "juros", "reclamacoes", "agencias", "fundos", "fundos_conciliacao",
    "seguros",
)


@dataclass
class ResultadoGold:
    caminho: Path | None
    silver_views: list[str] = field(default_factory=list)
    criadas: list[str] = field(default_factory=list)
    ignoradas: dict[str, str] = field(default_factory=dict)
    erros: dict[str, str] = field(default_factory=dict)


def _glob_silver(settings: Settings, padrao: str) -> str:
    return (settings.silver_dir / padrao).as_posix()


DESDE_PL_MINIMO_PADRAO = "2026-08-01"


def aplicar_views(con: duckdb.DuckDBPyConnection, settings: Settings,
                  resultado: ResultadoGold | None = None) -> ResultadoGold:
    """Aplica em QUALQUER conexao DuckDB (arquivo ou memoria) os schemas, as views silver, as tabelas de
    configuracao e as views gold. Unica definicao das views: `criar_gold` (arquivo) e o app Streamlit
    (memoria) usam esta funcao. Idempotente."""
    resultado = resultado or ResultadoGold(None)
    con.execute("create schema if not exists silver")
    con.execute("create schema if not exists gold")
    for nome, padrao in SILVER_VIEWS.items():
        alvo = _glob_silver(settings, padrao)
        if glob.glob(alvo):
            con.execute(
                f"create or replace view silver.{nome} as select * from read_parquet('{alvo}', union_by_name = true)"
            )
            resultado.silver_views.append(nome)
        else:
            con.execute(f"drop view if exists silver.{nome}")

    # tabelas de configuracao (pequenas)
    con.execute("create or replace table gold.dim_entidade "
                "(tipo_id varchar, identificador varchar, grupo varchar, entidade varchar, origem varchar)")
    dim = construir_dim_entidade(settings)
    if dim:
        con.executemany("insert into gold.dim_entidade values (?, ?, ?, ?, ?)", dim)

    refs = carregar_referencias(settings)
    con.execute("create or replace table gold.dim_reclamacao_nome (instituicao varchar, grupo varchar, entidade varchar)")
    for item in refs.get("reclamacoes_nomes") or []:
        con.execute("insert into gold.dim_reclamacao_nome values (?, ?, ?)",
                    [item["instituicao"], item["grupo"], item["entidade"]])
    con.execute(
        "create or replace table gold.referencias_fundos (id varchar, descricao varchar, data_referencia date, "
        "componentes varchar, valor_referencia double, visao varchar, inclui_previdencia_ses boolean, fonte varchar)"
    )
    for linha in referencias_fundos(refs):
        con.execute("insert into gold.referencias_fundos values (?, ?, ?, ?, ?, ?, ?, ?)", linha)

    desde_minimo = str(refs.get("fundos_pl_minimo_desde", DESDE_PL_MINIMO_PADRAO))
    if len(desde_minimo) == 7:
        desde_minimo += "-01"
    date.fromisoformat(desde_minimo)  # valida
    for nome, (requer, sql) in VIEWS_GOLD.items():
        faltam = [r for r in requer if r not in resultado.silver_views]
        if faltam:
            resultado.ignoradas[nome] = "faltam fontes silver: " + ", ".join(faltam)
            con.execute(f"drop view if exists gold.{nome}")
            continue
        try:
            con.execute(f"create or replace view gold.{nome} as {sql.replace('@@DESDE@@', desde_minimo)}")
            resultado.criadas.append(nome)
        except duckdb.Error as exc:
            resultado.erros[nome] = str(exc).splitlines()[0]
    return resultado


def criar_gold(settings: Settings, caminho: Path | None = None) -> ResultadoGold:
    """(Re)cria views e tabelas de configuracao em `data/gold/safra.duckdb`. Idempotente."""
    caminho = caminho or settings.gold_db_path
    caminho.parent.mkdir(parents=True, exist_ok=True)
    resultado = ResultadoGold(caminho)
    con = duckdb.connect(str(caminho))
    try:
        aplicar_views(con, settings, resultado)
    finally:
        con.close()
    return resultado


def conectar_gold(settings: Settings, somente_leitura: bool = True) -> duckdb.DuckDBPyConnection:
    caminho = settings.gold_db_path
    if not caminho.is_file():
        raise FileNotFoundError(f"{caminho} nao existe; rode `safra-dados gold` antes")
    return duckdb.connect(str(caminho), read_only=somente_leitura)


# ---------------------------------------------------------------- exportacao

def exportar_view(settings: Settings, view: str, formato: str, destino: Path | None = None) -> Path:
    """Exporta `gold.<view>` para data/exports/<view>.csv|xlsx."""
    if view not in VIEWS_GOLD:
        raise ValueError(f"view desconhecida: {view!r}. Use uma de: {', '.join(VIEWS_EXPORTAVEIS)}")
    if formato not in ("csv", "xlsx"):
        raise ValueError(f"formato invalido: {formato!r} (csv ou xlsx)")
    destino = destino or settings.exports_dir / f"{view}.{formato}"
    destino.parent.mkdir(parents=True, exist_ok=True)
    con = conectar_gold(settings)
    try:
        try:
            if formato == "csv":
                con.execute(f"copy (select * from gold.{view}) to '{destino.as_posix()}' (header, delimiter ',')")
            else:
                import polars as pl

                df = pl.from_arrow(con.sql(f"select * from gold.{view}").to_arrow_table())
                if df.height >= 1_048_576:
                    raise ValueError(f"{view} tem {df.height} linhas, acima do limite do Excel; use csv")
                df.write_excel(destino)
        except duckdb.CatalogException as exc:
            raise ValueError(f"gold.{view} nao existe no banco (fonte silver ausente? rode `safra-dados gold`)") from exc
    finally:
        con.close()
    return destino


# ---------------------------------------------------------------- resumo

def _virgula(txt: str) -> str:
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def _bi(x: Any) -> str:
    """R$ em bilhoes com 3 casas (separador decimal virgula): R$ 29,117 bi."""
    return "n/d" if x is None else _virgula(f"R$ {x / 1e9:,.3f} bi")


def _pct(x: Any) -> str:
    """Recebe o valor JA em % (as views gold guardam indices em %): 18.141 -> 18,141%."""
    return "n/d" if x is None else _virgula(f"{x:,.3f}%")


def _num(x: Any) -> str:
    return "n/d" if x is None else _virgula(f"{x:,.3f}")


def resumo_safra(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Linhas de texto com o ultimo periodo do Safra em cada view gold disponivel."""
    existentes = {r[0] for r in con.execute("select view_name from duckdb_views() where schema_name = 'gold'").fetchall()}
    linhas: list[str] = []

    def um(sql: str, params: list | None = None):
        return con.execute(sql, params or []).fetchone()

    if "capital" in existentes:
        r = um("select * from gold.capital where grupo = 'safra' order by data_base desc, entidade limit 1")
        if r:
            c = dict(zip([d[0] for d in con.description], r))
            linhas.append(f"[capital] {c['data_base']}: PR {_bi(c['patrimonio_referencia'])}, Nivel I {_bi(c['capital_nivel_1'])}, "
                          f"Capital Principal {_bi(c['capital_principal'])}, RWA {_bi(c['rwa_total'])} "
                          f"(credito {_bi(c['rwa_credito'])}, mercado {_bi(c['rwa_mercado'])}, operacional {_bi(c['rwa_operacional'])})")
            linhas.append(f"          Basileia {_pct(c['indice_basileia'])} (mediana pares {_pct(c['indice_basileia_mediana_pares'])}), "
                          f"Nivel I {_pct(c['indice_nivel_1'])} (pares {_pct(c['indice_nivel_1_mediana_pares'])}), "
                          f"Capital Principal {_pct(c['indice_capital_principal'])} (pares {_pct(c['indice_capital_principal_mediana_pares'])})")
    if "balanco_dre" in existentes:
        r = um("select * from gold.balanco_dre where grupo = 'safra' order by data_base desc, entidade limit 1")
        if r:
            c = dict(zip([d[0] for d in con.description], r))
            linhas.append(f"[balanco_dre] {c['data_base']}: ativo {_bi(c['ativo_total'])}, carteira {_bi(c['carteira_credito'])}, "
                          f"captacoes {_bi(c['captacoes'])}, PL {_bi(c['patrimonio_liquido'])}, lucro acum. {_bi(c['lucro_liquido_acum'])}")
            linhas.append(f"          ROE anualizado {_pct(c['roe_anualizado'])} (pares {_pct(c['roe_anualizado_mediana_pares'])}), "
                          f"ROA anualizado {_pct(c['roa_anualizado'])} (pares {_pct(c['roa_anualizado_mediana_pares'])}); "
                          f"Cosif: ROE {_pct(c['roe_anualizado_cosif'])}, ROA {_pct(c['roa_anualizado_cosif'])}")
    if "carteira" in existentes:
        rows = con.execute(
            "select corte, categoria, valor, participacao_na_carteira from gold.carteira where grupo = 'safra' "
            "and data_base = (select max(data_base) from gold.carteira where grupo = 'safra') "
            "and corte in ('porte', 'indexador') and valor > 0 and categoria not ilike 'Total%' "
            "order by valor desc limit 5").fetchall()
        if rows:
            linhas.append("[carteira] maiores cortes: " + "; ".join(f"{c}/{k} {_bi(v)} ({_pct(p)})" for c, k, v, p in rows))
    if "juros" in existentes:
        rows = con.execute(
            "select modalidade, taxa_ao_ano, mediana_pares_ao_ano, n_pares from gold.juros where grupo = 'safra' and "
            "periodicidade = 'mensal' and data_base = (select max(data_base) from gold.juros where grupo = 'safra' "
            "and periodicidade = 'mensal') and mediana_pares_ao_ano is not null order by modalidade limit 4").fetchall()
        if rows:
            linhas.append("[juros] taxa a.a. Safra vs mediana pares: " + "; ".join(
                f"{m} {_pct(t)} vs {_pct(p)} (n={n})" for m, t, p, n in rows))
    if "reclamacoes" in existentes:
        r = um("select data_base, indice, indice_mediana_pares, qtd_reclamacoes_respondidas, indice_mediana_categoria, "
               "categoria, n_categoria from gold.reclamacoes where grupo = 'safra' and tipo_instituicao = 'Conglomerado' "
               "order by data_base desc limit 1")
        if r:
            linhas.append(f"[reclamacoes] {r[0]}: indice {_num(r[1])} (mediana pares nomeados {_num(r[2])}; "
                          f"mediana da categoria '{r[5]}' {_num(r[4])}, n={r[6]}), {r[3]} reclamacoes respondidas")
    if "agencias" in existentes:
        r = um("select data_base, sum(n_agencias), any_value(n_agencias_mediana_pares) from gold.agencias where grupo = 'safra' "
               "group by data_base order by data_base desc limit 1")
        if r:
            linhas.append(f"[agencias] {r[0]}: {r[1]} agencias (mediana de agencias por par: {_num(r[2])})")
    if "fundos" in existentes:
        rows = con.execute(
            "select visao, n_fundos, pl_bruto, posicoes_em_cotas_do_grupo, pl_deduplicado, data_base, mes_encerrado, "
            "n_fundos_com_cda, cotas_confidenciais_sem_cnpj, pl_deduplicado_minimo, pl_deduplicado_principal, "
            "metodo_principal from gold.fundos "
            "where data_base = (select coalesce(max(data_base) filter (where mes_encerrado), max(data_base)) "
            "from gold.fundos) order by visao").fetchall()
        for v, n, bruto, cotas, dedup, db, enc, ncda, conf, minimo, principal, metodo in rows:
            linhas.append(
                f"[fundos] {db} {v}: {n} fundos, PL bruto {_bi(bruto)}; principal ({metodo}) {_bi(principal)}; "
                f"teto {_bi(dedup)} (cotas do grupo identificadas {_bi(cotas)}); minimo {_bi(minimo)} "
                f"(cotas confidenciais {_bi(conf)}) [CDA em {ncda}/{n} fundos]"
                + ("" if enc else " (mes em curso)"))
    if "seguros" in existentes:
        rows = con.execute(
            "select data_base, entidade, premio_direto_ano, sinistralidade_ano, sinistralidade_ano_mediana_pares, patrimonio_liquido, "
            "provisoes_tecnicas from gold.seguros where grupo = 'safra' "
            "and data_base = (select max(data_base) from gold.seguros where grupo = 'safra') order by entidade").fetchall()
        for db, ent, prem, sin, med, pl, prov in rows:
            linhas.append(f"[seguros] {db} {ent}: premios no ano {_bi(prem)}, sinistralidade {_pct(sin)} (mediana pares {_pct(med)}), "
                          f"PL {_bi(pl)}, provisoes {_bi(prov)}")
    return linhas


def conciliacao_fundos(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    try:
        cur = con.execute("select * from gold.fundos_conciliacao order by data_base, id")
    except duckdb.Error:
        return []
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]
