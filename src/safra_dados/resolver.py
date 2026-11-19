"""Resolvedor de entidades: consulta o IfDataCadastro (BCB Olinda IFDATA) e grava
config/entidades_resolvidas.yaml para revisao humana (nunca altera
config/entidades.yaml).

Status por entidade:
- confirmado:    ha identificador no YAML e ele existe no cadastro; os CNPJs do
                 conglomerado sao coletados por identificador (CNPJ lider /
                 codigo de conglomerado). Usado na marcacao.
- resolvido:     (so com `resolver_por_nome_lider: true`) o nome casou com UMA
                 unica instituicao lider de conglomerado ativa. Usado na marcacao.
- ambiguo:       mais de uma lider casou; nada e marcado.
- sugestao:      entidade sem identificador; candidatos por nome apenas para
                 revisao humana. NUNCA usada na marcacao ate alguem copiar o
                 identificador para entidades.yaml ou marcar `aprovado: true`.
- nao_encontrado / falha_api.

Sugestoes SUSEP (secao `susep.sugestoes`, so para revisao humana): empresas da LISTAEMPRESAS
(codigo FIP + CNPJ) que casam por CNPJ raiz ou por `busca_nome_susep`; `aprovado: true` passa a
marcar linhas (por CNPJ e por FIP nas tabelas do SES).

Sugestoes CVM (secao `cvm.sugestoes`, so para revisao humana): para cada entidade,
os registros dos cadastros da CVM (companhia aberta, intermediario, administrador de
carteira, coordenador de oferta) que casam por CNPJ raiz (identificadores ja conhecidos
da entidade/conglomerado) ou por nome (`busca_nome_cvm`, ou `busca_nome`). Item com
`aprovado: true` passa a marcar linhas (por CNPJ); os demais nunca marcam.
"""

from __future__ import annotations

from typing import Any

import yaml

from .config import Settings, load_entidades, load_entidades_resolvidas
from .connectors.bcb_ifdata import BcbIfDataConnector, latest_available_anomes
from .connectors.cvm_cadastros import CvmCadastrosConnector, linhas_para_busca
from .connectors.susep_ses import SusepSesConnector, parse_lista_empresas
from .entidades import mapa_de_dicts, normalizar_identificador, normalizar_nome
from .http import DownloadError, SafraDadosClient
from .manifest import now_iso


def _candidato(row: dict) -> dict[str, Any]:
    return {
        "cod_inst": row.get("CodInst"),
        "nome_instituicao": row.get("NomeInstituicao"),
        "codigo_conglomerado_prudencial": row.get("CodConglomeradoPrudencial") or None,
        "codigo_conglomerado_financeiro": row.get("CodConglomeradoFinanceiro") or None,
        "cnpj_instituicao_lider": row.get("CnpjInstituicaoLider") or None,
        "situacao": row.get("Situacao"),
    }


def _e_lider_ativa(row: dict) -> bool:
    return bool(row.get("CodInst")) and row.get("CodInst") == row.get("CnpjInstituicaoLider") and row.get("Situacao") == "A"


def _membros(cadastro: list[dict], cnpj: str | None, cong_pru: str | None, cong_fin: str | None) -> list[dict]:
    """Linhas do cadastro que pertencem ao conglomerado, sempre por identificador."""
    membros = []
    for row in cadastro:
        if (
            (cnpj and (row.get("CodInst") == cnpj or row.get("CnpjInstituicaoLider") == cnpj))
            or (cong_pru and row.get("CodConglomeradoPrudencial") == cong_pru)
            or (cong_fin and row.get("CodConglomeradoFinanceiro") == cong_fin)
        ):
            membros.append(row)
    return membros


def _preencher(entrada: dict[str, Any], membros: list[dict], principal: dict) -> None:
    entrada["cnpj"] = principal.get("CodInst")
    entrada["codigo_conglomerado_prudencial"] = principal.get("CodConglomeradoPrudencial") or None
    entrada["codigo_conglomerado_financeiro"] = principal.get("CodConglomeradoFinanceiro") or None
    entrada["cnpjs_relacionados"] = sorted(
        {m["CodInst"] for m in membros if m.get("CodInst") and len(m["CodInst"]) == 8}
    )


def resolver_entidade(cfg: dict, cadastro: list[dict], erro_api: str | None) -> dict[str, Any]:
    """Resolve uma entidade (pura: sem rede nem disco)."""
    if erro_api:
        return {"status": "falha_api", "candidatos": [], "erro": erro_api}

    cnpj = normalizar_identificador(cfg.get("cnpj_raiz") or cfg.get("cnpj"))
    cong_pru = normalizar_identificador(cfg.get("codigo_conglomerado_prudencial"))
    cong_fin = normalizar_identificador(cfg.get("codigo_conglomerado_financeiro"))
    termos = [t for t in (normalizar_nome(x) for x in cfg.get("busca_nome", [])) if t]

    # 1) identificador conhecido -> confirmacao exata
    if cnpj or cong_pru or cong_fin:
        membros = _membros(cadastro, cnpj, cong_pru, cong_fin)
        if not membros:
            return {"status": "nao_encontrado", "candidatos": []}
        principal = next((m for m in membros if cnpj and m.get("CodInst") == cnpj), membros[0])
        entrada: dict[str, Any] = {"status": "confirmado", "candidatos": [_candidato(m) for m in membros]}
        _preencher(entrada, membros, principal)
        if cnpj:
            entrada["cnpj"] = cnpj
        return entrada

    # 2) sem identificador: busca por nome
    por_nome = [
        row for row in cadastro
        if any(t in normalizar_nome(row.get("NomeInstituicao")) for t in termos)
    ]

    if cfg.get("resolver_por_nome_lider"):
        lideres = [r for r in por_nome if _e_lider_ativa(r)]
        if len(lideres) == 1:
            lider = lideres[0]
            membros = _membros(
                cadastro, lider["CodInst"],
                normalizar_identificador(lider.get("CodConglomeradoPrudencial")),
                normalizar_identificador(lider.get("CodConglomeradoFinanceiro")),
            )
            entrada = {"status": "resolvido", "candidatos": [_candidato(lider)]}
            _preencher(entrada, membros, lider)
            return entrada
        return {
            "status": "ambiguo" if lideres else "nao_encontrado",
            "candidatos": [_candidato(r) for r in lideres],
        }

    return {
        "status": "sugestao" if por_nome else "nao_encontrado",
        "candidatos": [_candidato(r) for r in por_nome],
    }


def _situacao_ativa(situacao: str | None) -> bool:
    return normalizar_nome(situacao).startswith(("ATIVO", "EM FUNCIONAMENTO NORMAL"))


def sugerir_cvm(
    cfg: dict, entrada: dict[str, Any], linhas_cvm: list[dict], ids_no_mapa: set[str]
) -> list[dict[str, Any]]:
    """Registros CVM candidatos de uma entidade (pura). Agrupa por CNPJ raiz."""
    ids = [cfg.get("cnpj_raiz"), cfg.get("cnpj"), entrada.get("cnpj"), *(entrada.get("cnpjs_relacionados") or [])]
    raizes = {r for r in (normalizar_identificador(i) for i in ids) if r and len(r) == 8 and r.isdigit()}
    termos = [t for t in (normalizar_nome(x) for x in (cfg.get("busca_nome_cvm") or cfg.get("busca_nome") or [])) if t]

    por_raiz: dict[str, dict[str, Any]] = {}
    for linha in linhas_cvm:
        raiz = linha["cnpj_raiz"]
        nomes = normalizar_nome(linha.get("nome")) + "|" + normalizar_nome(linha.get("nome_comercial"))
        por_id = raiz in raizes
        por_nome = any(t in nomes for t in termos)
        if not (por_id or por_nome):
            continue
        item = por_raiz.setdefault(raiz, {
            "cnpj": linha["cnpj"], "cnpj_raiz": raiz, "denominacao": linha.get("nome"),
            "origem": set(), "ativo": False, "no_mapa": raiz in ids_no_mapa, "aprovado": False, "registros": [],
        })
        item["origem"].add("identificador" if por_id else "nome")
        registro = {"cadastro": linha["cadastro"], "categoria": linha.get("categoria"), "situacao": linha.get("situacao")}
        if registro not in item["registros"]:
            item["registros"].append(registro)
        if _situacao_ativa(linha.get("situacao")):
            if not item["ativo"]:
                item["denominacao"] = linha.get("nome")
            item["ativo"] = True

    itens = list(por_raiz.values())
    for item in itens:
        item["origem"] = "+".join(sorted(item["origem"]))
    return sorted(itens, key=lambda i: (not i["ativo"], i["denominacao"] or ""))


def _resolver_cvm(
    settings: Settings, grupos_cfg: dict, grupos_res: dict, anterior: dict, linhas_cvm: list[dict] | None
) -> dict[str, Any]:
    """Monta a secao `cvm` do YAML resolvido. Em falha, preserva as sugestoes anteriores."""
    cvm_anterior = (anterior or {}).get("cvm") or {}
    erro: str | None = None
    if linhas_cvm is None:
        client = SafraDadosClient(settings.http)
        try:
            conector = CvmCadastrosConnector(settings, client=client)
            linhas_cvm = linhas_para_busca(conector.baixar_para_busca())
        except (DownloadError, RuntimeError) as exc:
            erro = str(exc)
        finally:
            client.close()
    if erro is not None or linhas_cvm is None:
        return {**cvm_anterior, "status": "falha_api", "erro": erro, "tentado_em": now_iso()}

    ids_no_mapa = set(mapa_de_dicts(load_entidades(settings), {"grupos": grupos_res}).por_identificador)
    aprovados = {
        (g, e, i["cnpj_raiz"])
        for g, por_ent in (cvm_anterior.get("sugestoes") or {}).items()
        for e, itens in (por_ent or {}).items()
        for i in itens or [] if i.get("aprovado")
    }
    sugestoes = {g: v for g, v in (cvm_anterior.get("sugestoes") or {}).items() if g not in grupos_cfg}
    for nome_grupo, grupo_cfg in grupos_cfg.items():
        por_entidade = {}
        for nome, cfg in (grupo_cfg.get("entidades") or {}).items():
            entrada = grupos_res[nome_grupo]["entidades"][nome]
            itens = sugerir_cvm(cfg or {}, entrada, linhas_cvm, ids_no_mapa)
            for item in itens:
                item["aprovado"] = (nome_grupo, nome, item["cnpj_raiz"]) in aprovados
            por_entidade[nome] = itens
        sugestoes[nome_grupo] = por_entidade
    return {
        "gerado_em": now_iso(),
        "status": "ok",
        "erro": None,
        "fonte": "cvm: cia_aberta-cad, intermed-cad, adm_cart-cad, coord_oferta-cad",
        "sugestoes": sugestoes,
    }


def sugerir_susep(
    cfg: dict, entrada: dict[str, Any], lista: list[dict], ids_no_mapa: set[str]
) -> list[dict[str, Any]]:
    """Empresas da LISTAEMPRESAS da SUSEP candidatas de uma entidade (pura): por CNPJ raiz
    ja conhecido da entidade/conglomerado ou por `busca_nome_susep`. Sem a chave de busca por
    nome, so casa por CNPJ raiz."""
    ids = [cfg.get("cnpj_raiz"), cfg.get("cnpj"), entrada.get("cnpj"), *(entrada.get("cnpjs_relacionados") or [])]
    raizes = {r for r in (normalizar_identificador(i) for i in ids) if r and len(r) == 8 and r.isdigit()}
    termos = [t for t in (normalizar_nome(x) for x in (cfg.get("busca_nome_susep") or [])) if t]
    itens = []
    for linha in lista:
        cnpj = linha.get("cnpj") or None
        raiz = cnpj[:8] if cnpj else None
        por_id = bool(raiz and raiz in raizes)
        por_nome = any(t in normalizar_nome(linha.get("nome")) for t in termos)
        if not (por_id or por_nome):
            continue
        itens.append({
            "codigo_fip": linha["codigo_fip"], "cnpj": cnpj, "cnpj_raiz": raiz, "denominacao": linha.get("nome"),
            "origem": "+".join(o for o, ok in (("identificador", por_id), ("nome", por_nome)) if ok),
            "no_mapa": bool(raiz and raiz in ids_no_mapa), "aprovado": False,
        })
    return sorted(itens, key=lambda i: (i["denominacao"] or "", i["codigo_fip"]))


def _resolver_susep(
    settings: Settings, grupos_cfg: dict, grupos_res: dict, anterior: dict, lista: list[dict] | None
) -> dict[str, Any]:
    """Monta a secao `susep` do YAML resolvido. Em falha, preserva as sugestoes anteriores."""
    susep_anterior = (anterior or {}).get("susep") or {}
    erro: str | None = None
    if lista is None:
        client = SafraDadosClient(settings.http)
        try:
            conector = SusepSesConnector(settings, client=client)
            info = conector.ler_pagina()
            resposta = client.get(info["lista_url"])
            if resposta.status_code != 200:
                raise RuntimeError(f"LISTAEMPRESAS: HTTP {resposta.status_code}")
            lista = parse_lista_empresas(resposta.content).to_dicts()
        except (DownloadError, RuntimeError) as exc:
            erro = str(exc)
        finally:
            client.close()
    if erro is not None or lista is None:
        return {**susep_anterior, "status": "falha_api", "erro": erro, "tentado_em": now_iso()}

    ids_no_mapa = set(mapa_de_dicts(load_entidades(settings), {"grupos": grupos_res}).por_identificador)
    aprovados = {
        (g, e, i["codigo_fip"])
        for g, por_ent in (susep_anterior.get("sugestoes") or {}).items()
        for e, itens in (por_ent or {}).items()
        for i in itens or [] if i.get("aprovado")
    }
    sugestoes = {g: v for g, v in (susep_anterior.get("sugestoes") or {}).items() if g not in grupos_cfg}
    for nome_grupo, grupo_cfg in grupos_cfg.items():
        por_entidade = {}
        for nome, cfg in (grupo_cfg.get("entidades") or {}).items():
            itens = sugerir_susep(cfg or {}, grupos_res[nome_grupo]["entidades"][nome], lista, ids_no_mapa)
            for item in itens:
                item["aprovado"] = (nome_grupo, nome, item["codigo_fip"]) in aprovados
            por_entidade[nome] = itens
        sugestoes[nome_grupo] = por_entidade
    return {
        "gerado_em": now_iso(), "status": "ok", "erro": None,
        "fonte": "susep: LISTAEMPRESAS.csv (pagina do SES)", "sugestoes": sugestoes,
    }


def resolver_entidades(
    settings: Settings,
    grupo: str | None = None,
    anomes: str | None = None,
    cadastro: list[dict] | None = None,
    incluir_cvm: bool = True,
    cadastro_cvm: list[dict] | None = None,
    incluir_susep: bool = True,
    lista_susep: list[dict] | None = None,
) -> dict[str, Any]:
    """Resolve um grupo (ou todos, se `grupo` for None) e grava o YAML resolvido."""
    anomes = anomes or latest_available_anomes()
    erro_api: str | None = None
    if cadastro is None:
        client = SafraDadosClient(settings.http)
        try:
            connector = BcbIfDataConnector(settings, client=client)
            cadastro = connector.buscar_cadastro(anomes)
        except (DownloadError, RuntimeError) as exc:
            cadastro, erro_api = [], str(exc)
        finally:
            client.close()

    grupos_cfg = load_entidades(settings).get("grupos", {})
    if grupo is not None:
        if grupo not in grupos_cfg:
            raise KeyError(f"grupo '{grupo}' nao encontrado em config/entidades.yaml")
        grupos_cfg = {grupo: grupos_cfg[grupo]}

    anterior = (load_entidades_resolvidas(settings) or {}).get("grupos", {})
    grupos_res: dict[str, Any] = {g: v for g, v in anterior.items() if g not in grupos_cfg}
    for nome_grupo, grupo_cfg in grupos_cfg.items():
        entidades_res: dict[str, Any] = {}
        for nome, cfg in (grupo_cfg.get("entidades") or {}).items():
            entrada = resolver_entidade(cfg or {}, cadastro, erro_api)
            # aprovacao humana sobrevive a novas execucoes
            if ((anterior.get(nome_grupo) or {}).get("entidades") or {}).get(nome, {}).get("aprovado"):
                entrada["aprovado"] = True
            entidades_res[nome] = entrada
        grupos_res[nome_grupo] = {"entidades": entidades_res}

    cvm = (
        _resolver_cvm(settings, grupos_cfg, grupos_res, load_entidades_resolvidas(settings) or {}, cadastro_cvm)
        if incluir_cvm else ((load_entidades_resolvidas(settings) or {}).get("cvm"))
    )
    susep = (
        _resolver_susep(settings, grupos_cfg, grupos_res, load_entidades_resolvidas(settings) or {}, lista_susep)
        if incluir_susep else ((load_entidades_resolvidas(settings) or {}).get("susep"))
    )
    resultado = {
        "gerado_em": now_iso(),
        "anomes_consultado": anomes,
        "fonte": "bcb_ifdata.IfDataCadastro",
        "status_consulta": "falha_api" if erro_api else "ok",
        "erro_consulta": erro_api,
        "grupos": grupos_res,
    }
    if cvm is not None:
        resultado["cvm"] = cvm
    if susep is not None:
        resultado["susep"] = susep

    out_path = settings.entidades_resolvidas_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(resultado, fh, allow_unicode=True, sort_keys=False)
    return resultado
