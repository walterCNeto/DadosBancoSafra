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
"""

from __future__ import annotations

from typing import Any

import yaml

from .config import Settings, load_entidades, load_entidades_resolvidas
from .connectors.bcb_ifdata import BcbIfDataConnector, latest_available_anomes
from .entidades import normalizar_identificador, normalizar_nome
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


def resolver_entidades(
    settings: Settings,
    grupo: str | None = None,
    anomes: str | None = None,
    cadastro: list[dict] | None = None,
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

    resultado = {
        "gerado_em": now_iso(),
        "anomes_consultado": anomes,
        "fonte": "bcb_ifdata.IfDataCadastro",
        "status_consulta": "falha_api" if erro_api else "ok",
        "erro_consulta": erro_api,
        "grupos": grupos_res,
    }

    out_path = settings.entidades_resolvidas_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(resultado, fh, allow_unicode=True, sort_keys=False)
    return resultado
