"""Resolvedor de entidades: busca no IfDataCadastro (BCB Olinda IFDATA) todos
os CNPJs/codigos de conglomerado ligados a raiz configurada em
config/entidades.yaml, e grava o resultado em config/entidades_resolvidas.yaml
para revisao humana (nunca sobrescreve o arquivo de configuracao original)."""

from __future__ import annotations

from typing import Any

import yaml

from .config import Settings, load_entidades
from .connectors.bcb_ifdata import BcbIfDataConnector, latest_available_anomes
from .entidades import normalizar_nome
from .http import DownloadError, SafraDadosClient
from .manifest import now_iso


def _buscar_cadastro(connector: BcbIfDataConnector, client: SafraDadosClient, anomes: str) -> tuple[list[dict], str | None]:
    url = f"{connector.base_url}/IfDataCadastro(AnoMes=@AnoMes)"
    try:
        response = client.get(url, params={"@AnoMes": int(anomes), "$format": "json"})
    except DownloadError as exc:
        return [], str(exc)
    if response.status_code != 200:
        return [], f"HTTP {response.status_code}: {response.text[:300]}"
    return response.json().get("value", []), None


def resolver_entidades(
    settings: Settings, grupo: str = "safra", anomes: str | None = None
) -> dict[str, Any]:
    anomes = anomes or latest_available_anomes()
    client = SafraDadosClient(settings.http)
    try:
        connector = BcbIfDataConnector(settings, client=client)
        cadastro, erro_api = _buscar_cadastro(connector, client, anomes)
    finally:
        client.close()

    entidades_cfg = load_entidades(settings)
    grupo_cfg = entidades_cfg.get("grupos", {}).get(grupo)
    if grupo_cfg is None:
        raise KeyError(f"grupo '{grupo}' nao encontrado em config/entidades.yaml")

    resultado_entidades: dict[str, Any] = {}
    for nome_entidade, entidade_cfg in grupo_cfg.get("entidades", {}).items():
        termos = [normalizar_nome(t) for t in entidade_cfg.get("busca_nome", [])]
        hint = entidade_cfg.get("codigo_conglomerado_prudencial_hint")

        candidatos = []
        for row in cadastro:
            nome_norm = normalizar_nome(row.get("NomeInstituicao"))
            casa_por_nome = any(t and t in nome_norm for t in termos)
            casa_por_hint = hint and row.get("CodConglomeradoPrudencial") == hint
            if casa_por_nome or casa_por_hint:
                candidatos.append(
                    {
                        "cod_inst": row.get("CodInst"),
                        "nome_instituicao": row.get("NomeInstituicao"),
                        "codigo_conglomerado_prudencial": row.get("CodConglomeradoPrudencial") or None,
                        "codigo_conglomerado_financeiro": row.get("CodConglomeradoFinanceiro") or None,
                        "cnpj_instituicao_lider": row.get("CnpjInstituicaoLider") or None,
                        "situacao": row.get("Situacao"),
                    }
                )

        if erro_api:
            status = "falha_api"
        elif candidatos:
            status = "encontrado"
        else:
            status = "nao_encontrado"

        entrada: dict[str, Any] = {"status": status, "candidatos": candidatos}
        if erro_api:
            entrada["erro"] = erro_api
        if candidatos:
            principal = candidatos[0]
            cod_inst = principal["cod_inst"] or ""
            entrada["cnpj"] = cod_inst if len(cod_inst) == 8 else entidade_cfg.get("cnpj_raiz")
            entrada["codigo_conglomerado_prudencial"] = principal["codigo_conglomerado_prudencial"]
            entrada["codigo_conglomerado_financeiro"] = principal["codigo_conglomerado_financeiro"]
            entrada["cnpjs_relacionados"] = sorted(
                {c["cod_inst"] for c in candidatos if c["cod_inst"] and len(c["cod_inst"]) == 8}
            )
        resultado_entidades[nome_entidade] = entrada

    resultado = {
        "gerado_em": now_iso(),
        "anomes_consultado": anomes,
        "fonte": "bcb_ifdata.IfDataCadastro",
        "status_consulta": "ok" if not erro_api else "falha_api",
        "erro_consulta": erro_api,
        "grupos": {grupo: {"entidades": resultado_entidades}},
    }

    out_path = settings.entidades_resolvidas_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(resultado, fh, allow_unicode=True, sort_keys=False)

    return resultado
