"""Helper unico para montar URLs do BCB Olinda (OData).

Motivo: o Olinda responde HTTP 500 quando as chaves da query vem codificadas
(`%40AnoMes`, `%24format`), que e o que o httpx faz com `params=`. A query
string precisa ter "@" e "$" literais nas chaves. Por isso TODA chamada Olinda
do projeto monta a URL completa aqui e a passa ao httpx sem `params=`.

Exemplo:

    olinda_url(BASE, "IfDataCadastro", {"AnoMes": 202606}, top=5)
    -> BASE/IfDataCadastro(AnoMes=@AnoMes)?@AnoMes=202606&$top=5&$format=json
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote


def _valor_odata(valor: Any) -> str:
    """Literal OData de um argumento: str vira 'texto' (aspas simples duplicadas),
    numeros e bool ficam sem aspas."""
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (int, float)):
        return str(valor)
    texto = str(valor).replace("'", "''")
    return f"'{texto}'"


def _enc(valor: str) -> str:
    return quote(valor, safe="'")


def olinda_url(
    base_url: str,
    recurso: str,
    args: dict[str, Any] | None = None,
    *,
    filtro: str | None = None,
    select: str | None = None,
    orderby: str | None = None,
    top: int | None = None,
    skip: int | None = None,
    formato: str | None = "json",
) -> str:
    """Monta a URL completa de um recurso Olinda.

    - `recurso`: nome da funcao/entidade. Com `args`, vira `recurso(A=@A,B=@B)`
      e cada argumento e enviado na query como `@A=valor` (chaves literais).
      Sem `args`, o nome e usado como veio (ex.: "ListaDeRelatorio()").
    - Valores: `urllib.parse.quote(v, safe="'")`; strings ganham aspas simples.
    - `filtro/select/orderby/top/skip/formato` viram `$filter/$select/...`.
    """
    caminho = f"{base_url.rstrip('/')}/{recurso}"
    partes: list[str] = []
    if args:
        caminho += "(" + ",".join(f"{nome}=@{nome}" for nome in args) + ")"
        partes.extend(f"@{nome}={_enc(_valor_odata(v))}" for nome, v in args.items())
    if top is not None:
        partes.append(f"$top={int(top)}")
    if skip is not None:
        partes.append(f"$skip={int(skip)}")
    if filtro:
        partes.append(f"$filter={_enc(filtro)}")
    if select:
        partes.append(f"$select={_enc(select)}")
    if orderby:
        partes.append(f"$orderby={_enc(orderby)}")
    if formato:
        partes.append(f"$format={formato}")
    return caminho + ("?" + "&".join(partes) if partes else "")


def com_paginacao(url: str, top: int, skip: int) -> str:
    """Acrescenta `$top`/`$skip` literais a uma URL Olinda ja montada."""
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}$top={int(top)}&$skip={int(skip)}"


def juntar_paginas(client: Any, url: str, pagina: int = 50_000) -> bytes:
    """GET `url`; se a resposta vier truncada (traz `@odata.nextLink`), refaz a
    busca paginando com `$top/$skip` e devolve um unico JSON `{"value": [...]}`.

    Levanta DownloadError (via client) em falha de rede e RuntimeError com
    "HTTP <status>" para respostas nao-200.
    """
    resposta = client.get(url)
    if resposta.status_code != 200:
        raise RuntimeError(f"HTTP {resposta.status_code}: {resposta.text[:300]}")
    corpo = resposta.json()
    if "@odata.nextLink" not in corpo:
        return resposta.content

    linhas: list[Any] = []
    skip = 0
    while True:
        resp = client.get(com_paginacao(url, pagina, skip))
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} (skip={skip}): {resp.text[:300]}")
        valores = resp.json().get("value", [])
        linhas.extend(valores)
        if len(valores) < pagina:
            break
        skip += pagina
    return json.dumps({"value": linhas}, ensure_ascii=False).encode("utf-8")
