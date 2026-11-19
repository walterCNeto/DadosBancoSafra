"""Avaliador de robots.txt (RFC 9309) usado pelos conectores de sites (SUSEP, RI Safra).

Nao usa `urllib.robotparser`: ele aplica a PRIMEIRA regra que casa, e o robots.txt do
site da Safra comeca com `Allow: /` seguido de `Disallow: /data/`, o que liberaria tudo.
Aqui vale a regra do RFC: a diretiva mais especifica (padrao mais longo) vence; empate
favorece `Allow`; sem regra aplicavel, o acesso e permitido. Suporta `*` e `$`.

Politica de falha (RFC 9309): robots.txt ausente (4xx) = tudo permitido; erro de servidor
ou de rede = tudo proibido (conservador).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from .http import DownloadError, SafraDadosClient


def _padrao_para_regex(padrao: str) -> re.Pattern[str]:
    ancorado = padrao.endswith("$")
    corpo = padrao[:-1] if ancorado else padrao
    regex = "".join(".*" if c == "*" else re.escape(c) for c in corpo)
    return re.compile(regex + ("$" if ancorado else ""))


@dataclass
class RegrasRobots:
    # (permite?, padrao) so dos grupos que se aplicam ao nosso agente
    regras: list[tuple[bool, str]] = field(default_factory=list)
    tudo_proibido: bool = False
    origem: str = ""

    @classmethod
    def de_texto(cls, texto: str, agente: str = "safra-dados", origem: str = "") -> "RegrasRobots":
        agente = agente.lower()
        grupos: list[tuple[list[str], list[tuple[bool, str]]]] = []
        agentes: list[str] = []
        regras: list[tuple[bool, str]] = []
        lendo_regras = False
        for bruta in texto.splitlines():
            linha = bruta.split("#", 1)[0].strip()
            if ":" not in linha:
                continue
            campo, valor = (p.strip() for p in linha.split(":", 1))
            campo = campo.lower()
            if campo == "user-agent":
                if lendo_regras:  # novo grupo
                    grupos.append((agentes, regras))
                    agentes, regras, lendo_regras = [], [], False
                agentes.append(valor.lower())
            elif campo in ("allow", "disallow"):
                lendo_regras = True
                if valor:  # `Disallow:` vazio = nada proibido
                    regras.append((campo == "allow", valor))
        if agentes or regras:
            grupos.append((agentes, regras))

        especificos = [r for ags, r in grupos if any(a != "*" and a in agente for a in ags)]
        escolhidos = especificos or [r for ags, r in grupos if "*" in ags]
        return cls([regra for r in escolhidos for regra in r], origem=origem)

    @classmethod
    def liberado(cls, origem: str = "") -> "RegrasRobots":
        return cls([], origem=origem)

    @classmethod
    def bloqueado(cls, origem: str = "") -> "RegrasRobots":
        return cls([], tudo_proibido=True, origem=origem)

    def permitido(self, url_ou_caminho: str) -> bool:
        if self.tudo_proibido:
            return False
        partes = urlsplit(url_ou_caminho)
        caminho = partes.path or "/"
        if partes.query:
            caminho += "?" + partes.query
        melhor: tuple[int, bool] | None = None
        for permite, padrao in self.regras:
            if _padrao_para_regex(padrao).match(caminho):
                candidato = (len(padrao), permite)
                if melhor is None or candidato[0] > melhor[0] or (candidato[0] == melhor[0] and permite):
                    melhor = candidato
        return True if melhor is None else melhor[1]


def carregar_robots(client: SafraDadosClient, url_qualquer: str, agente: str = "safra-dados") -> RegrasRobots:
    """Baixa e interpreta o robots.txt do host de `url_qualquer`."""
    partes = urlsplit(url_qualquer)
    url = f"{partes.scheme}://{partes.netloc}/robots.txt"
    try:
        resposta = client.get(url)
    except DownloadError:
        return RegrasRobots.bloqueado(url)
    if resposta.status_code == 200:
        return RegrasRobots.de_texto(resposta.text, agente, origem=url)
    if 400 <= resposta.status_code < 500:
        return RegrasRobots.liberado(url)
    return RegrasRobots.bloqueado(url)
