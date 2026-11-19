"""CLI `safra-dados` (Typer): ingest, resolver-entidades, status."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import typer

from .config import Settings, load_settings
from .connectors.base import Connector
from .connectors.bcb_cosif_balancetes import BcbCosifBalancetesConnector
from .connectors.bcb_estban import BcbEstbanConnector
from .connectors.bcb_ifdata import BcbIfDataConnector
from .connectors.bcb_reclamacoes import BcbReclamacoesConnector
from .connectors.bcb_sgs import BcbSgsConnector
from .connectors.bcb_tarifas import BcbTarifasConnector
from .connectors.bcb_taxas_juros import BcbTaxasJurosConnector
from .connectors.bcb_unicad import BcbUnicadConnector
from .connectors.cvm_cadastros import CvmCadastrosConnector
from .connectors.cvm_cia_aberta import CvmDfpConnector, CvmFreConnector, CvmIpeConnector, CvmItrConnector
from .connectors.cvm_fundos import CvmFundosCadConnector, CvmFundosCdaConnector, CvmFundosInformeConnector
from .connectors.cvm_ofertas import CvmOfertasConnector
from .manifest import Checkpoint, load_checkpoint, read_entries, save_checkpoint, ultimas_por_url
from .resolver import resolver_entidades

app = typer.Typer(help="Coleta de dados publicos (BCB, CVM, SUSEP) sobre o grupo Banco Safra e pares.")

# alias da CLI -> (nome da fonte, classe do conector)
_FONTES: dict[str, tuple[str, type[Connector]]] = {
    "bcb.ifdata": ("bcb_ifdata", BcbIfDataConnector),
    "bcb.cosif": ("bcb_cosif", BcbCosifBalancetesConnector),
    "bcb.juros": ("bcb_taxas_juros", BcbTaxasJurosConnector),
    "bcb.reclamacoes": ("bcb_reclamacoes", BcbReclamacoesConnector),
    "bcb.tarifas": ("bcb_tarifas", BcbTarifasConnector),
    "bcb.estban": ("bcb_estban", BcbEstbanConnector),
    "bcb.unicad": ("bcb_unicad", BcbUnicadConnector),
    "bcb.sgs": ("bcb_sgs", BcbSgsConnector),
    # Bloco 3 (CVM). A ordem importa para `--fonte all`: o cadastro de fundos vem antes
    # do informe diario e do CDA, que filtram pelos fundos do grupo.
    "cvm.cadastros": ("cvm_cadastros", CvmCadastrosConnector),
    "cvm.fundos_cad": ("cvm_fundos_cad", CvmFundosCadConnector),
    "cvm.fundos_informe": ("cvm_fundos_informe", CvmFundosInformeConnector),
    "cvm.fundos_cda": ("cvm_fundos_cda", CvmFundosCdaConnector),
    "cvm.ofertas": ("cvm_ofertas", CvmOfertasConnector),
    "cvm.dfp": ("cvm_dfp", CvmDfpConnector),
    "cvm.itr": ("cvm_itr", CvmItrConnector),
    "cvm.fre": ("cvm_fre", CvmFreConnector),
    "cvm.ipe": ("cvm_ipe", CvmIpeConnector),
}
_FONTE_ALIASES = {alias: nome for alias, (nome, _) in _FONTES.items()}
_CONECTORES = {nome: cls for nome, cls in _FONTES.values()}


def _build_connector(settings: Settings, fonte: str) -> Connector:
    try:
        return _CONECTORES[fonte](settings)
    except KeyError:
        raise ValueError(f"fonte desconhecida: {fonte}") from None


def _resolver_fontes(fonte: str) -> list[str]:
    if fonte == "all":
        return list(_FONTE_ALIASES.values())
    if fonte in _FONTE_ALIASES:
        return [_FONTE_ALIASES[fonte]]
    raise typer.BadParameter(
        f"fonte invalida: {fonte!r}. Use um de: {', '.join(_FONTE_ALIASES)}, all"
    )


def _ingest_fonte(settings: Settings, fonte: str, desde: str, ate: str | None) -> list[str]:
    """Ingere uma fonte e devolve as falhas de descoberta (ex.: CKAN fora do ar)."""
    connector = _build_connector(settings, fonte)
    typer.echo(f"[{fonte}] descobrindo recursos desde {desde}...")
    recursos = connector.discover(desde, ate)
    falhas = [f"[{fonte}] {f}" for f in getattr(connector, "falhas_discover", [])]
    for falha in falhas:
        typer.echo(f"{falha}", err=True)
    if not recursos:
        typer.echo(f"[{fonte}] nenhum recurso encontrado.")
        return falhas

    por_data_base: dict[str, list] = defaultdict(list)
    for recurso in recursos:
        por_data_base[recurso.data_base].append(recurso)

    checkpoint_path = settings.checkpoint_path(fonte)
    checkpoint = load_checkpoint(checkpoint_path, fonte)

    for data_base in sorted(por_data_base):
        recursos_db = por_data_base[data_base]
        typer.echo(f"[{fonte}] {data_base}: baixando {len(recursos_db)} recurso(s)...")
        for recurso in recursos_db:
            connector.fetch(recurso)

        manifest_path = settings.manifest_path(fonte, data_base)
        entradas = read_entries(manifest_path)
        urls_atuais = {r.url for r in recursos_db}  # ignora entradas de URLs antigas/obsoletas
        entradas_data_base = ultimas_por_url([e for e in entradas if e.url in urls_atuais])
        ok = sum(1 for e in entradas_data_base if e.status in ("ok", "ja_existe"))
        falha = sum(1 for e in entradas_data_base if e.status == "falha")
        checkpoint.marcar(data_base, arquivos_ok=ok, arquivos_falha=falha)

        resultado = connector.normalize(data_base)
        if resultado:
            typer.echo(f"[{fonte}] {data_base}: silver gravada em {resultado}")
        else:
            typer.echo(f"[{fonte}] {data_base}: nenhum dado para normalizar (ok={ok}, falha={falha})")

    save_checkpoint(checkpoint_path, checkpoint)
    return falhas


@app.command()
def ingest(
    fonte: str = typer.Option(..., "--fonte", help="bcb.ifdata | bcb.cosif | bcb.juros | bcb.reclamacoes | bcb.tarifas | bcb.estban | bcb.unicad | bcb.sgs | cvm.cadastros | cvm.fundos_cad | cvm.fundos_informe | cvm.fundos_cda | cvm.ofertas | cvm.dfp | cvm.itr | cvm.fre | cvm.ipe | all"),
    desde: str = typer.Option(None, "--desde", help="Data-base inicial, formato AAAA-MM (padrao: config)"),
    ate: str = typer.Option(None, "--ate", help="Data-base final, formato AAAA-MM (padrao: ultima disponivel)"),
) -> None:
    """Descobre e baixa os recursos de uma ou mais fontes, e normaliza a silver."""
    settings = load_settings()
    falhas: list[str] = []
    for fonte_resolvida in _resolver_fontes(fonte):
        desde_fonte = desde or settings.fonte_config(fonte_resolvida).get("desde", "2015-03")
        try:
            falhas += _ingest_fonte(settings, fonte_resolvida, desde_fonte, ate)
        except (RuntimeError, ValueError) as exc:  # uma fonte com problema nao derruba as demais
            typer.echo(f"[{fonte_resolvida}] FALHOU: {exc}", err=True)
            falhas.append(f"[{fonte_resolvida}] {exc}")
    if falhas:
        typer.echo(f"{len(falhas)} falha(s) na ingestao:", err=True)
        raise typer.Exit(code=1)


@app.command("resolver-entidades")
def resolver_entidades_cmd(
    grupo: str = typer.Option(None, "--grupo", help="Grupo de config/entidades.yaml (padrao: todos)"),
    anomes: str = typer.Option(None, "--anomes", help="Data-base AAAAMM para consulta (padrao: ultima disponivel)"),
    sem_cvm: bool = typer.Option(False, "--sem-cvm", help="Nao consulta os cadastros da CVM (sugestoes de CNPJ CVM)"),
) -> None:
    """Busca no IfDataCadastro os CNPJs/codigos de conglomerado do grupo e grava
    config/entidades_resolvidas.yaml para revisao humana."""
    settings = load_settings()
    resultado = resolver_entidades(settings, grupo=grupo, anomes=anomes, incluir_cvm=not sem_cvm)
    typer.echo(f"status da consulta: {resultado['status_consulta']}")
    if resultado.get("erro_consulta"):
        typer.echo(f"erro: {resultado['erro_consulta']}")
    for nome_grupo, dados in resultado["grupos"].items():
        if grupo and nome_grupo != grupo:
            continue
        for nome, entrada in dados["entidades"].items():
            typer.echo(
                f"  [{nome_grupo}] {nome}: {entrada['status']} (cnpj={entrada.get('cnpj')!r}, "
                f"cong_prudencial={entrada.get('codigo_conglomerado_prudencial')!r})"
            )
    cvm = resultado.get("cvm") or {}
    if cvm:
        typer.echo(f"sugestoes CVM: {cvm.get('status')}" + (f" ({cvm['erro']})" if cvm.get("erro") else ""))
        for nome_grupo, por_entidade in (cvm.get("sugestoes") or {}).items():
            if grupo and nome_grupo != grupo:
                continue
            for nome, itens in por_entidade.items():
                typer.echo(f"  [{nome_grupo}] {nome}: {len(itens)} registro(s) CVM ({sum(1 for i in itens if i['ativo'])} ativos)")
    typer.echo(f"gravado em {settings.entidades_resolvidas_path()}")


@app.command()
def status() -> None:
    """Mostra, por fonte, a ultima data-base processada, numero de arquivos e falhas."""
    settings = load_settings()
    for fonte in _FONTE_ALIASES.values():
        checkpoint_path = settings.checkpoint_path(fonte)
        checkpoint = load_checkpoint(checkpoint_path, fonte)
        typer.echo(f"== {fonte} ==")
        if not checkpoint.data_bases:
            typer.echo("  (sem dados coletados ainda)")
            continue
        typer.echo(f"  ultima data-base ok: {checkpoint.ultima_data_base_ok}")
        for data_base in sorted(checkpoint.data_bases):
            info = checkpoint.data_bases[data_base]
            typer.echo(
                f"  {data_base}: arquivos_ok={info['arquivos_ok']} "
                f"arquivos_falha={info['arquivos_falha']} atualizado_em={info['atualizado_em']}"
            )


if __name__ == "__main__":
    app()
