"""CLI `safra-dados` (Typer): ingest, resolver-entidades, status."""

from __future__ import annotations

import contextlib
import json
import sys
from collections import defaultdict
from datetime import date, datetime
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
from .connectors.safra_ri import SafraRiConnector
from .connectors.susep_ses import SusepSesConnector
from .manifest import Checkpoint, load_checkpoint, read_entries, save_checkpoint, ultimas_por_url
from .calendario import desde_para_atualizar, vencida
from .gold import VIEWS_EXPORTAVEIS, _bi, _pct, conciliacao_fundos, conectar_gold, criar_gold, exportar_view, resumo_safra
from .importacao_manual import FONTE_RI, TIPOS_VALIDOS, importar_manual
from .reprocessar import reprocessar_fonte
from .resolver import resolver_entidades
from . import snapshot as snap

ULTIMA_ATUALIZACAO = "ultima_atualizacao.json"

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
    # Bloco 4
    "susep.ses": ("susep_ses", SusepSesConnector),
    "safra.ri": ("safra_ri", SafraRiConnector),
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
        ok = sum(1 for e in entradas_data_base if e.status in ("ok", "ja_existe", "importado_manual"))
        falha = sum(1 for e in entradas_data_base if e.status == "falha")
        resultado = connector.normalize(data_base)
        if resultado:
            checkpoint.marcar(data_base, arquivos_ok=ok, arquivos_falha=falha)
            typer.echo(f"[{fonte}] {data_base}: silver gravada em {resultado}")
        else:
            # resposta vazia (ex.: periodo ainda nao publicado) NAO vira "coletado": o atualizar tenta de novo
            typer.echo(f"[{fonte}] {data_base}: nenhum dado para normalizar (ok={ok}, falha={falha}); checkpoint nao atualizado")

    save_checkpoint(checkpoint_path, checkpoint)
    return falhas


@app.command()
def ingest(
    fonte: str = typer.Option(..., "--fonte", help="bcb.ifdata | bcb.cosif | bcb.juros | bcb.reclamacoes | bcb.tarifas | bcb.estban | bcb.unicad | bcb.sgs | cvm.cadastros | cvm.fundos_cad | cvm.fundos_informe | cvm.fundos_cda | cvm.ofertas | cvm.dfp | cvm.itr | cvm.fre | cvm.ipe | susep.ses | safra.ri | all"),
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
    sem_susep: bool = typer.Option(False, "--sem-susep", help="Nao consulta a LISTAEMPRESAS da SUSEP (sugestoes de FIP)"),
) -> None:
    """Busca no IfDataCadastro os CNPJs/codigos de conglomerado do grupo e grava
    config/entidades_resolvidas.yaml para revisao humana."""
    settings = load_settings()
    resultado = resolver_entidades(settings, grupo=grupo, anomes=anomes, incluir_cvm=not sem_cvm, incluir_susep=not sem_susep)
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
    susep = resultado.get("susep") or {}
    if susep:
        typer.echo(f"sugestoes SUSEP: {susep.get('status')}" + (f" ({susep['erro']})" if susep.get("erro") else ""))
        for nome_grupo, por_entidade in (susep.get("sugestoes") or {}).items():
            if grupo and nome_grupo != grupo:
                continue
            for nome, itens in por_entidade.items():
                if itens:
                    typer.echo(f"  [{nome_grupo}] {nome}: {len(itens)} empresa(s) SUSEP")
    typer.echo(f"gravado em {settings.entidades_resolvidas_path()}")


@app.command()
def reprocessar(
    fonte: str = typer.Option(..., "--fonte", help="fonte (ex.: susep.ses, cvm.fundos_cda) ou all"),
) -> None:
    """Refaz a silver a partir do bronze existente, SEM rede, com as entidades aprovadas atuais."""
    settings = load_settings()
    erros = 0
    for fonte_resolvida in _resolver_fontes(fonte):
        connector = _build_connector(settings, fonte_resolvida)
        resultados = reprocessar_fonte(settings, connector, fonte_resolvida)
        if not resultados:
            typer.echo(f"[{fonte_resolvida}] sem bronze para reprocessar.")
        for r in resultados:
            if r.erro:
                erros += 1
                typer.echo(f"[{fonte_resolvida}] {r.data_base}: FALHOU: {r.erro}", err=True)
            elif r.silver:
                typer.echo(f"[{fonte_resolvida}] {r.data_base}: silver regravada em {r.silver}")
            else:
                typer.echo(f"[{fonte_resolvida}] {r.data_base}: nada para normalizar")
    if erros:
        raise typer.Exit(code=1)


@app.command("importar-manual")
def importar_manual_cmd(
    fonte: str = typer.Option(FONTE_RI, "--fonte", help="so safra_ri"),
    tipo: str = typer.Option(None, "--tipo", help="para arquivos sem correspondencia: " + " | ".join(TIPOS_VALIDOS)),
    data_base: str = typer.Option(None, "--data-base", help="para arquivos sem correspondencia: AAAA-MM"),
) -> None:
    """Importa para a bronze os arquivos baixados pelo navegador em data/manual/<fonte>/ (sem rede)."""
    settings = load_settings()
    nome = {"safra_ri": "safra_ri", "safra.ri": "safra_ri"}.get(fonte)
    if nome is None:
        raise typer.BadParameter("importacao manual so existe para safra_ri")
    try:
        resultados = importar_manual(settings, nome, tipo=tipo, data_base=data_base)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    pasta = settings.manual_dir / nome
    if not resultados:
        typer.echo(f"nenhum arquivo em {pasta}")
        return
    pendentes = 0
    bases = set()
    for r in resultados:
        if r.estado == "pendente":
            pendentes += 1
            typer.echo(f"  PENDENTE {r.arquivo}: {r.detalhe}", err=True)
        else:
            bases.add(r.data_base)
            typer.echo(f"  {r.estado} {r.arquivo} -> {r.data_base} [{r.tipo}] ({r.detalhe})")
    connector = _build_connector(settings, nome)
    for db in sorted(bases):
        connector.normalize(db)
        typer.echo(f"[{nome}] {db}: silver atualizada")
    if pendentes:
        raise typer.Exit(code=1)


def _imprimir_gold(settings: Settings, resultado) -> None:
    typer.echo(f"gold: {resultado.caminho}")
    typer.echo(f"  views criadas: {', '.join(resultado.criadas) or '(nenhuma)'}")
    for nome, motivo in resultado.ignoradas.items():
        typer.echo(f"  ignorada {nome}: {motivo}")
    for nome, erro in resultado.erros.items():
        typer.echo(f"  ERRO em {nome}: {erro}", err=True)
    con = conectar_gold(settings)
    try:
        typer.echo("resumo do Safra (ultimo periodo de cada view):")
        for linha in resumo_safra(con):
            typer.echo("  " + linha)
        for c in conciliacao_fundos(con):
            if c["pl_dedup_visao"] is None:
                typer.echo(f"  [conciliacao] {c['id']}: sem PL no gold para {c['data_base']}")
                continue
            typer.echo(
                f"  [conciliacao] {c['id']} ({c['componentes']}; {c['data_base']}): referencia {_bi(c['valor_referencia'])}; "
                f"gold {_bi(c['valor_gold'])}; diferenca {_bi(c['diferenca'])} ({_pct(c['diferenca_pct'])}); "
                f"teto (so cotas identificadas) {_bi(c['valor_gold_teto'])} ({_pct(c['diferenca_teto_pct'])})"
            )
    finally:
        con.close()


@app.command()
def gold() -> None:
    """(Re)cria as views da camada gold (data/gold/safra.duckdb) e imprime o resumo do ultimo periodo do Safra."""
    settings = load_settings()
    resultado = criar_gold(settings)
    _imprimir_gold(settings, resultado)
    if resultado.erros:
        raise typer.Exit(code=1)


@app.command()
def exportar(
    view: str = typer.Option(..., "--view", help=" | ".join(VIEWS_EXPORTAVEIS)),
    formato: str = typer.Option("csv", "--formato", help="csv | xlsx"),
) -> None:
    """Exporta uma view gold para data/exports/<view>.<formato>."""
    settings = load_settings()
    try:
        destino = exportar_view(settings, view, formato)
    except (ValueError, FileNotFoundError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"exportado: {destino}")


class _Tee:
    """Escreve no terminal e no arquivo de log."""

    def __init__(self, *fluxos) -> None:
        self.fluxos = fluxos

    def write(self, texto: str) -> int:
        for f in self.fluxos:
            f.write(texto)
            f.flush()
        return len(texto)

    def flush(self) -> None:
        for f in self.fluxos:
            f.flush()


@app.command()
def atualizar(
    dry_run: bool = typer.Option(False, "--dry-run", help="So lista as fontes vencidas; nao coleta nem recria a gold"),
    sem_gold: bool = typer.Option(False, "--sem-gold", help="Nao recria a gold ao final"),
) -> None:
    """Roda so as fontes com publicacao vencida pelo calendario de cada conector, depois `gold`.
    Log em data/logs/atualizar_AAAAMMDD_HHMMSS.log."""
    settings = load_settings()
    hoje = date.today()
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = settings.logs_dir / f"atualizar_{datetime.now():%Y%m%d_%H%M%S}.log"
    falhas: list[str] = []
    with log_path.open("w", encoding="utf-8") as log:
        tee_out, tee_err = _Tee(sys.stdout, log), _Tee(sys.stderr, log)
        with contextlib.redirect_stdout(tee_out), contextlib.redirect_stderr(tee_err):
            typer.echo(f"atualizar em {datetime.now().isoformat(timespec='seconds')} (log: {log_path})")
            vencidas = []
            for fonte in _FONTE_ALIASES.values():
                checkpoint = load_checkpoint(settings.checkpoint_path(fonte), fonte)
                esta_vencida, motivo = vencida(fonte, checkpoint, hoje)
                typer.echo(f"  {'VENCIDA' if esta_vencida else 'em dia '} {fonte}: {motivo}")
                if esta_vencida:
                    vencidas.append((fonte, checkpoint))
            if dry_run:
                typer.echo(f"dry-run: {len(vencidas)} fonte(s) seriam atualizadas.")
                return
            atualizadas: list[str] = []
            for fonte, checkpoint in vencidas:
                desde = desde_para_atualizar(fonte, checkpoint, settings.fonte_config(fonte).get("desde", "2015-03"))
                try:
                    falhas += _ingest_fonte(settings, fonte, desde, None)
                    atualizadas.append(fonte)
                except Exception as exc:  # uma fonte com problema nao derruba as demais nem a gold
                    typer.echo(f"[{fonte}] FALHOU: {exc}", err=True)
                    falhas.append(f"[{fonte}] {exc}")
            if not sem_gold:
                try:
                    resultado = criar_gold(settings)
                    _imprimir_gold(settings, resultado)
                    falhas += [f"gold.{n}: {e}" for n, e in resultado.erros.items()]
                except Exception as exc:
                    typer.echo(f"gold FALHOU: {exc}", err=True)
                    falhas.append(f"gold: {exc}")
            typer.echo(f"fim: {len(vencidas)} fonte(s) atualizadas, {len(falhas)} falha(s).")
            # marcador lido por atualizar.ps1 para decidir se gera/publica o snapshot
            (settings.logs_dir / ULTIMA_ATUALIZACAO).write_text(json.dumps(
                {"em": datetime.now().isoformat(timespec="seconds"), "fontes_atualizadas": atualizadas,
                 "n_atualizadas": len(atualizadas), "n_falhas": len(falhas)}, ensure_ascii=False), encoding="utf-8")
    if falhas:
        raise typer.Exit(code=1)


@app.command("snapshot")
def snapshot_cmd(
    excluir: str = typer.Option("", "--excluir", help="outras relacoes silver a omitir, separadas por virgula"),
    completo: bool = typer.Option(False, "--completo", help=f"inclui tambem {', '.join(snap.SILVER_BRUTA_FUNDOS)} (~740 MB)"),
) -> None:
    """Gera data/snapshot/: gold materializada, silver filtrada (safra/pares) e manifest_snapshot.json (parquet zstd).
    Por padrao omite a silver bruta de fundos (cda, informe): a gold de fundos ja vai materializada e o Explorador,
    unico consumidor de silver, fica desabilitado na nuvem."""
    settings = load_settings()
    typer.echo("gerando snapshot (materializa as views de fundos; pode levar alguns minutos)...")
    omitir = tuple(x.strip() for x in excluir.split(",") if x.strip()) + (() if completo else snap.SILVER_BRUTA_FUNDOS)
    r = snap.gerar_snapshot(settings, excluir=omitir)
    typer.echo(f"snapshot em {r.pasta}: {len(r.arquivos)} arquivos, {r.total_bytes / 1e6:,.1f} MB (gerado em {r.manifest['gerado_em']})")
    for a in r.maiores(8):
        typer.echo(f"  {a['bytes'] / 1e6:10,.1f} MB  {a['linhas']:>12,} linhas  {a['caminho']}")
    if r.acima_do_limite:
        typer.echo(f"AVISO: total acima de {snap.AVISO_TOTAL_BYTES // 1024 // 1024} MB; maiores arquivos:", err=True)
        for a in r.maiores(5):
            typer.echo(f"  {a['bytes'] / 1e6:10,.1f} MB  {a['caminho']}", err=True)
    if r.manifest["erros_gold"]:
        typer.echo(f"views gold com erro: {r.manifest['erros_gold']}", err=True)


@app.command("publicar-snapshot")
def publicar_snapshot_cmd(
    repo: str = typer.Option(snap.REPO, "--repo", help="repositorio GitHub (dono/nome)"),
    tag: str = typer.Option(snap.TAG_RELEASE, "--tag", help="tag da release que guarda o asset"),
) -> None:
    """Compacta data/snapshot/ em snapshot.zip e publica como asset da release (gh release; exige `gh auth login`)."""
    settings = load_settings()
    pasta = settings.data_root / "snapshot"
    try:
        snap.validar_snapshot(pasta)
    except snap.SnapshotInvalido as exc:
        raise typer.BadParameter(f"snapshot ausente ou invalido ({exc}); rode `safra-dados snapshot`") from exc
    zip_path = snap.compactar_snapshot(pasta)
    typer.echo(f"{zip_path}: {zip_path.stat().st_size / 1e6:,.1f} MB; enviando para {repo}@{tag}...")
    try:
        url = snap.publicar_zip(zip_path, repo, tag)
    except RuntimeError as exc:
        typer.echo(f"ERRO: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"publicado: {url}")


@app.command("app")
def app_cmd(
    porta: int = typer.Option(None, "--porta", help="Porta do servidor (padrao do Streamlit: 8501)"),
    headless: bool = typer.Option(False, "--headless", help="Nao abre o navegador"),
) -> None:
    """Abre o app Streamlit local (somente leitura), equivalente a `streamlit run app/app.py`. Requer o extra `app`."""
    import importlib.util
    import os
    import subprocess

    if importlib.util.find_spec("streamlit") is None or importlib.util.find_spec("plotly") is None:
        raise typer.BadParameter('streamlit/plotly nao instalados; rode: pip install -e ".[app]"')
    settings = load_settings()
    raiz = settings.config_dir.parent
    entrada = raiz / "app" / "app.py"
    if not entrada.is_file():
        raise typer.BadParameter(f"{entrada} nao encontrado")
    cmd = [sys.executable, "-m", "streamlit", "run", str(entrada)]
    if porta:
        cmd += ["--server.port", str(porta)]
    if headless:
        cmd += ["--server.headless", "true"]
    env = {**os.environ, "SAFRA_DADOS_CONFIG_DIR": str(settings.config_dir)}
    raise typer.Exit(code=subprocess.call(cmd, cwd=raiz, env=env))


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
