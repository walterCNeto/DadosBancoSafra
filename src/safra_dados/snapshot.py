"""Snapshot publicavel dos dados (Bloco 7): gera `data/snapshot/` (gold materializada + silver filtrada, em parquet
zstd, com `manifest_snapshot.json`), publica como asset de release do GitHub e, no lado do app em nuvem, baixa,
valida (sha256) e registra as views em DuckDB em memoria. Nao depende de streamlit; testavel sem rede.

Layout do snapshot::

    manifest_snapshot.json
    gold/<relacao>.parquet       # views/tabelas do schema gold (fundos* ja materializadas)
    silver/<relacao>.parquet     # silver: grupo in ('safra','pares') ou a tabela completa se for pequena/sem grupo
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .app_core import _silver_extras, criar_conexao_memoria, relacoes_disponiveis, status_fontes
from .config import Settings
from .gold import SILVER_VIEWS, ResultadoGold, _glob_silver
from .manifest import load_checkpoint

REPO = "walterCNeto/DadosBancoSafra"
TAG_RELEASE = "dados-latest"
NOME_ASSET = "snapshot.zip"
URL_PADRAO = f"https://github.com/{REPO}/releases/download/{TAG_RELEASE}/{NOME_ASSET}"
ENV_URL = "SAFRA_SNAPSHOT_URL"
MANIFEST = "manifest_snapshot.json"
VERSAO_MANIFEST = 1
GRUPOS_SNAPSHOT = ("safra", "pares")
LIMITE_TABELA_PEQUENA = 100_000  # linhas: abaixo disso a tabela silver vai completa, mesmo tendo coluna `grupo`
# silver bruta de fundos (~740 MB filtrada): a CLI a omite por padrao, pois gold.fundos* ja vai materializada
SILVER_BRUTA_FUNDOS = ("cda", "informe")
AVISO_TOTAL_BYTES = 100 * 1024 * 1024


class SnapshotInvalido(RuntimeError):
    """Manifest ausente/corrompido, arquivo faltando ou sha256 divergente."""


@dataclass
class ResultadoSnapshot:
    pasta: Path
    manifest: dict[str, Any]
    total_bytes: int
    arquivos: list[dict[str, Any]] = field(default_factory=list)

    @property
    def acima_do_limite(self) -> bool:
        return self.total_bytes > AVISO_TOTAL_BYTES

    def maiores(self, n: int = 5) -> list[dict[str, Any]]:
        return sorted(self.arquivos, key=lambda a: a["bytes"], reverse=True)[:n]


# ---------------------------------------------------------------- utilitarios

def sha256_arquivo(caminho: Path) -> str:
    h = hashlib.sha256()
    with caminho.open("rb") as fh:
        for bloco in iter(lambda: fh.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def _sql_str(caminho: Path | str) -> str:
    return str(caminho).replace("\\", "/").replace("'", "''")


def _copiar_parquet(con: duckdb.DuckDBPyConnection, consulta: str, destino: Path) -> int:
    destino.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy ({consulta}) to '{_sql_str(destino)}' (format parquet, compression zstd)")
    return con.execute(f"select count(*) from read_parquet('{_sql_str(destino)}')").fetchone()[0]


def _fontes_silver(settings: Settings) -> dict[str, str]:
    """nome da relacao silver -> glob (as de `SILVER_VIEWS` mais as fontes extras, como o app)."""
    saida = {nome: _glob_silver(settings, padrao) for nome, padrao in SILVER_VIEWS.items()}
    saida.update(_silver_extras(settings))
    return saida


def _data_bases(settings: Settings) -> dict[str, str | None]:
    saida: dict[str, str | None] = {}
    if settings.bronze_dir.is_dir():
        for d in sorted(p for p in settings.bronze_dir.iterdir() if p.is_dir()):
            saida[d.name] = load_checkpoint(settings.checkpoint_path(d.name), d.name).ultima_data_base_ok
    return saida


# ---------------------------------------------------------------- geracao

def gerar_snapshot(settings: Settings, destino: Path | None = None, excluir: tuple[str, ...] = ()) -> ResultadoSnapshot:
    """Gera `data/snapshot/` do zero (apaga o conteudo anterior). `excluir`: relacoes silver a omitir."""
    pasta = destino or (settings.data_root / "snapshot")
    if pasta.exists():
        shutil.rmtree(pasta)
    pasta.mkdir(parents=True)
    arquivos: list[dict[str, Any]] = []

    def registrar(tipo: str, nome: str, caminho: Path, linhas: int, **extra: Any) -> None:
        arquivos.append({"caminho": caminho.relative_to(pasta).as_posix(), "tipo": tipo, "relacao": nome,
                         "linhas": linhas, "bytes": caminho.stat().st_size, "sha256": sha256_arquivo(caminho), **extra})

    con, res = criar_conexao_memoria(settings)  # materializa fundos, fundos_decomposicao e fundos_conciliacao
    try:
        for nome in relacoes_disponiveis(con)["gold"]:
            destino_g = pasta / "gold" / f"{nome}.parquet"
            registrar("gold", nome, destino_g, _copiar_parquet(con, f'select * from gold."{nome}"', destino_g))
        for nome, alvo in _fontes_silver(settings).items():
            if nome in excluir:
                continue
            leitura = f"read_parquet('{_sql_str(alvo)}', union_by_name = true)"
            try:
                colunas = {r[0] for r in con.execute(f"describe select * from {leitura}").fetchall()}
            except duckdb.Error:
                continue  # fonte silver ainda inexistente
            total = con.execute(f"select count(*) from {leitura}").fetchone()[0]
            filtrar = "grupo" in colunas and total > LIMITE_TABELA_PEQUENA
            consulta = f"select * from {leitura}" + (
                " where grupo in (" + ", ".join(f"'{g}'" for g in GRUPOS_SNAPSHOT) + ")" if filtrar else "")
            destino_s = pasta / "silver" / f"{nome}.parquet"
            registrar("silver", nome, destino_s, _copiar_parquet(con, consulta, destino_s),
                      filtrado=filtrar, linhas_origem=total)
    finally:
        con.close()

    fontes = status_fontes(settings)
    manifest = {
        "versao": VERSAO_MANIFEST,
        "gerado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "grupos_silver": list(GRUPOS_SNAPSHOT),
        "data_base_fontes": _data_bases(settings),
        "fontes": json.loads(json.dumps(fontes, default=str)),  # status por fonte (a nuvem nao tem bronze/checkpoints)
        "total_bytes": sum(a["bytes"] for a in arquivos),
        "arquivos": arquivos,
        "erros_gold": res.erros,
    }
    (pasta / MANIFEST).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return ResultadoSnapshot(pasta, manifest, manifest["total_bytes"], arquivos)


def compactar_snapshot(pasta: Path, zip_path: Path | None = None) -> Path:
    """`snapshot.zip` com o conteudo de `pasta`. Parquet zstd nao comprime mais: arquivos entram sem recompressao."""
    zip_path = zip_path or pasta.parent / NOME_ASSET
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED, allowZip64=True) as z:
        z.write(pasta / MANIFEST, MANIFEST)
        for f in sorted(p for p in pasta.rglob("*") if p.is_file() and p.name != MANIFEST):
            z.write(f, f.relative_to(pasta).as_posix())
    return zip_path


# ---------------------------------------------------------------- publicacao (gh release)

def _gh(args: list[str], *, capturar: bool = True) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["gh", *args], capture_output=capturar, text=True, encoding="utf-8")
    except FileNotFoundError as exc:
        raise RuntimeError("GitHub CLI (gh) nao encontrado no PATH; instale (winget install GitHub.cli) e rode `gh auth login`") from exc


def publicar_zip(zip_path: Path, repo: str = REPO, tag: str = TAG_RELEASE) -> str:
    """Cria a release `tag` se nao existir e envia/substitui o asset (`--clobber`). Devolve a URL publica do asset."""
    if _gh(["auth", "status"]).returncode != 0:
        raise RuntimeError("gh nao autenticado: rode `gh auth login`")
    if _gh(["release", "view", tag, "--repo", repo]).returncode != 0:
        criar = _gh(["release", "create", tag, "--repo", repo, "--title", "Dados (snapshot mais recente)",
                     "--notes", "Snapshot dos dados publicos usado pelo app Streamlit. Atualizado por `safra-dados publicar-snapshot`.",
                     "--latest=false"])
        if criar.returncode != 0:
            raise RuntimeError(f"falha ao criar a release {tag}: {criar.stderr.strip()}")
    envio = _gh(["release", "upload", tag, str(zip_path), "--repo", repo, "--clobber"])
    if envio.returncode != 0:
        raise RuntimeError(f"falha ao enviar o asset: {envio.stderr.strip()}")
    return f"https://github.com/{repo}/releases/download/{tag}/{zip_path.name}"


# ---------------------------------------------------------------- lado do app: baixar, validar, registrar

def url_do_snapshot(secrets: Any = None) -> str:
    """SAFRA_SNAPSHOT_URL (variavel de ambiente) > st.secrets > URL do asset da release. Aceita http(s):// e file://."""
    url = os.environ.get(ENV_URL)
    if not url and secrets is not None:
        try:
            url = secrets.get(ENV_URL)
        except Exception:  # sem secrets.toml
            url = None
    return str(url or URL_PADRAO)


def baixar(url: str, destino: Path) -> Path:
    req = urllib.request.Request(url, headers={"User-Agent": "safra-dados-app/0.1"})
    with urllib.request.urlopen(req, timeout=120) as resp, destino.open("wb") as out:
        shutil.copyfileobj(resp, out, 1 << 20)
    return destino


def ler_manifest(pasta: Path) -> dict[str, Any]:
    caminho = pasta / MANIFEST
    if not caminho.is_file():
        raise SnapshotInvalido(f"{MANIFEST} ausente")
    try:
        manifest = json.loads(caminho.read_text(encoding="utf-8"))
        manifest["arquivos"], manifest["gerado_em"]
    except (ValueError, KeyError) as exc:
        raise SnapshotInvalido(f"{MANIFEST} invalido: {exc}") from exc
    return manifest


def validar_snapshot(pasta: Path) -> dict[str, Any]:
    """Confere tamanho e sha256 de cada arquivo do manifest. Devolve o manifest ou levanta `SnapshotInvalido`."""
    manifest = ler_manifest(pasta)
    raiz = pasta.resolve()
    for a in manifest["arquivos"]:
        f = (pasta / a["caminho"]).resolve()
        if raiz not in f.parents:
            raise SnapshotInvalido(f"caminho fora do snapshot: {a['caminho']}")
        if not f.is_file():
            raise SnapshotInvalido(f"arquivo ausente: {a['caminho']}")
        if f.stat().st_size != a["bytes"]:
            raise SnapshotInvalido(f"tamanho divergente: {a['caminho']}")
        if sha256_arquivo(f) != a["sha256"]:
            raise SnapshotInvalido(f"sha256 divergente: {a['caminho']}")
    return manifest


def baixar_e_extrair(url: str, base: Path | None = None) -> tuple[Path, dict[str, Any]]:
    """Baixa o zip, extrai em diretorio temporario NOVO e valida pelo manifest. Remove tudo se invalido."""
    pasta = Path(tempfile.mkdtemp(prefix="safra_snapshot_", dir=base))
    try:
        zip_path = baixar(url, pasta / NOME_ASSET)
        dados = pasta / "dados"
        with zipfile.ZipFile(zip_path) as z:
            for nome in z.namelist():
                destino = (dados / nome).resolve()
                if dados.resolve() not in destino.parents:
                    raise SnapshotInvalido(f"entrada insegura no zip: {nome}")
            z.extractall(dados)
        zip_path.unlink()
        return dados, validar_snapshot(dados)
    except Exception:
        shutil.rmtree(pasta, ignore_errors=True)
        raise


def registrar_views(pasta: Path, manifest: dict[str, Any] | None = None) -> tuple[duckdb.DuckDBPyConnection, ResultadoGold]:
    """DuckDB em memoria com `gold.*` e `silver.*` como views sobre os parquets do snapshot (nada e recalculado)."""
    manifest = manifest or ler_manifest(pasta)
    con = duckdb.connect(":memory:")
    con.execute("create schema silver")
    con.execute("create schema gold")
    res = ResultadoGold(None)
    for a in manifest["arquivos"]:
        alvo = _sql_str(pasta / a["caminho"])
        nome = a["relacao"].replace('"', '""')
        con.execute(f'create view {a["tipo"]}."{nome}" as select * from read_parquet(\'{alvo}\')')
        (res.criadas if a["tipo"] == "gold" else res.silver_views).append(a["relacao"])
    res.erros.update({str(k): str(v) for k, v in (manifest.get("erros_gold") or {}).items()})
    return con, res


def carregar_snapshot(url: str) -> tuple[duckdb.DuckDBPyConnection, ResultadoGold, dict[str, Any]]:
    pasta, manifest = baixar_e_extrair(url)
    con, res = registrar_views(pasta, manifest)
    return con, res, manifest
