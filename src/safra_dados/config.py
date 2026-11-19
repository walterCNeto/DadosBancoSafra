"""Carregamento de configuracao (config/settings.yaml, config/entidades*.yaml) e
resolucao da raiz dos dados (bronze/silver/gold)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ENV_DATA_HOME = "SAFRA_DADOS_HOME"
ENV_CONFIG_DIR = "SAFRA_DADOS_CONFIG_DIR"


def find_config_dir(start: Path | None = None) -> Path:
    """Localiza o diretorio `config/` subindo a partir de `start` (ou do diretorio
    de trabalho atual). Pode ser sobrescrito pela variavel SAFRA_DADOS_CONFIG_DIR."""
    env_dir = os.environ.get(ENV_CONFIG_DIR)
    if env_dir:
        path = Path(env_dir).expanduser().resolve()
        if not path.is_dir():
            raise FileNotFoundError(
                f"SAFRA_DADOS_CONFIG_DIR aponta para um diretorio inexistente: {path}"
            )
        return path

    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        config_dir = candidate / "config"
        if (config_dir / "settings.yaml").is_file():
            return config_dir

    raise FileNotFoundError(
        "Nao foi possivel localizar config/settings.yaml a partir de "
        f"{current}. Defina a variavel {ENV_CONFIG_DIR} ou rode a partir da raiz do projeto."
    )


@dataclass
class HttpSettings:
    user_agent: str = "safra-dados/0.1"
    timeout_seconds: float = 60.0
    max_retries: int = 5
    backoff_factor: float = 1.5
    min_interval_seconds: float = 0.5


@dataclass
class Settings:
    config_dir: Path
    data_root: Path
    http: HttpSettings
    fontes: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    # ---- camadas de dados ----
    @property
    def bronze_dir(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_root / "gold"

    @property
    def gold_db_path(self) -> Path:
        return self.gold_dir / "safra.duckdb"

    @property
    def manual_dir(self) -> Path:
        return self.data_root / "manual"

    @property
    def exports_dir(self) -> Path:
        return self.data_root / "exports"

    @property
    def logs_dir(self) -> Path:
        return self.data_root / "logs"

    def referencias_path(self) -> Path:
        return self.config_dir / "referencias.yaml"

    def bronze_fonte_dir(self, fonte: str, data_base: str | None = None) -> Path:
        base = self.bronze_dir / fonte
        return base / data_base if data_base else base

    def silver_fonte_dir(self, fonte: str) -> Path:
        return self.silver_dir / fonte

    def checkpoint_path(self, fonte: str) -> Path:
        return self.bronze_fonte_dir(fonte) / "checkpoint.json"

    def manifest_path(self, fonte: str, data_base: str) -> Path:
        return self.bronze_fonte_dir(fonte, data_base) / "manifest.jsonl"

    def fonte_config(self, fonte: str) -> dict[str, Any]:
        try:
            return self.fontes[fonte]
        except KeyError as exc:
            raise KeyError(
                f"Fonte '{fonte}' nao configurada em config/settings.yaml (fontes: {list(self.fontes)})"
            ) from exc

    def entidades_path(self) -> Path:
        return self.config_dir / "entidades.yaml"

    def entidades_resolvidas_path(self) -> Path:
        return self.config_dir / "entidades_resolvidas.yaml"


def load_settings(config_dir: Path | None = None) -> Settings:
    config_dir = config_dir or find_config_dir()
    settings_path = config_dir / "settings.yaml"
    with settings_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    data_root_env = os.environ.get(ENV_DATA_HOME)
    if data_root_env:
        data_root = Path(data_root_env).expanduser()
    else:
        data_root_cfg = raw.get("data_root", "data")
        data_root = Path(data_root_cfg)
        if not data_root.is_absolute():
            # relativo a raiz do projeto (pai do diretorio config/)
            data_root = config_dir.parent / data_root

    http_raw = raw.get("http", {})
    http_settings = HttpSettings(
        user_agent=http_raw.get("user_agent", HttpSettings.user_agent),
        timeout_seconds=float(http_raw.get("timeout_seconds", HttpSettings.timeout_seconds)),
        max_retries=int(http_raw.get("max_retries", HttpSettings.max_retries)),
        backoff_factor=float(http_raw.get("backoff_factor", HttpSettings.backoff_factor)),
        min_interval_seconds=float(
            http_raw.get("min_interval_seconds", HttpSettings.min_interval_seconds)
        ),
    )

    return Settings(
        config_dir=config_dir,
        data_root=data_root.resolve(),
        http=http_settings,
        fontes=raw.get("fontes", {}),
        raw=raw,
    )


def load_entidades(settings: Settings) -> dict[str, Any]:
    path = settings.entidades_path()
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_entidades_resolvidas(settings: Settings) -> dict[str, Any] | None:
    path = settings.entidades_resolvidas_path()
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}
