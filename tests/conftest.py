from __future__ import annotations

import yaml
import pytest

from safra_dados.config import HttpSettings, Settings


@pytest.fixture
def settings_factory(tmp_path):
    """Monta um Settings de teste com config/entidades*.yaml gravados em um
    diretorio temporario, sem depender do repositorio real nem de rede."""

    def _make(entidades: dict, entidades_resolvidas: dict | None = None, fontes: dict | None = None) -> Settings:
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "entidades.yaml").write_text(
            yaml.safe_dump(entidades, allow_unicode=True), encoding="utf-8"
        )
        if entidades_resolvidas is not None:
            (config_dir / "entidades_resolvidas.yaml").write_text(
                yaml.safe_dump(entidades_resolvidas, allow_unicode=True), encoding="utf-8"
            )
        return Settings(
            config_dir=config_dir,
            data_root=tmp_path / "data",
            http=HttpSettings(),
            fontes=fontes or {},
        )

    return _make
