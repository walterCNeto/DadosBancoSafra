"""Cliente HTTP com retry/backoff, limite de taxa e download com hash, usado por
todos os conectores. Baseado em httpx; nao contorna autenticacao de nenhum servico."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from .config import HttpSettings

logger = logging.getLogger(__name__)

# Status HTTP que valem retry (erros transitorios do servidor / rede)
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class DownloadError(RuntimeError):
    """Erro ao baixar ou requisitar um recurso remoto, apos esgotar as tentativas."""


@dataclass
class DownloadResult:
    bytes_written: int
    sha256: str
    status: str  # "ok" | "falha"
    erro: str | None = None


class RateLimiter:
    """Limite de taxa simples: garante um intervalo minimo entre chamadas."""

    def __init__(self, min_interval_seconds: float) -> None:
        self._min_interval = max(0.0, min_interval_seconds)
        self._last_call: float | None = None

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        if self._last_call is not None:
            elapsed = now - self._last_call
            remaining = self._min_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_call = time.monotonic()


class SafraDadosClient:
    """Wrapper fino sobre httpx.Client com retry/backoff e rate limiting,
    identificado por um User-Agent proprio (boas praticas com APIs publicas)."""

    def __init__(self, settings: HttpSettings) -> None:
        self._settings = settings
        self._rate_limiter = RateLimiter(settings.min_interval_seconds)
        self._client = httpx.Client(
            headers={"User-Agent": settings.user_agent},
            timeout=settings.timeout_seconds,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SafraDadosClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _request_with_retry(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1, self._settings.max_retries + 1):
            self._rate_limiter.wait()
            try:
                response = self._client.request(method, url, **kwargs)
            except httpx.TransportError as exc:
                last_exc = exc
                logger.warning("tentativa %s/%s falhou (%s): %s", attempt, self._settings.max_retries, url, exc)
            else:
                if response.status_code not in _RETRYABLE_STATUS:
                    return response
                last_exc = DownloadError(
                    f"HTTP {response.status_code} em {url}: {response.text[:300]!r}"
                )
                logger.warning(
                    "tentativa %s/%s recebeu status retentavel %s em %s",
                    attempt, self._settings.max_retries, response.status_code, url,
                )
            if attempt < self._settings.max_retries:
                sleep_s = self._settings.backoff_factor ** attempt
                time.sleep(sleep_s)
        assert last_exc is not None
        raise DownloadError(str(last_exc)) from last_exc

    def get(self, url: str, params: dict | None = None) -> httpx.Response:
        return self._request_with_retry("GET", url, params=params)

    def head(self, url: str) -> httpx.Response:
        """HEAD sem retry/backoff longo: usado so para sondar existencia de um recurso."""
        self._rate_limiter.wait()
        return self._client.request("HEAD", url)

    def exists(self, url: str) -> bool:
        try:
            response = self.head(url)
        except httpx.TransportError:
            return False
        if response.status_code == 200:
            return True
        if response.status_code in (403, 405):
            # alguns servidores nao aceitam HEAD; confirma com GET em range curto
            try:
                response = self.get(url, params=None)
            except DownloadError:
                return False
            return response.status_code == 200
        return False

    def download_to_file(self, url: str, dest: Path, params: dict | None = None) -> DownloadResult:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".part")
        sha256 = hashlib.sha256()
        total = 0
        try:
            response = self._request_with_retry("GET", url, params=params)
            if response.status_code != 200:
                return DownloadResult(0, "", "falha", f"HTTP {response.status_code}")
            with tmp_path.open("wb") as fh:
                for chunk in response.iter_bytes():
                    fh.write(chunk)
                    sha256.update(chunk)
                    total += len(chunk)
            tmp_path.replace(dest)
            return DownloadResult(total, sha256.hexdigest(), "ok")
        except DownloadError as exc:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(0, "", "falha", str(exc))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
