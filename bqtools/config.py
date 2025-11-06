from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover
    load_dotenv = None


def _env_flag(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _running_in_cloud_run() -> bool:
    cloud_run_markers = ("K_SERVICE", "K_REVISION", "K_CONFIGURATION")
    return any(os.environ.get(marker) for marker in cloud_run_markers)


def load_environment(dotenv_path: str | os.PathLike[str] | None = None) -> None:
    """Load local .env files unless running on Cloud Run (which injects env vars via Secret Manager)."""
    if load_dotenv is None:
        return

    # Cloud Run receives secrets as environment variables, so skip .env loading unless explicitly forced.
    if _running_in_cloud_run() and not _env_flag("FORCE_DOTENV", False):
        return

    if dotenv_path:
        load_dotenv(dotenv_path)
    else:
        load_dotenv()


def default_credentials_file() -> Path:
    return Path(os.environ.get("BIGQUERY_CREDENTIALS_FILE", "oauth_credentials.json"))
