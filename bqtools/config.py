from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover
    load_dotenv = None


def load_environment(dotenv_path: str | os.PathLike[str] | None = None) -> None:
    """Load environment variables from a .env file when python-dotenv is available."""
    if load_dotenv is None:
        return
    if dotenv_path:
        load_dotenv(dotenv_path)
    else:
        load_dotenv()


def default_credentials_file() -> Path:
    return Path(os.environ.get("BIGQUERY_CREDENTIALS_FILE", "oauth_credentials.json"))
