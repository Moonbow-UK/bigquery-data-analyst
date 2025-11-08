from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

from google.auth.transport.requests import Request
from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

DEFAULT_SCOPES: tuple[str, ...] = ("https://www.googleapis.com/auth/bigquery",)


def _normalize_oauth_url(value: str | None) -> str | None:
    """Ensure OAuth endpoints have the proper scheme syntax."""
    if not value:
        return value

    stripped = value.strip()
    if "://" in stripped:
        return stripped

    if stripped.startswith("https:/"):
        return "https://" + stripped[len("https:/") :]
    if stripped.startswith("http:/"):
        return "http://" + stripped[len("http:/") :]
    return stripped


def load_credentials(path: Path, scopes: Sequence[str] | None = None) -> Credentials:
    """Load either OAuth user or service account credentials."""
    expanded_path = path.expanduser()
    raw_value = str(expanded_path)
    inline_json = raw_value.lstrip().startswith("{") and raw_value.rstrip().endswith("}")

    if inline_json:
        try:
            credential_info = json.loads(raw_value)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise RuntimeError(
                "BIGQUERY_CREDENTIALS_FILE appears to contain inline JSON but it could not be parsed."
            ) from exc
    else:
        if not expanded_path.exists():
            raise FileNotFoundError(
                f"Credentials file not found at {expanded_path}. "
                "Download an OAuth 'authorized_user' JSON or a service account key and point the script to it."
            )
        with expanded_path.open("r", encoding="utf-8") as fh:
            credential_info = json.load(fh)

    normalized_token_uri = _normalize_oauth_url(credential_info.get("token_uri"))
    if normalized_token_uri:
        credential_info["token_uri"] = normalized_token_uri

    scopes_tuple = tuple(scopes) if scopes else DEFAULT_SCOPES
    credentials_type = credential_info.get("type")

    if credentials_type == "authorized_user":
        credentials = Credentials.from_authorized_user_info(credential_info, scopes=scopes_tuple)
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        if not credentials.valid:
            raise RuntimeError(
                "OAuth credentials could not be validated. "
                "Ensure the file contains a refresh token and the scopes are correct."
            )
        return credentials

    if credentials_type == "service_account":
        return service_account.Credentials.from_service_account_info(
            credential_info,
            scopes=scopes_tuple,
        )

    raise RuntimeError(
        "Unsupported credentials file. Expected 'authorized_user' or 'service_account' JSON."
    )


def derive_project_id(credentials: Credentials, explicit_project: str | None) -> str:
    """Resolve the project that should back BigQuery requests."""
    if explicit_project:
        return explicit_project

    for candidate in (
        getattr(credentials, "quota_project_id", None),
        getattr(credentials, "project_id", None),
    ):
        if candidate:
            return candidate

    raise RuntimeError(
        "Project ID is required but was not supplied and could not be inferred. "
        "Pass --project explicitly."
    )


def build_client(
    *,
    credentials_file: Path,
    scopes: Iterable[str] | None = None,
    project_override: str | None = None,
) -> bigquery.Client:
    """Construct a BigQuery client using the shared auth helpers."""
    expanded_path = credentials_file.expanduser()
    credentials = load_credentials(expanded_path, tuple(scopes) if scopes else None)
    project_id = derive_project_id(credentials, project_override)
    return bigquery.Client(project=project_id, credentials=credentials)
