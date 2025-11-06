from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.cloud import storage

from .config import running_in_cloud_run

logger = logging.getLogger("bqtools.storage")


def _env_flag(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class StorageContext:
    running_in_cloud_run: bool
    state_root: Path
    export_root: Path
    csv_dir: Path
    json_dir: Path
    log_dir: Path
    bucket_name: str | None
    export_prefix: str
    log_prefix: str
    use_gcs_exports: bool
    use_gcs_logs: bool


def _build_context() -> StorageContext:
    running = running_in_cloud_run()
    force_gcs = _env_flag("FORCE_GCS_STORAGE", False)
    bucket_name = os.environ.get("GCS_APP_BUCKET") or os.environ.get("APP_STORAGE_BUCKET")
    export_prefix = (os.environ.get("GCS_EXPORT_PREFIX", "exports") or "exports").strip()
    log_prefix = (os.environ.get("GCS_LOG_PREFIX", "var/logs") or "var/logs").strip()
    state_root = Path("/tmp") if running else Path(".")
    export_root = Path(os.environ.get("APP_EXPORT_ROOT", str(state_root / "var/exports")))
    log_root = Path(os.environ.get("APP_LOG_ROOT", str(state_root / "var/logs")))
    csv_dir = export_root / "csv"
    json_dir = export_root / "json"
    export_root.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)
    use_gcs = bool(bucket_name) and (running or force_gcs)
    return StorageContext(
        running_in_cloud_run=running,
        state_root=state_root,
        export_root=export_root,
        csv_dir=csv_dir,
        json_dir=json_dir,
        log_dir=log_root,
        bucket_name=bucket_name,
        export_prefix=export_prefix,
        log_prefix=log_prefix,
        use_gcs_exports=use_gcs,
        use_gcs_logs=use_gcs,
    )


_CTX = _build_context()

EXPORT_ROOT = _CTX.export_root
CSV_EXPORT_DIR = _CTX.csv_dir
JSON_EXPORT_DIR = _CTX.json_dir
LOG_DIR = _CTX.log_dir
GCS_BUCKET_NAME = _CTX.bucket_name
GCS_EXPORT_PREFIX = _CTX.export_prefix
GCS_LOG_PREFIX = _CTX.log_prefix
USE_GCS_EXPORTS = _CTX.use_gcs_exports
USE_GCS_LOGS = _CTX.use_gcs_logs
RUNNING_IN_CLOUD_RUN = _CTX.running_in_cloud_run

_STORAGE_CLIENT: storage.Client | None = None
_STORAGE_BUCKET: Any | None = None
_LOG_UPLOAD_MTIMES: dict[Path, float] = {}


def storage_context() -> StorageContext:
    return _CTX


def _get_storage_client() -> storage.Client | None:
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is None:
        try:
            _STORAGE_CLIENT = storage.Client()
        except Exception as exc:  # pragma: no cover - network/auth failures
            logger.error("Failed to initialise Cloud Storage client: %s", exc)
            return None
    return _STORAGE_CLIENT


def _get_bucket() -> Any:
    global _STORAGE_BUCKET
    if GCS_BUCKET_NAME is None:
        return None
    if _STORAGE_BUCKET is None:
        client = _get_storage_client()
        if client is None:
            return None
        _STORAGE_BUCKET = client.bucket(GCS_BUCKET_NAME)
    return _STORAGE_BUCKET


def _relative_to_base(path: Path, base: Path) -> Path | None:
    try:
        return path.resolve().relative_to(base.resolve())
    except ValueError:
        return None


def relative_to_export_root(path: Path) -> Path | None:
    return _relative_to_base(path, EXPORT_ROOT)


def _build_blob_name(prefix: str, relative_path: Path) -> str:
    clean_prefix = (prefix or "").strip("/")
    relative = relative_path.as_posix().lstrip("/")
    if clean_prefix and relative:
        return f"{clean_prefix}/{relative}"
    if clean_prefix:
        return clean_prefix
    return relative


def _download_from_gcs(path: Path, *, prefix: str) -> None:
    if not USE_GCS_EXPORTS or not GCS_BUCKET_NAME:
        return
    relative = relative_to_export_root(path)
    if relative is None:
        return
    bucket = _get_bucket()
    if bucket is None:
        return
    blob_name = _build_blob_name(prefix, relative)
    try:
        blob = bucket.get_blob(blob_name)
    except Exception as exc:  # pragma: no cover - network failure
        logger.warning("Failed to query blob %s: %s", blob_name, exc)
        return
    if blob is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        blob.download_to_filename(path)
        logger.debug("Downloaded gs://%s/%s -> %s", GCS_BUCKET_NAME, blob_name, path)
    except Exception as exc:  # pragma: no cover - network failure
        logger.warning("Failed to download %s: %s", blob_name, exc)


def ensure_local_export_file(path: Path) -> None:
    if path.exists():
        return
    _download_from_gcs(path, prefix=GCS_EXPORT_PREFIX)


def _upload_to_gcs(path: Path, *, prefix: str) -> None:
    if not path.exists() or not GCS_BUCKET_NAME:
        return
    relative = relative_to_export_root(path)
    if relative is None:
        return
    bucket = _get_bucket()
    if bucket is None:
        return
    blob_name = _build_blob_name(prefix, relative)
    blob = bucket.blob(blob_name)
    try:
        blob.upload_from_filename(path)
        logger.debug("Uploaded %s to gs://%s/%s", path, GCS_BUCKET_NAME, blob_name)
    except Exception as exc:  # pragma: no cover - network failure
        logger.warning("Failed to upload %s to %s: %s", path, blob_name, exc)


def sync_export_artifact(path: Path) -> None:
    if not USE_GCS_EXPORTS:
        return
    _upload_to_gcs(path, prefix=GCS_EXPORT_PREFIX)


def export_file_exists(path: Path) -> bool:
    ensure_local_export_file(path)
    if path.exists():
        return True
    if not USE_GCS_EXPORTS or not GCS_BUCKET_NAME:
        return False
    relative = relative_to_export_root(path)
    if relative is None:
        return False
    bucket = _get_bucket()
    if bucket is None:
        return False
    blob_name = _build_blob_name(GCS_EXPORT_PREFIX, relative)
    try:
        blob = bucket.blob(blob_name)
        client = _get_storage_client()
        if client is None:
            return False
        return blob.exists(client=client)
    except Exception as exc:  # pragma: no cover - network failure
        logger.warning("Failed to check blob %s: %s", blob_name, exc)
        return False


def export_modified_time(path: Path) -> datetime | None:
    if USE_GCS_EXPORTS and GCS_BUCKET_NAME:
        relative = relative_to_export_root(path)
        if relative is not None:
            bucket = _get_bucket()
            if bucket is not None:
                blob_name = _build_blob_name(GCS_EXPORT_PREFIX, relative)
                try:
                    blob = bucket.get_blob(blob_name)
                except Exception as exc:  # pragma: no cover
                    logger.warning("Failed to read blob metadata %s: %s", blob_name, exc)
                    blob = None
                if blob and blob.updated:
                    return blob.updated
    if path.exists():
        try:
            return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return None
    return None


def mirror_log_file(path: Path) -> None:
    if not USE_GCS_LOGS or not path.exists():
        return
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return
    last_uploaded = _LOG_UPLOAD_MTIMES.get(path)
    if last_uploaded is not None and mtime <= last_uploaded:
        return
    _LOG_UPLOAD_MTIMES[path] = mtime
    _upload_to_gcs(path, prefix=GCS_LOG_PREFIX)


def list_remote_export_relpaths(*, suffix: str | None = None) -> list[Path]:
    if not USE_GCS_EXPORTS or not GCS_BUCKET_NAME:
        return []
    bucket = _get_bucket()
    if bucket is None:
        return []
    prefix = GCS_EXPORT_PREFIX.strip("/")
    list_prefix = f"{prefix}/" if prefix else ""
    try:
        blobs = bucket.list_blobs(prefix=list_prefix or None)
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to list blobs under %s: %s", list_prefix or "<root>", exc)
        return []
    results: list[Path] = []
    prefix_len = len(list_prefix)
    for blob in blobs:
        name = blob.name
        if not name or name.endswith("/"):
            continue
        if list_prefix and name.startswith(list_prefix):
            relative_name = name[prefix_len:]
        else:
            relative_name = name
        if not relative_name:
            continue
        if suffix and not relative_name.endswith(suffix):
            continue
        results.append(Path(relative_name))
    return results


def storage_display_path(path: Path) -> str:
    relative = relative_to_export_root(path)
    rel_str = relative.as_posix().lstrip("/") if relative is not None else None
    if USE_GCS_EXPORTS and GCS_BUCKET_NAME and rel_str is not None:
        prefix = GCS_EXPORT_PREFIX.strip("/")
        path_part = f"{prefix}/{rel_str}" if prefix else rel_str
        return f"gs://{GCS_BUCKET_NAME}/{path_part}" if path_part else f"gs://{GCS_BUCKET_NAME}"
    if rel_str is not None:
        return f"var/exports/{rel_str}"
    return str(path)


__all__ = [
    "CSV_EXPORT_DIR",
    "EXPORT_ROOT",
    "JSON_EXPORT_DIR",
    "LOG_DIR",
    "RUNNING_IN_CLOUD_RUN",
    "GCS_BUCKET_NAME",
    "GCS_EXPORT_PREFIX",
    "GCS_LOG_PREFIX",
    "USE_GCS_EXPORTS",
    "USE_GCS_LOGS",
    "ensure_local_export_file",
    "export_file_exists",
    "export_modified_time",
    "list_remote_export_relpaths",
    "mirror_log_file",
    "relative_to_export_root",
    "storage_context",
    "sync_export_artifact",
    "storage_display_path",
]
