from __future__ import annotations

import sys
from pathlib import Path
import types


# Ensure `dags/` is importable as a top-level package during tests.
# This makes imports like `from dag_affald...` work reliably regardless of how pytest is invoked.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_DAGS_PATH = _REPO_ROOT / "dags"

if _DAGS_PATH.exists():
    dags_str = str(_DAGS_PATH)
    if dags_str not in sys.path:
        sys.path.insert(0, dags_str)


def _ensure_module(name: str) -> types.ModuleType:
    mod = sys.modules.get(name)
    if mod is None:
        mod = types.ModuleType(name)
        sys.modules[name] = mod
    return mod


# Provide minimal stubs for the Airflow modules used by this repo when missing.
# This covers both "Airflow not installed" and "Airflow installed but provider extras missing".
try:
    import airflow  # noqa: F401
except Exception:
    _ensure_module("airflow")


try:
    from airflow.hooks.base import BaseHook as _BaseHook  # noqa: F401
except Exception:
    _ensure_module("airflow.hooks")
    base_mod = _ensure_module("airflow.hooks.base")

    class BaseHook:  # type: ignore[too-few-public-methods]
        @staticmethod
        def get_connection(_conn_id: str):  # noqa: ANN001
            raise RuntimeError("Airflow hook stub; patch BaseHook.get_connection in tests")

    base_mod.BaseHook = BaseHook


try:
    from airflow.providers.sftp.hooks.sftp import SFTPHook as _SFTPHook  # noqa: F401
except Exception:
    _ensure_module("airflow.providers")
    _ensure_module("airflow.providers.sftp")
    _ensure_module("airflow.providers.sftp.hooks")
    sftp_mod = _ensure_module("airflow.providers.sftp.hooks.sftp")

    class SFTPHook:  # type: ignore[too-few-public-methods]
        def __init__(self, _conn_id: str):
            self.conn_id = _conn_id

        def get_conn(self):  # noqa: ANN001
            raise RuntimeError("Airflow SFTP stub; patch SFTPHook.get_conn in tests")

    sftp_mod.SFTPHook = SFTPHook
