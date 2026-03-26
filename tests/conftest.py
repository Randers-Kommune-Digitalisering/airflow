from __future__ import annotations

import sys
from pathlib import Path
import types
import json


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

    class _FakeConnection:  # type: ignore[too-few-public-methods]
        """Minimal stand-in for Airflow Connection used in unit tests."""

        def __init__(self):
            self.host = ""
            self.login = ""
            self.password = ""
            self.schema = ""
            self.port = None
            self.extra = "{}"

    class BaseHook:  # type: ignore[too-few-public-methods]
        @staticmethod
        def get_connection(_conn_id: str):  # noqa: ANN001
            # Default to a harmless fake connection so modules that call
            # BaseHook.get_connection at import time can still be imported.
            # Individual tests may monkeypatch this for more specific behavior.
            return _FakeConnection()

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


try:
    from airflow.models import Variable as _Variable  # noqa: F401
except Exception:
    models_mod = _ensure_module("airflow.models")

    class Variable:  # type: ignore[too-few-public-methods]
        _store: dict[str, str] = {}

        @classmethod
        def get(
            cls,
            key: str,
            default_var=None,  # noqa: ANN001
            deserialize_json: bool = False,
        ):  # noqa: ANN201
            val = cls._store.get(key)
            if val is None:
                return default_var
            if not deserialize_json:
                return val
            try:
                return json.loads(val)
            except Exception:
                return default_var

        @classmethod
        def set(
            cls,
            key: str,
            value,  # noqa: ANN001
            serialize_json: bool = False,
        ) -> None:
            if serialize_json:
                cls._store[key] = json.dumps(value)
            else:
                cls._store[key] = str(value)

    models_mod.Variable = Variable
