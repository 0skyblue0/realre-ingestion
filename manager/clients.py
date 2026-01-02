from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any


class ClientLoadError(RuntimeError):
    """Raised when a client module cannot be loaded."""


@dataclass
class ClientLoader:
    default_timeout: float = 8.0

    def load(self, name: str) -> Any:
        candidates = [f"clients.{name}_client", f"clients.{name}"]
        last_error: Exception | None = None
        for candidate in candidates:
            try:
                return importlib.import_module(candidate)
            except ModuleNotFoundError as exc:
                last_error = exc
                continue
        raise ClientLoadError(f"Could not load client '{name}': {last_error}")

    def call(self, client: Any, method: str, *args: Any, **kwargs: Any) -> Any:
        func = getattr(client, method, None)
        if not callable(func):
            raise ClientLoadError(f"Client does not implement method '{method}'.")
        return func(*args, **kwargs)
