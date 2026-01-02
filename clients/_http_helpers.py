"""Reusable helpers for lightweight HTTP-based OpenAPI clients."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

__all__ = ["normalize_params", "request_bytes"]

ErrorT = TypeVar("ErrorT", bound=Exception)


def normalize_params(
    params: Mapping[str, Any] | None,
    *,
    preserve_bool: bool = False,
) -> dict[str, Any]:
    """
    Convert mapping values to ``str`` while dropping ``None`` entries.

    When ``preserve_bool`` is enabled boolean values are kept as-is so that
    callers can control their final representation.
    """
    if not params:
        return {}

    normalized: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue

        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            sequence: list[Any] = []
            for item in value:
                if item is None:
                    continue
                if preserve_bool and isinstance(item, bool):
                    sequence.append(item)
                else:
                    sequence.append(str(item))
            if sequence:
                normalized[key] = sequence
            continue

        if preserve_bool and isinstance(value, bool):
            normalized[key] = value
        else:
            normalized[key] = str(value)

    return normalized


def request_bytes(
    endpoint: str,
    params: Mapping[str, Any] | None,
    *,
    timeout: float,
    error_cls: type[ErrorT],
    service_name: str,
    preserve_bool: bool = False,
) -> tuple[bytes, dict[str, str]]:
    """
    Perform a simple GET request, returning the raw body and headers.

    Parameters are URL-encoded using :func:`urlencode` with ``doseq=True`` to
    support multi-valued items.
    """
    encoded_params = urlencode(
        normalize_params(params, preserve_bool=preserve_bool),
        doseq=True,
    )
    request_url = endpoint if not encoded_params else f"{endpoint}?{encoded_params}"

    try:
        with urlopen(request_url, timeout=timeout) as response:
            raw_body = response.read()
            headers = dict(response.headers.items())
    except HTTPError as exc:
        raise error_cls(f"{service_name} returned HTTP {exc.code}: {exc.reason}") from exc
    except URLError as exc:
        raise error_cls(f"Failed to reach {service_name}: {exc.reason}") from exc

    return raw_body, headers
