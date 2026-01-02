"""
Lightweight client helpers for the Korean Juso (address) OpenAPI.

The module focuses on the JSON variants of the *addrLink* (road address search)
and *addrDetail* (road address detail) endpoints provided by
``https://business.juso.go.kr``. It mirrors the structure and defensive coding
style of ``clients.vworld_client`` to keep the project consistent.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal, MutableMapping
from ._http_helpers import request_bytes

__all__ = [
    "JusoAPIError",
    "call_juso_api",
    "search_road_addresses",
    "fetch_road_address_detail",
]

JUSO_BASE_URL = "https://business.juso.go.kr/addrlink"
JUSO_ROAD_ENDPOINT = f"{JUSO_BASE_URL}/addrLinkApi.do"
JUSO_DETAIL_ENDPOINT = f"{JUSO_BASE_URL}/addrDetailApi.do"

EndpointName = Literal["road", "detail"]

_DEFAULT_TIMEOUT = 8.0
_SUCCESS_CODE = "0"


class JusoAPIError(RuntimeError):
    """Raised when a Juso OpenAPI request fails."""


def _extract_results(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise JusoAPIError("Unexpected response payload: expected a JSON object.")
    results = payload.get("results")
    if not isinstance(results, MutableMapping):
        raise JusoAPIError("Unexpected response payload: missing 'results'.")
    common = results.get("common")
    if not isinstance(common, MutableMapping):
        raise JusoAPIError("Unexpected response payload: missing 'common'.")
    error_code = str(common.get("errorCode", "")).strip()
    if error_code != _SUCCESS_CODE:
        error_message = str(common.get("errorMessage", "")).strip() or "unknown error"
        raise JusoAPIError(f"Juso API error {error_code}: {error_message}")
    return dict(results)


def _perform_request(endpoint: str, params: Mapping[str, Any], timeout: float) -> dict[str, Any]:
    raw_body, _ = request_bytes(
        endpoint,
        params,
        timeout=timeout,
        error_cls=JusoAPIError,
        service_name="Juso API",
        preserve_bool=True,
    )
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise JusoAPIError(f"Failed to decode JSON response: {exc.msg}") from exc

    return _extract_results(payload)


def call_juso_api(
    endpoint: EndpointName,
    params: Mapping[str, Any],
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    Invoke one of the supported Juso API endpoints and return the parsed results.

    Parameters
    ----------
    endpoint:
        Supported endpoint selector. ``"road"`` resolves to the road-address
        search API while ``"detail"`` targets the road-address detail API.
    params:
        Query parameters to forward to the API. ``resultType`` is forced to
        ``"json"`` by the public helpers, but callers may override it if needed.
    timeout:
        Socket timeout in seconds passed to ``urllib.request.urlopen``.
    """
    if endpoint == "road":
        target = JUSO_ROAD_ENDPOINT
    elif endpoint == "detail":
        target = JUSO_DETAIL_ENDPOINT
    else:  # pragma: no cover - narrow typing prevents regular execution.

        raise ValueError(f"Unsupported Juso endpoint '{endpoint}'.")

    return _perform_request(target, params, timeout)


def _coerce_flag(value: bool | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in {"Y", "N"}:
            return normalized
        raise ValueError("String flags must be either 'Y' or 'N'.")
    return "Y" if value else "N"


def search_road_addresses(
    keyword: str,
    *,
    api_key: str,
    page: int = 1,
    size: int = 20,
    history: bool | str | None = None,
    timeout: float = _DEFAULT_TIMEOUT,
    extra_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Search road-name addresses using the Juso ``addrLinkApi`` endpoint.

    The function validates common parameters, forces JSON responses, and
    normalises the returned payload while bubbling up API level failures as
    :class:`JusoAPIError`.
    """
    if not keyword or not keyword.strip():
        raise ValueError("keyword must be a non-empty string.")
    if not api_key or not api_key.strip():
        raise ValueError("api_key must be provided.")
    if page < 1:
        raise ValueError("page must be greater than or equal to 1.")
    if not (1 <= size <= 100):
        raise ValueError("size must be between 1 and 100.")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero.")

    params: dict[str, Any] = {
        "confmKey": api_key.strip(),
        "keyword": keyword.strip(),
        "currentPage": page,
        "countPerPage": size,
        "resultType": "json",
        "addInfoYn":"Y"
    }

    history_flag = _coerce_flag(history)
    if history_flag is not None:
        params["hstryYn"] = history_flag

    if extra_params:
        for key, value in extra_params.items():
            if value is None:
                continue
            params[key] = value

    return call_juso_api("road", params, timeout=timeout)


def fetch_road_address_detail(
    adm_code: str,
    road_name_code: str,
    *,
    api_key: str,
    underground: bool | str,
    building_main_number: int,
    building_sub_number: int,
    timeout: float = _DEFAULT_TIMEOUT,
    extra_params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Retrieve detailed information for a road-name address.

    Parameters map to the ``addrDetailApi`` specification:

    ``adm_code``
        Administrative district code (``admCd``).
    ``road_name_code``
        Road-name management serial (``rnMgtSn``).
    ``underground``
        ``True``/``"Y"`` for underground, ``False``/``"N"`` otherwise (``udrtYn``).
    ``building_main_number``
        Primary building number (``buldMnnm``).
    ``building_sub_number``
        Secondary building number (``buldSlno``). Use ``0`` when absent.
    """
    if not adm_code or not adm_code.strip():
        raise ValueError("adm_code must be a non-empty string.")
    if not road_name_code or not road_name_code.strip():
        raise ValueError("road_name_code must be a non-empty string.")
    if not api_key or not api_key.strip():
        raise ValueError("api_key must be provided.")
    if building_main_number < 0:
        raise ValueError("building_main_number must be non-negative.")
    if building_sub_number < 0:
        raise ValueError("building_sub_number must be non-negative.")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero.")

    params: dict[str, Any] = {
        "confmKey": api_key.strip(),
        "admCd": adm_code.strip(),
        "rnMgtSn": road_name_code.strip(),
        "udrtYn": _coerce_flag(underground),
        "buldMnnm": building_main_number,
        "buldSlno": building_sub_number,
        "resultType": "json",
    }

    if params["udrtYn"] is None:
        raise ValueError("underground must be provided as a bool or 'Y'/'N'.")

    if extra_params:
        for key, value in extra_params.items():
            if value is None:
                continue
            params[key] = value

    return call_juso_api("detail", params, timeout=timeout)
