from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import re

from ._http_helpers import normalize_params, request_bytes

VWORLD_METADATA_PATH = (Path(__file__).resolve().parent / "vworld" / "vworld_url.json").resolve()
VWORLD_SEARCH_ENDPOINT = "https://api.vworld.kr/req/search"

_KOREAN_REQUIRED_FLAG = "필수"
_REQUEST_KEY = "요청 변수"
_URL_KEY = "URL"


class VWorldAPIError(RuntimeError):
    """Raised when a vworld API call fails."""


@dataclass(frozen=True)
class VWorldApiDefinition:
    name: str
    metadata: dict[str, Any]

    @property
    def endpoint(self) -> str:
        try:
            return self.metadata[_URL_KEY]
        except KeyError as exc:  # pragma: no cover - defensive, depends on external file integrity.
            raise VWorldAPIError(f"Metadata for '{self.name}' is missing the '{_URL_KEY}' field.") from exc

    @property
    def request_fields(self) -> dict[str, Any]:
        return self.metadata.get(_REQUEST_KEY, {})


def _load_api_catalog() -> dict[str, VWorldApiDefinition]:
    try:
        with VWORLD_METADATA_PATH.open(encoding="utf-8") as metadata_file:
            raw_catalog = json.load(metadata_file)
    except FileNotFoundError as exc:
        raise VWorldAPIError(f"Missing vworld metadata file: {VWORLD_METADATA_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise VWorldAPIError(
            f"Failed to decode JSON metadata from {VWORLD_METADATA_PATH}: {exc.msg}"
        ) from exc

    return {name: VWorldApiDefinition(name, info) for name, info in raw_catalog.items()}


_API_CATALOG: dict[str, VWorldApiDefinition] | None = None


def _get_api_catalog() -> dict[str, VWorldApiDefinition]:
    global _API_CATALOG
    if _API_CATALOG is None:
        _API_CATALOG = _load_api_catalog()
    return _API_CATALOG


def get_vworld_api_info(api_name: str) -> VWorldApiDefinition:
    """
    Retrieve the metadata for the requested API.

    Parameters
    ----------
    api_name:
        The key stored in ``vworld_url.json`` (e.g. ``"getBuildingAge"``).
    """
    try:
        return _get_api_catalog()[api_name]
    except KeyError as exc:
        available = ", ".join(sorted(_get_api_catalog().keys()))
        raise VWorldAPIError(f"Unknown vworld API '{api_name}'. Available APIs: {available}") from exc


def call_vworld_api(
    api_name: str,
    params: Mapping[str, Any] | None = None,
    *,
    api_key: str | None = None,
    domain: str | None = None,
    timeout: float = 10.0,
    parse_json: bool | None = None,
) -> Any:
    """
    Call one of the vworld OpenAPI endpoints using metadata from ``vworld_url.json``.

    Parameters
    ----------
    api_name:
        Entry key inside the metadata file (e.g. ``"getBuildingAge"``).
    params:
        Query parameters for the request. Values are stringified automatically.
    api_key:
        Optional API key. If provided, it is injected as the ``key`` query parameter
        unless it already exists in ``params``.
    domain:
        Optional domain parameter. Injected only when missing from ``params``.
    timeout:
        Socket timeout (seconds) passed to ``urllib.request.urlopen``.
    parse_json:
        Force JSON decoding of the response (``True``) or skip it (``False``).
        When ``None`` (default) the function attempts to decode JSON whenever the
        request includes ``format=json`` or the response advertises
        ``Content-Type: application/json``.

    Returns
    -------
    Any
        Parsed JSON data or the raw UTF-8 decoded response body.

    Raises
    ------
    VWorldAPIError
        If metadata is missing, required parameters are absent, or the HTTP request fails.
    ValueError
        If ``timeout`` is non-positive.
    """
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero.")

    api_info = get_vworld_api_info(api_name)
    request_fields = api_info.request_fields

    query_params = normalize_params(params)
    if api_key is not None:
        query_params.setdefault("key", api_key)
    if domain is not None:
        query_params.setdefault("domain", domain)

    missing = [
        field_name
        for field_name, field_meta in request_fields.items()
        if field_meta.get("Required") == _KOREAN_REQUIRED_FLAG and field_name not in query_params
    ]
    if missing:
        raise VWorldAPIError(
            f"Missing required parameters for '{api_name}': {', '.join(sorted(missing))}"
        )

    raw_body, headers = request_bytes(
        api_info.endpoint,
        query_params,
        timeout=timeout,
        error_cls=VWorldAPIError,
        service_name=f"vworld API '{api_name}'",
    )
    content_type = headers.get("Content-Type", "")

    if parse_json is None:
        format_param = str(query_params.get("format", "")).lower()
        parse_json = format_param == "json" or "application/json" in content_type.lower()

    if parse_json:
        try:
            return json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise VWorldAPIError(
                f"Unable to decode JSON response from '{api_name}': {exc.msg}"
            ) from exc

    return raw_body.decode("utf-8")


def _perform_address_search_request(
    base_params: dict[str, Any],
    category: str,
    timeout: float,
) -> dict[str, Any]:
    query_params = dict(base_params)
    query_params["category"] = category.upper()

    raw_body, _ = request_bytes(
        VWORLD_SEARCH_ENDPOINT,
        query_params,
        timeout=timeout,
        error_cls=VWorldAPIError,
        service_name="vworld address search",
    )

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise VWorldAPIError(
            f"Unable to decode JSON response from address search: {exc.msg}"
        ) from exc

    response = payload.get("response")
    if not isinstance(response, Mapping):
        raise VWorldAPIError("Unexpected vworld address search payload: missing 'response'.")
    response_data = dict(response)

    status = response_data.get("status")
    if status == "NOT_FOUND":
        result = response_data.get("result")
        if isinstance(result, Mapping):
            result_dict = dict(result)
            if not isinstance(result_dict.get("items"), list):
                result_dict["items"] = []
        else:
            result_dict = {"items": []}
        response_data["result"] = result_dict
        return response_data
    if status != "OK":
        error_info = response_data.get("error")
        error_message = ""
        if isinstance(error_info, Mapping):
            error_message = str(error_info.get("text") or error_info.get("message") or "")
        elif error_info is not None:
            error_message = str(error_info)
        raise VWorldAPIError(
            f"vworld address search failed (status={status}): {error_message or 'unknown error'}"
        )

    return response_data


def _perform_validated_address_search_request(
    address: str,
    options: str,
    items: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Filter search items to those that closely match ``address``."""

    norm_address = address.strip()

    def _road_parts(value: str) -> tuple[str, str] | None:
        candidate = value.strip()
        if not candidate:
            return None
        match = re.search(r"([^\d\s]{1,}(?:로|길))\s*(\d+(?:-\d+)?)", candidate)
        if not match:
            match = re.search(r"([^\d\s]{1,}(?:로|길))(\d+(?:-\d+)?)", candidate.replace(" ", ""))
        if not match:
            return None
        return match.group(1), match.group(2)

    def _parcel_parts(value: str) -> tuple[str, str] | None:
        candidate = value.strip()
        if not candidate:
            return None
        match = re.search(r"([^\d\s]{1,})(?:동|리|가)?\s*(\d+(?:-\d+)?)", candidate)
        if not match:
            match = re.search(r"([^\d\s]{1,})(?:동|리|가)?(\d+(?:-\d+)?)", candidate.replace(" ", ""))
        if not match:
            return None
        return match.group(1), match.group(2)

    def _address_field(item: Mapping[str, Any], key: str) -> str:
        block = item.get('address') if isinstance(item, Mapping) else None
        if not isinstance(block, Mapping):
            return ''
        value = block.get(key, '')
        return str(value).strip()

    def _numbers_match(query: str, candidate: str) -> bool:
        if '-' in query:
            return query == candidate
        return '-' not in candidate and query.split('-', 1)[0] == candidate.split('-', 1)[0]

    filtered: list[dict[str, Any]] = []
    option = options.lower()

    if option == 'road':
        parts = _road_parts(norm_address)
        if not parts:
            return [dict(item) for item in items]
        query_name, query_no = parts
        normalized_query_name = query_name.replace(' ', '')
        for item in items:
            candidate = _address_field(item, 'road')
            if not candidate:
                continue
            candidate_parts = _road_parts(candidate)
            if not candidate_parts:
                continue
            cand_name, cand_no = candidate_parts
            if cand_name.replace(' ', '') != normalized_query_name:
                continue
            if _numbers_match(query_no, cand_no):
                filtered.append(dict(item))
        return filtered

    if option == 'parcel':
        parts = _parcel_parts(norm_address)
        if not parts:
            return [dict(item) for item in items]
        query_name, query_no = parts
        normalized_query_name = query_name.replace(' ', '')
        for item in items:
            candidate = _address_field(item, 'parcel')
            if not candidate:
                continue
            candidate_parts = _parcel_parts(candidate)
            if not candidate_parts:
                continue
            cand_name, cand_no = candidate_parts
            if cand_name.replace(' ', '') != normalized_query_name:
                continue
            if _numbers_match(query_no, cand_no):
                filtered.append(dict(item))
        return filtered

    return [dict(item) for item in items]


def search_address(
    address: str,
    *,
    api_key: str,
    category: str = "ROAD",
    crs: str = "EPSG:4326",
    size: int = 10,
    page: int = 1,
    bbox: Sequence[float] | None = None,
    domain: str | None = None,
    timeout: float = 10.0,
    format: str = "json",
    errorformat: str = "json",
    search_option = 'PARCEL',
    filter_option = False
) -> dict[str, Any]:
    """
    Query the vworld Search API for address information.

    Parameters
    ----------
    address:
        Human-readable address string (도로명 or 지번).
    api_key:
        vworld issued API key.
    category:
        Deprecated. The helper always searches ``"ROAD"`` first and falls back to
        ``"PARCEL"`` automatically when no results are returned.
    crs:
        Coordinate reference system to receive results in (e.g. ``"EPSG:4326"``).
    size:
        Number of results to request (1-1000).
    page:
        Page number to request (>=1).
    bbox:
        Optional bounding box (minx, miny, maxx, maxy) to spatially constrain the search.
    domain:
        Optional domain parameter to include in the request.
    timeout:
        Socket timeout (seconds) passed to ``urllib.request.urlopen``.
    format:
        Response format requested from the API. Only ``"json"`` is supported by this helper.
    errorformat:
        Error response format. Only ``"json"`` is supported by this helper.
    search_option:
        Internal option to control the search category. Normally, the function
        searches ``"ROAD"`` first and falls back to ``"PARCEL"`` if no results
        are found. This parameter can be used to force a specific category.
    filter_option:
        If ``True``, the returned results are filtered to ensure exact matches

    Returns
    -------
    dict[str, Any]
        Parsed JSON ``response`` block from the Search API.

    Raises
    ------
    ValueError
        If arguments are malformed (e.g. empty address, invalid size/page).
    VWorldAPIError
        If the API request fails or returns a non-JSON payload.
    """
    _ = category  # Kept for compatibility; actual search order is ROAD then PARCEL.


    if not address or not address.strip():
        raise ValueError("address must be a non-empty string.")
    if not api_key or not api_key.strip():
        raise ValueError("api_key must be provided.")
    if size < 1 or size > 1000:
        raise ValueError("size must be between 1 and 1000.")
    if page < 1:
        raise ValueError("page must be greater than or equal to 1.")
    if timeout <= 0:
        raise ValueError("timeout must be greater than zero.")
    if format.lower() != "json":
        raise ValueError("only JSON format responses are supported by this helper.")
    if errorformat.lower() != "json":
        raise ValueError("only JSON errorformat responses are supported by this helper.")
    query_params: dict[str, Any] = {
        "service": "search",
        "request": "search",
        "version": "2.0",
        "format": format,
        "errorformat": errorformat,
        "type": "address",
        "crs": crs,
        "size": size,
        "page": page,
        "query": address.strip(),
        "key": api_key.strip(),
    }

    if bbox is not None:
        if len(bbox) != 4:
            raise ValueError("bbox must contain exactly four values: minx, miny, maxx, maxy.")
        query_params["bbox"] = ",".join(str(value) for value in bbox)

    if domain:
        query_params['domain'] = domain

    road_hint = bool(
        re.search(r"\d+\s*(?:로|길|번길)", address)
        or re.search(r"[^\d\s]+(?:로|길)\s*\d", address)
    )

    requested_category = str(search_option).upper() if search_option else 'PARCEL'
    if road_hint:
        requested_category = 'ROAD'
    if requested_category not in {'ROAD', 'PARCEL'}:
        requested_category = 'PARCEL'

    primary_category = requested_category
    search_response = _perform_address_search_request(query_params, primary_category, timeout)

    if search_response.get('status') == 'NOT_FOUND':
        fallback_category = 'PARCEL' if primary_category == 'ROAD' else 'ROAD'
        search_response = _perform_address_search_request(query_params, fallback_category, timeout)
        primary_category = fallback_category

    if filter_option and search_response.get('status') == 'OK':
        result_block = search_response.get('result')
        items = []
        if isinstance(result_block, Mapping):
            items = result_block.get('items', [])
        filtered_items = _perform_validated_address_search_request(
            address,
            'road' if primary_category == 'ROAD' else 'parcel',
            items,
        )
        if isinstance(result_block, Mapping):
            result_block['items'] = filtered_items

    return search_response



__all__ = [
    "call_vworld_api",
    "get_vworld_api_info",
    "VWorldAPIError",
    "search_address",
]
