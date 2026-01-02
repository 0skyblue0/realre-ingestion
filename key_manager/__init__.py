"""
Utilities for managing API keys and encrypted secrets across multiple services.

The :class:`KeyManager` in this module keeps secrets in memory, can persist them
to disk as JSON, and optionally encrypt values using a user provided passphrase.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import binascii
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping, MutableMapping, Optional

__all__ = [
    "KeyManager",
    "KeyManagerError",
    "IntegrityError",
    "generate_passphrase",
    "encrypt_value",
    "decrypt_value",
]

_SALT_BYTES = 16
_PBKDF_ROUNDS = 390_000


class KeyManagerError(RuntimeError):
    """Base error for key manager operations."""


class IntegrityError(KeyManagerError):
    """Raised when encrypted payloads fail integrity checks."""


def generate_passphrase(length: int = 32) -> str:
    """Return a random URL-safe passphrase."""
    if length <= 0:
        raise ValueError("length must be positive")
    raw = secrets.token_urlsafe(length)
    return raw[:length]


def _derive_keys(passphrase: str, salt: bytes) -> tuple[bytes, bytes]:
    material = hashlib.pbkdf2_hmac(
        "sha256",
        passphrase.encode("utf-8"),
        salt,
        _PBKDF_ROUNDS,
        dklen=64,
    )
    return material[:32], material[32:]


def _xor_bytes(data: bytes, key: bytes) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


def encrypt_value(value: str, passphrase: str) -> str:
    """
    Encrypt *value* with *passphrase* and return a base64 payload.

    The function uses PBKDF2 derivation combined with an XOR stream cipher and
    a keyed HMAC for integrity.
    """
    if not passphrase:
        raise ValueError("passphrase is required for encryption")
    salt = secrets.token_bytes(_SALT_BYTES)
    enc_key, mac_key = _derive_keys(passphrase, salt)
    plaintext = value.encode("utf-8")
    ciphertext = _xor_bytes(plaintext, enc_key)
    mac = hmac.new(mac_key, salt + ciphertext, hashlib.sha256).digest()
    payload = salt + mac + ciphertext
    return base64.urlsafe_b64encode(payload).decode("ascii")


def decrypt_value(payload: str, passphrase: str) -> str:
    """Inverse of :func:`encrypt_value`."""
    if not passphrase:
        raise ValueError("passphrase is required for decryption")
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
    except (ValueError, binascii.Error) as exc:
        raise KeyManagerError("invalid payload encoding") from exc
    if len(decoded) < _SALT_BYTES + hashlib.sha256().digest_size:
        raise KeyManagerError("payload too short to contain metadata")
    salt = decoded[:_SALT_BYTES]
    mac = decoded[_SALT_BYTES : _SALT_BYTES + hashlib.sha256().digest_size]
    ciphertext = decoded[_SALT_BYTES + hashlib.sha256().digest_size :]
    enc_key, mac_key = _derive_keys(passphrase, salt)
    expected_mac = hmac.new(mac_key, salt + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected_mac):
        raise IntegrityError("ciphertext integrity check failed")
    plaintext = _xor_bytes(ciphertext, enc_key)
    return plaintext.decode("utf-8")


def _normalize_path(path: Optional[os.PathLike[str] | str]) -> Optional[Path]:
    if path is None:
        return None
    resolved = Path(path).expanduser().resolve()
    return resolved


@dataclass
class _Entry:
    value: str
    encrypted: bool


@dataclass
class KeyManager:
    """
    Manage API keys and secrets with optional JSON persistence.

    Parameters
    ----------
    storage_path:
        File path where secrets are stored as JSON. When ``None`` the manager
        retains everything in memory only.
    passphrase:
        Passphrase used to encrypt values. When omitted, values are persisted
        in plain text.
    auto_persist:
        Persist changes automatically whenever keys are modified.
    """

    storage_path: Optional[Path] = None
    passphrase: Optional[str] = None
    auto_persist: bool = False
    _entries: Dict[str, _Entry] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.storage_path is not None:
            self.storage_path = _normalize_path(self.storage_path)
        if self.storage_path and self.storage_path.exists():
            self._load_from_disk()

    # --------------------------------------------------------------------- API
    def list_keys(self) -> list[str]:
        """Return a sorted list of managed key identifiers."""
        return sorted(self._entries.keys())

    def get(self, name: str, default: Optional[str] = None, *, raw: bool = False) -> Optional[str]:
        """
        Retrieve a secret by *name*.

        Parameters
        ----------
        default:
            Returned when the key is not found.
        raw:
            When ``True`` returns the stored representation without decrypting.
        """
        entry = self._entries.get(name)
        if entry is None:
            return default
        if raw or not entry.encrypted:
            return entry.value

        if self.passphrase is None:
            raise KeyManagerError("passphrase is required to decrypt stored value")
        return decrypt_value(entry.value, self.passphrase)

    def set(
        self,
        name: str,
        value: str,
        *,
        encrypt: Optional[bool] = None,
        persist: Optional[bool] = None,
    ) -> None:
        """
        Store a secret.

        Parameters
        ----------
        encrypt:
            When ``None`` the manager encrypts automatically whenever a
            passphrase is configured.
        persist:
            Override the manager wide auto persist behaviour.
        """
        if encrypt is None:
            encrypt = self.passphrase is not None
        stored_value = value
        if encrypt:
            if self.passphrase is None:
                raise KeyManagerError("cannot encrypt without a passphrase")
            stored_value = encrypt_value(value, self.passphrase)
        self._entries[name] = _Entry(value=stored_value, encrypted=bool(encrypt))
        self._maybe_persist(persist)

    def delete(self, name: str, *, persist: Optional[bool] = None) -> bool:
        """Remove a secret; returns ``True`` when a value was removed."""
        removed = self._entries.pop(name, None)
        if removed is not None:
            self._maybe_persist(persist)
            return True
        return False

    def bulk_set(
        self,
        values: Mapping[str, str],
        *,
        encrypt: Optional[bool] = None,
        persist: Optional[bool] = None,
    ) -> None:
        """Insert multiple secrets in a single call."""
        for name, value in values.items():
            self.set(name, value, encrypt=encrypt, persist=False)
        self._maybe_persist(persist)

    def import_from_env(
        self,
        env_map: Mapping[str, str],
        *,
        encrypt: Optional[bool] = None,
        persist: Optional[bool] = None,
        missing: str = "skip",
    ) -> list[str]:
        """
        Import secrets from environment variables.

        Parameters
        ----------
        env_map:
            Mapping of secret names to environment variable identifiers.
        missing:
            ``"skip"`` ignores missing environment variables while ``"error"``
            raises :class:`KeyManagerError`.
        """
        imported: list[str] = []
        for name, env_var in env_map.items():
            value = os.environ.get(env_var)
            if value is None:
                if missing == "error":
                    raise KeyManagerError(f"environment variable '{env_var}' is not set")
                continue
            self.set(name, value, encrypt=encrypt, persist=False)
            imported.append(name)
        self._maybe_persist(persist)
        return imported

    def export_to_env(
        self,
        env_map: Mapping[str, str],
        *,
        raw: bool = False,
        overwrite: bool = False,
    ) -> list[str]:
        """
        Export tracked secrets to environment variables.

        Parameters
        ----------
        raw:
            When ``True`` writes the stored representation without decrypting.
        overwrite:
            Replace existing environment variables when ``True``.
        """
        exported: list[str] = []
        for name, env_var in env_map.items():
            if not overwrite and env_var in os.environ:
                continue
            value = self.get(name, raw=raw)
            if value is None:
                continue
            os.environ[env_var] = value
            exported.append(env_var)
        return exported

    def load_from_disk(self) -> None:
        """Force a reload from the storage path."""
        self._load_from_disk()

    def save_to_disk(self) -> None:
        """Persist current entries to disk."""
        self._save_to_disk()

    # ----------------------------------------------------------------- helpers
    def _load_from_disk(self) -> None:
        if not self.storage_path:
            return
        if not self.storage_path.exists():
            return
        try:
            raw = self.storage_path.read_text(encoding="utf-8")
            payload = json.loads(raw)
        except OSError as exc:
            raise KeyManagerError(f"failed to read storage: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise KeyManagerError("storage file contains invalid JSON") from exc
        if not isinstance(payload, MutableMapping):
            raise KeyManagerError("storage file must contain a JSON object")
        new_entries: Dict[str, _Entry] = {}
        for name, entry in payload.items():
            if not isinstance(entry, MutableMapping):
                continue
            value = entry.get("value")
            encrypted = entry.get("encrypted", False)
            if not isinstance(value, str):
                continue
            new_entries[str(name)] = _Entry(value=value, encrypted=bool(encrypted))
        self._entries = new_entries

    def _save_to_disk(self) -> None:
        if not self.storage_path:
            return
        payload = {
            name: {"value": entry.value, "encrypted": entry.encrypted}
            for name, entry in self._entries.items()
        }
        try:
            if self.storage_path.parent and not self.storage_path.parent.exists():
                self.storage_path.parent.mkdir(parents=True, exist_ok=True)
            self.storage_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            raise KeyManagerError(f"failed to write storage: {exc}") from exc

    def _maybe_persist(self, override: Optional[bool]) -> None:
        should_persist = self.auto_persist if override is None else override
        if should_persist:
            self._save_to_disk()
