from __future__ import annotations

from datetime import datetime, timezone
from random import randint, random
from typing import Any


def fetch_transactions(limit: int = 5, **_: Any) -> list[dict[str, Any]]:
    """
    Generate deterministic-ish mock transactions for offline testing.
    """
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    return [
        {
            "tx_id": f"mock-{i}",
            "amount": round(random() * 1000, 2),
            "currency": "KRW",
            "updated_at": now,
        }
        for i in range(limit)
    ]
