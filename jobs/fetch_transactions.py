from __future__ import annotations

from typing import Any


def run(
    *,
    manager: Any,
    source: str = "mock",
    client_method: str = "fetch_transactions",
    limit: int = 5,
    scd_table: str = "transactions_scd",
) -> dict[str, Any]:
    """
    Example job that pulls transactions from a client and persists them via SCD2.
    """
    records = manager.request_source_data(source, client_method, limit=limit)
    if not isinstance(records, list):
        records = []
    manager.log_history(
        job_name="fetch_transactions",
        event_type="data_load",
        status="success",
        row_count=len(records),
        details={"source": source, "method": client_method},
    )
    inserted = manager.upsert_scd2(
        table=scd_table,
        records=records,
        key_fields=["tx_id"],
        attribute_fields=["amount", "currency", "updated_at"],
    )
    manager.log_history(
        job_name="fetch_transactions",
        event_type="scd2_upsert",
        status="success",
        row_count=inserted,
        details={"table": scd_table},
    )
    return {"row_count": inserted, "table": scd_table}
