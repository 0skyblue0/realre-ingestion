"""
Job registry mapping names to callables consumed by the ingestion manager.
"""

from . import fetch_transactions

JOB_REGISTRY = {
    "fetch_transactions": fetch_transactions.run,
}

__all__ = ["JOB_REGISTRY", "fetch_transactions"]
