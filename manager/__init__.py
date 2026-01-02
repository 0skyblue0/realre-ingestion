"""
Manager package orchestrating schedules, jobs, clients, and persistence.

The public entrypoint is :class:`manager.core.IngestionManager` which is
leveraged by ``Ingestion_Manager.py`` and :mod:`manager.run`.
"""

from .core import IngestionManager

__all__ = ["IngestionManager"]
