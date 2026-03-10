"""
Base adapter interface — all DB adapters implement this.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseAdapter(ABC):
    """
    Every adapter implements these four methods.
    Conduit doesn't care what's underneath — Postgres, Mongo, Salesforce — 
    it just calls these.
    """

    def __init__(self, connection_string: str, name: str):
        self.connection_string = connection_string
        self.name = name
        self.adapter_type = self.__class__.__name__.replace("Adapter", "").lower()

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def introspect_schema(self) -> dict:
        """
        Return a normalized schema description.

        Returns:
            {
                "tables": {
                    "table_name": {
                        "columns": [{"name": str, "type": str, "nullable": bool}],
                        "row_count_estimate": int,
                        "primary_key": str | None,
                        "foreign_keys": [{"column": str, "references": str}]
                    }
                },
                "views": { ... },  # same structure
                "adapter": str,
                "database": str
            }
        """
        pass

    @abstractmethod
    def get_query_history(self, limit: int = 500) -> list[dict]:
        """
        Return recent query history for context mining.

        Returns:
            [
                {
                    "query": str,           # the SQL / query text
                    "execution_count": int, # how many times run
                    "avg_duration_ms": float,
                    "last_run": str         # ISO timestamp
                }
            ]
        """
        pass

    @abstractmethod
    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        """
        Execute a query and return rows as list of dicts.
        For non-SQL sources (Mongo, Salesforce), adapters translate internally.
        """
        pass

    def test_connection(self) -> bool:
        """Optional health check. Override for custom logic."""
        try:
            self.connect()
            return True
        except Exception:
            return False
