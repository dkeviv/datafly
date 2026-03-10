"""
MongoDB adapter — samples documents to infer schema (no fixed schema).
This is the interesting one: LLMs are great at inferring meaning from sampled docs.
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class MongoAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str):
        super().__init__(connection_string, name)
        self._client = None
        self._db = None

    def connect(self) -> None:
        from pymongo import MongoClient
        self._client = MongoClient(self.connection_string)
        # Extract DB name from connection string or use default
        db_name = self.connection_string.split("/")[-1].split("?")[0] or "default"
        self._db = self._client[db_name]
        logger.info(f"[{self.name}] Connected to MongoDB: {db_name}")

    def introspect_schema(self) -> dict:
        """
        Sample documents from each collection to infer field types.
        Uses 100-doc sample — enough for LLM to infer business meaning.
        """
        collections = {}
        for coll_name in self._db.list_collection_names():
            sample_docs = list(self._db[coll_name].find().limit(100))
            field_types = self._infer_fields(sample_docs)
            count = self._db[coll_name].estimated_document_count()
            collections[coll_name] = {
                "columns": field_types,
                "row_count_estimate": count,
                "primary_key": "_id",
                "foreign_keys": [],
                "sample_doc": self._safe_sample(sample_docs)
            }

        return {
            "adapter": self.name,
            "adapter_type": "mongodb",
            "tables": collections,  # normalized to same key as SQL adapters
            "views": {}
        }

    def _infer_fields(self, docs: list[dict]) -> list[dict]:
        """Infer field names + types from sampled documents."""
        field_map: dict[str, set] = {}
        for doc in docs:
            for key, val in doc.items():
                if key not in field_map:
                    field_map[key] = set()
                field_map[key].add(type(val).__name__)

        return [
            {"name": k, "type": " | ".join(sorted(v)), "nullable": True}
            for k, v in field_map.items()
        ]

    def _safe_sample(self, docs: list[dict]) -> dict:
        """Return a single sanitized sample document for context."""
        if not docs:
            return {}
        doc = dict(docs[0])
        doc.pop("_id", None)
        # Truncate long string values
        return {k: (str(v)[:50] if isinstance(v, str) and len(str(v)) > 50 else v)
                for k, v in list(doc.items())[:10]}

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """Mine system.profile if profiling is enabled."""
        try:
            profile_docs = list(
                self._db["system.profile"]
                .find({"op": {"$in": ["query", "find"]}})
                .sort("ts", -1)
                .limit(limit)
            )
            return [
                {
                    "query": str(doc.get("query", doc.get("command", {}))),
                    "execution_count": 1,
                    "avg_duration_ms": doc.get("millis", 0),
                    "last_run": str(doc.get("ts", ""))
                }
                for doc in profile_docs
            ]
        except Exception:
            logger.warning(f"[{self.name}] system.profile not available")
            return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        """
        Execute a MongoDB query passed as a JSON string.
        Format: {"collection": "orders", "filter": {...}, "limit": 100}
        """
        import json
        q = json.loads(query)
        coll = self._db[q["collection"]]
        cursor = coll.find(
            q.get("filter", {}),
            q.get("projection", None)
        ).limit(q.get("limit", 100))
        return [
            {k: v for k, v in doc.items() if k != "_id"}
            for doc in cursor
        ]
