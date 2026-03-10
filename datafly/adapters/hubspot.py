"""
HubSpot adapter — uses Properties API for schema, CRM API for data.
HubSpot's Properties API returns field labels, types, and options — 
very rich for context building, similar to Salesforce describe().
"""

from __future__ import annotations
import logging
import requests
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"

# Core HubSpot CRM objects to introspect
DEFAULT_OBJECTS = [
    "contacts",
    "companies",
    "deals",
    "tickets",
    "products",
    "line_items",
    "quotes",
    "meetings",
    "calls",
    "emails",
]


class HubSpotAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str, access_token: str = ""):
        super().__init__(connection_string, name)
        # Accept token from connection string: hubspot://access_token
        if connection_string.startswith("hubspot://"):
            self._token = connection_string.replace("hubspot://", "")
        else:
            self._token = access_token
        self._session = None

    def connect(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        })
        # Verify connection
        resp = self._session.get(f"{HUBSPOT_BASE}/crm/v3/objects/contacts?limit=1")
        resp.raise_for_status()
        logger.info(f"[{self.name}] Connected to HubSpot")

    def introspect_schema(self) -> dict:
        tables: dict = {}

        for obj_name in DEFAULT_OBJECTS:
            try:
                props = self._get_properties(obj_name)
                count = self._get_count(obj_name)

                columns = []
                for prop in props:
                    col = {
                        "name": prop["name"],
                        "label": prop.get("label", prop["name"]),
                        "type": prop.get("type", "string"),
                        "nullable": True,
                        "hubspot_type": prop.get("fieldType", "")
                    }
                    # Enumeration options — great for context (pipeline stages, etc.)
                    if prop.get("options"):
                        col["options"] = [
                            {"value": o["value"], "label": o["label"]}
                            for o in prop["options"]
                            if not o.get("hidden", False)
                        ]
                    # Description from HubSpot
                    if prop.get("description"):
                        col["description"] = prop["description"]
                    columns.append(col)

                # Identify standard identifier fields
                pk_map = {
                    "contacts": "hs_object_id",
                    "companies": "hs_object_id",
                    "deals": "hs_object_id",
                    "tickets": "hs_object_id",
                }

                tables[obj_name] = {
                    "columns": columns,
                    "row_count_estimate": count,
                    "primary_key": pk_map.get(obj_name, "hs_object_id"),
                    "foreign_keys": self._get_associations(obj_name),
                    "label": obj_name.replace("_", " ").title()
                }

            except Exception as e:
                logger.warning(f"[{self.name}] Could not introspect {obj_name}: {e}")

        return {
            "adapter": self.name,
            "adapter_type": "hubspot",
            "tables": tables,
            "views": {}
        }

    def _get_properties(self, obj_name: str) -> list[dict]:
        resp = self._session.get(
            f"{HUBSPOT_BASE}/crm/v3/properties/{obj_name}",
            params={"archived": False}
        )
        resp.raise_for_status()
        return resp.json().get("results", [])

    def _get_count(self, obj_name: str) -> int:
        try:
            resp = self._session.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/{obj_name}/search",
                json={"limit": 1, "properties": ["hs_object_id"]}
            )
            return resp.json().get("total", 0)
        except Exception:
            return 0

    def _get_associations(self, obj_name: str) -> list[dict]:
        """Return standard HubSpot associations as foreign key equivalents."""
        assoc_map = {
            "contacts": [
                {"column": "hs_object_id", "references": "companies.hs_object_id"},
                {"column": "hs_object_id", "references": "deals.hs_object_id"},
            ],
            "deals": [
                {"column": "hs_object_id", "references": "companies.hs_object_id"},
                {"column": "hs_object_id", "references": "contacts.hs_object_id"},
            ],
            "tickets": [
                {"column": "hs_object_id", "references": "contacts.hs_object_id"},
                {"column": "hs_object_id", "references": "companies.hs_object_id"},
            ],
        }
        return assoc_map.get(obj_name, [])

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """HubSpot doesn't expose query/search history via API."""
        logger.info(f"[{self.name}] HubSpot query history not available via API")
        return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        """
        Execute a HubSpot search query passed as JSON.
        Format: {
            "object": "contacts",
            "filters": [{"propertyName": "...", "operator": "EQ", "value": "..."}],
            "properties": ["firstname", "lastname", "email"],
            "limit": 100
        }
        """
        import json
        q = json.loads(query)
        obj = q.get("object", "contacts")

        payload = {
            "filterGroups": [{"filters": q.get("filters", [])}],
            "properties": q.get("properties", []),
            "limit": min(q.get("limit", 100), 100)
        }

        resp = self._session.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/{obj}/search",
            json=payload
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return [r.get("properties", {}) for r in results]
