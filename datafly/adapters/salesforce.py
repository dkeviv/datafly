"""
Salesforce adapter — uses describe() API for rich schema extraction.
Salesforce's describe() is actually excellent for context: it returns
field labels, picklist values, and relationship metadata.
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class SalesforceAdapter(BaseAdapter):

    # Core objects to introspect (add more as needed)
    DEFAULT_OBJECTS = [
        "Account", "Contact", "Lead", "Opportunity",
        "Case", "Product2", "Pricebook2", "Contract",
        "Campaign", "Task", "Event"
    ]

    def __init__(self, connection_string: str, name: str,
                 username: str = "", password: str = "", token: str = ""):
        super().__init__(connection_string, name)
        self.username = username
        self.password = password
        self.token = token
        self._sf = None

    def connect(self) -> None:
        from simple_salesforce import Salesforce
        self._sf = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.token
        )
        logger.info(f"[{self.name}] Connected to Salesforce")

    def introspect_schema(self) -> dict:
        """
        describe() returns field labels, types, relationships, picklist values.
        Far richer than a SQL information_schema — great for context building.
        """
        tables = {}
        for obj_name in self.DEFAULT_OBJECTS:
            try:
                desc = getattr(self._sf, obj_name).describe()
                fields = []
                for f in desc["fields"]:
                    field_info = {
                        "name": f["name"],
                        "label": f["label"],          # human-readable name
                        "type": f["type"],
                        "nullable": f["nillable"],
                    }
                    # Capture picklist values — very useful for business context
                    if f["type"] == "picklist" and f.get("picklistValues"):
                        field_info["picklist_values"] = [
                            p["value"] for p in f["picklistValues"] if p["active"]
                        ]
                    # Capture relationship names
                    if f.get("relationshipName"):
                        field_info["relationship"] = f["relationshipName"]

                    fields.append(field_info)

                tables[obj_name] = {
                    "columns": fields,
                    "label": desc.get("label", obj_name),
                    "row_count_estimate": self._get_count(obj_name),
                    "primary_key": "Id",
                    "foreign_keys": [
                        {"column": f["name"], "references": f.get("referenceTo", [""])[0]}
                        for f in desc["fields"]
                        if f["type"] in ("reference",) and f.get("referenceTo")
                    ]
                }
            except Exception as e:
                logger.warning(f"[{self.name}] Could not describe {obj_name}: {e}")

        return {
            "adapter": self.name,
            "adapter_type": "salesforce",
            "tables": tables,
            "views": {}
        }

    def _get_count(self, obj_name: str) -> int:
        try:
            result = self._sf.query(f"SELECT COUNT() FROM {obj_name}")
            return result.get("totalSize", 0)
        except Exception:
            return 0

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """Salesforce doesn't expose query history via API — return empty."""
        logger.info(f"[{self.name}] Salesforce query history not available via API")
        return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        """Execute SOQL query."""
        result = self._sf.query_all(query)
        records = result.get("records", [])
        # Strip Salesforce metadata fields
        return [
            {k: v for k, v in r.items() if not k.startswith("attributes")}
            for r in records
        ]
