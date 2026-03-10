"""
Adapter factory — creates the right adapter from a connection string prefix.
"""
from __future__ import annotations
from datafly.adapters.base import BaseAdapter

# kept at module level for backwards compat
_ADAPTER_MAP = {
    "postgres":     "datafly.adapters.postgres.PostgresAdapter",
    "postgresql":   "datafly.adapters.postgres.PostgresAdapter",
    "snowflake":    "datafly.adapters.snowflake.SnowflakeAdapter",
    "bigquery":     "datafly.adapters.bigquery.BigQueryAdapter",
    "redshift":     "datafly.adapters.redshift.RedshiftAdapter",
    "mongodb":      "datafly.adapters.mongo.MongoAdapter",
    "mongodb+srv":  "datafly.adapters.mongo.MongoAdapter",
    "salesforce":   "datafly.adapters.salesforce.SalesforceAdapter",
    "dynamodb":     "datafly.adapters.dynamodb.DynamoDBAdapter",
    "hubspot":      "datafly.adapters.hubspot.HubSpotAdapter",
}


class AdapterFactory:
    ADAPTER_MAP = _ADAPTER_MAP  # expose as class attribute for tests

    @staticmethod
    def create(connection_string: str, name: str) -> BaseAdapter:
        prefix = connection_string.split("://")[0].lower()
        adapter_path = _ADAPTER_MAP.get(prefix)
        if not adapter_path:
            supported = list(_ADAPTER_MAP.keys())
            raise ValueError(
                f"No adapter for prefix '{prefix}'. "
                f"Supported: {supported}"
            )
        module_path, class_name = adapter_path.rsplit(".", 1)
        import importlib
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        return cls(connection_string=connection_string, name=name)

    @staticmethod
    def supported() -> list[str]:
        return list(_ADAPTER_MAP.keys())
