"""
DynamoDB adapter — samples items per table to infer schema.
DynamoDB has no fixed schema, so like MongoDB we sample and infer.
Table describe() gives us key schema, GSIs, and capacity — all useful context.
"""

from __future__ import annotations
import logging
from datafly.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

SAMPLE_SIZE = 100  # Items to scan per table for schema inference


class DynamoDBAdapter(BaseAdapter):

    def __init__(self, connection_string: str, name: str,
                 region: str = "us-east-1",
                 aws_access_key_id: str = "",
                 aws_secret_access_key: str = ""):
        super().__init__(connection_string, name)
        # Parse region from connection string: dynamodb://region
        if connection_string.startswith("dynamodb://"):
            self._region = connection_string.replace("dynamodb://", "").split("/")[0] or region
        else:
            self._region = region
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._resource = None
        self._client = None

    def connect(self) -> None:
        import boto3
        kwargs = {"region_name": self._region}
        if self._aws_access_key_id:
            kwargs["aws_access_key_id"] = self._aws_access_key_id
            kwargs["aws_secret_access_key"] = self._aws_secret_access_key

        self._resource = boto3.resource("dynamodb", **kwargs)
        self._client = boto3.client("dynamodb", **kwargs)
        logger.info(f"[{self.name}] Connected to DynamoDB: {self._region}")

    def introspect_schema(self) -> dict:
        tables: dict = {}

        # List all tables
        paginator = self._client.get_paginator("list_tables")
        all_table_names = []
        for page in paginator.paginate():
            all_table_names.extend(page["TableNames"])

        for table_name in all_table_names:
            try:
                desc = self._client.describe_table(TableName=table_name)["Table"]
                table = self._resource.Table(table_name)

                # Sample items to infer attribute schema
                scan_result = table.scan(Limit=SAMPLE_SIZE)
                items = scan_result.get("Items", [])
                inferred_cols = self._infer_attributes(items, desc)

                # Key schema
                key_schema = {k["AttributeName"]: k["KeyType"] for k in desc.get("KeySchema", [])}
                pk = next((k for k, v in key_schema.items() if v == "HASH"), None)
                sk = next((k for k, v in key_schema.items() if v == "RANGE"), None)

                # GSIs — tell us a lot about query patterns
                gsis = []
                for gsi in desc.get("GlobalSecondaryIndexes", []):
                    gsi_keys = {k["AttributeName"]: k["KeyType"] for k in gsi.get("KeySchema", [])}
                    gsis.append({
                        "name": gsi["IndexName"],
                        "hash_key": next((k for k, v in gsi_keys.items() if v == "HASH"), None),
                        "range_key": next((k for k, v in gsi_keys.items() if v == "RANGE"), None),
                        "projection": gsi.get("Projection", {}).get("ProjectionType")
                    })

                tables[table_name] = {
                    "columns": inferred_cols,
                    "row_count_estimate": desc.get("ItemCount", 0),
                    "primary_key": pk,
                    "sort_key": sk,
                    "foreign_keys": [],
                    "global_secondary_indexes": gsis,
                    "size_bytes": desc.get("TableSizeBytes", 0),
                    "billing_mode": desc.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
                    "sample_item": self._safe_sample(items)
                }

            except Exception as e:
                logger.warning(f"[{self.name}] Could not introspect table {table_name}: {e}")

        return {
            "adapter": self.name,
            "adapter_type": "dynamodb",
            "region": self._region,
            "tables": tables,
            "views": {}
        }

    def _infer_attributes(self, items: list[dict], desc: dict) -> list[dict]:
        """Infer attribute names and types from sampled items + attribute definitions."""
        attr_map: dict[str, set] = {}

        # Start with defined attributes (from key schema)
        for attr in desc.get("AttributeDefinitions", []):
            type_map = {"S": "String", "N": "Number", "B": "Binary"}
            attr_map[attr["AttributeName"]] = {type_map.get(attr["AttributeType"], attr["AttributeType"])}

        # Expand from sampled items
        for item in items:
            for key, val in item.items():
                if key not in attr_map:
                    attr_map[key] = set()
                attr_map[key].add(type(val).__name__)

        return [
            {"name": k, "type": " | ".join(sorted(v)), "nullable": True}
            for k, v in attr_map.items()
        ]

    def _safe_sample(self, items: list[dict]) -> dict:
        if not items:
            return {}
        item = dict(items[0])
        return {k: (str(v)[:50] if len(str(v)) > 50 else v)
                for k, v in list(item.items())[:8]}

    def get_query_history(self, limit: int = 500) -> list[dict]:
        """
        DynamoDB has no native query history API.
        Could be pulled from CloudWatch if enabled — stub for now.
        """
        logger.info(f"[{self.name}] DynamoDB query history requires CloudWatch Logs integration")
        return []

    def execute(self, query: str, params: dict | None = None) -> list[dict]:
        """
        Execute a DynamoDB operation passed as JSON.
        Format: {"operation": "scan"|"query"|"get_item", "table": "...", ...}
        
        For context-aware queries, the LLM will generate this JSON format
        instead of SQL when routing to DynamoDB.
        """
        import json
        op = json.loads(query)
        table = self._resource.Table(op["table"])

        operation = op.get("operation", "scan")
        if operation == "scan":
            result = table.scan(
                FilterExpression=op.get("filter_expression"),
                Limit=op.get("limit", 100)
            ) if op.get("filter_expression") else table.scan(Limit=op.get("limit", 100))
            return result.get("Items", [])

        elif operation == "query":
            from boto3.dynamodb.conditions import Key
            result = table.query(
                KeyConditionExpression=Key(op["key"]).eq(op["value"]),
                Limit=op.get("limit", 100)
            )
            return result.get("Items", [])

        elif operation == "get_item":
            result = table.get_item(Key=op["key"])
            item = result.get("Item")
            return [item] if item else []

        raise ValueError(f"Unknown DynamoDB operation: {operation}")
