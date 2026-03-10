"""
Datafly core tests — runnable without real DB connections.
Uses mocks for adapters and a temp YAML store.
"""

import json
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import MagicMock, patch


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_yaml(tmp_path):
    return str(tmp_path / "context.yaml")


@pytest.fixture
def sample_context():
    return {
        "entities": {
            "customer": {
                "description": "An active customer",
                "source_of_truth": "postgres.dim_customer",
                "aliases": ["client", "account"],
                "primary_key": "customer_id",
                "confidence": 0.95
            }
        },
        "metrics": {
            "revenue": {
                "description": "Annual Recurring Revenue",
                "source_of_truth": "snowflake.fct_revenue",
                "formula": "SUM(arr) WHERE status='active'",
                "aliases": ["ARR", "MRR"],
                "filters": ["status = 'active'"],
                "confidence": 0.91,
                "review_flag": False
            },
            "churn_rate": {
                "description": "Monthly churn percentage",
                "source_of_truth": "snowflake.fct_churn",
                "confidence": 0.60,
                "review_flag": True
            }
        },
        "routing_rules": [
            {"pattern": "revenue*", "adapter": "snowflake", "table": "fct_revenue",
             "reason": "source of truth for revenue"},
            {"pattern": "customer*", "adapter": "postgres", "table": "dim_customer",
             "reason": "operational customer data"},
        ],
        "tribal_knowledge": [
            "Revenue uses fiscal quarters ending November 30",
            "Exclude test accounts (@test.company.com) from all user metrics"
        ],
        "review_required": ["churn_rate"]
    }


@pytest.fixture
def mock_adapter():
    adapter = MagicMock()
    adapter.adapter_type = "postgres"
    adapter.name = "test_postgres"
    adapter.introspect_schema.return_value = {
        "adapter": "test_postgres",
        "adapter_type": "postgres",
        "tables": {
            "dim_customer": {
                "columns": [
                    {"name": "customer_id", "type": "integer", "nullable": False},
                    {"name": "company_name", "type": "text", "nullable": True},
                    {"name": "status", "type": "text", "nullable": True},
                ],
                "row_count_estimate": 5000,
                "primary_key": "customer_id",
                "foreign_keys": []
            },
            "fct_revenue": {
                "columns": [
                    {"name": "customer_id", "type": "integer", "nullable": False},
                    {"name": "arr", "type": "numeric", "nullable": False},
                    {"name": "fiscal_quarter", "type": "text", "nullable": True},
                ],
                "row_count_estimate": 20000,
                "primary_key": None,
                "foreign_keys": [{"column": "customer_id", "references": "dim_customer.customer_id"}]
            }
        },
        "views": {}
    }
    adapter.get_query_history.return_value = [
        {"query": "SELECT SUM(arr) FROM fct_revenue WHERE status='active'",
         "execution_count": 150, "avg_duration_ms": 230, "last_run": "2025-01-10"},
        {"query": "SELECT * FROM dim_customer WHERE status='active'",
         "execution_count": 89, "avg_duration_ms": 45, "last_run": "2025-01-11"},
    ]
    adapter.execute.return_value = [
        {"customer_id": 1, "company_name": "Acme Corp", "arr": 50000},
        {"customer_id": 2, "company_name": "Globex", "arr": 120000},
    ]
    return adapter


# ── Context Store Tests ───────────────────────────────────────────────────────

class TestContextStore:

    def test_save_and_load_yaml(self, tmp_yaml, sample_context):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        store.save(sample_context)
        loaded = store.load()
        assert "entities" in loaded
        assert "revenue" in loaded["metrics"]
        assert loaded["_meta"]["version"] == 1

    def test_version_increments(self, tmp_yaml, sample_context):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        store.save(sample_context)
        store.save(sample_context)
        loaded = store.load()
        assert loaded["_meta"]["version"] == 2

    def test_exists_false_when_empty(self, tmp_yaml):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        assert not store.exists()

    def test_exists_true_after_save(self, tmp_yaml, sample_context):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        store.save(sample_context)
        assert store.exists()

    def test_load_raises_when_missing(self, tmp_yaml):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        with pytest.raises(FileNotFoundError):
            store.load()

    def test_approve_removes_from_review(self, tmp_yaml, sample_context):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        store.save(sample_context)
        assert "churn_rate" in store.get_review_items()
        store.approve("churn_rate")
        assert "churn_rate" not in store.get_review_items()

    def test_add_tribal_knowledge(self, tmp_yaml, sample_context):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        store.save(sample_context)
        store.add_tribal_knowledge("New business rule for test")
        loaded = store.load()
        assert "New business rule for test" in loaded["tribal_knowledge"]

    def test_no_duplicate_tribal_knowledge(self, tmp_yaml, sample_context):
        from datafly.context.store import ContextStore
        store = ContextStore(yaml_path=tmp_yaml, db_url="", backend="yaml")
        store.save(sample_context)
        rule = "Duplicate rule"
        store.add_tribal_knowledge(rule)
        store.add_tribal_knowledge(rule)
        loaded = store.load()
        assert loaded["tribal_knowledge"].count(rule) == 1


# ── Query Router Tests ────────────────────────────────────────────────────────

class TestQueryRouter:

    def _make_router(self, adapters, context, agent=None):
        from datafly.gateway import QueryRouter
        if agent is None:
            agent = MagicMock()
            agent.generate_query.return_value = "SELECT SUM(arr) FROM fct_revenue"
        return QueryRouter(adapters, context, agent)

    def test_routes_by_pattern(self, sample_context, mock_adapter):
        router = self._make_router(
            {"snowflake": mock_adapter, "postgres": mock_adapter},
            sample_context
        )
        # "revenue" should match "revenue*" rule → snowflake
        adapter_name = router._resolve_adapter("What was revenue last quarter?")
        assert adapter_name == "snowflake"

    def test_routes_customer_to_postgres(self, sample_context, mock_adapter):
        router = self._make_router(
            {"snowflake": mock_adapter, "postgres": mock_adapter},
            sample_context
        )
        adapter_name = router._resolve_adapter("How many active customers do we have?")
        assert adapter_name == "postgres"

    def test_context_injected_for_revenue_query(self, sample_context, mock_adapter):
        router = self._make_router({"postgres": mock_adapter}, sample_context)
        enriched = router._inject_context("What was revenue last quarter?")
        assert "ARR" in enriched or "fct_revenue" in enriched or "Annual Recurring Revenue" in enriched

    def test_successful_query_returns_rows(self, sample_context, mock_adapter):
        agent = MagicMock()
        agent.generate_query.return_value = "SELECT * FROM fct_revenue LIMIT 10"
        router = self._make_router({"postgres": mock_adapter}, sample_context, agent)
        result = router.route("Show me revenue data", adapter_hint="postgres")
        assert result["success"] is True
        assert len(result["rows"]) == 2
        assert result["adapter"] == "postgres"

    def test_failed_query_returns_error(self, sample_context, mock_adapter):
        mock_adapter.execute.side_effect = Exception("Table not found")
        agent = MagicMock()
        agent.generate_query.return_value = "SELECT * FROM nonexistent"
        router = self._make_router({"postgres": mock_adapter}, sample_context, agent)
        result = router.route("Show me data", adapter_hint="postgres")
        assert result["success"] is False
        assert "error" in result

    def test_missing_adapter_returns_error(self, sample_context, mock_adapter):
        router = self._make_router({"postgres": mock_adapter}, sample_context)
        result = router.route("test question", adapter_hint="nonexistent")
        assert result["success"] is False


# ── Adapter Factory Tests ─────────────────────────────────────────────────────

class TestAdapterFactory:

    def test_postgres_prefix(self):
        from datafly.adapters.factory import AdapterFactory
        from datafly.adapters.postgres import PostgresAdapter
        # Just test class resolution, don't actually connect
        adapter_path = "datafly.adapters.postgres.PostgresAdapter"
        assert "postgres" in AdapterFactory.ADAPTER_MAP

    def test_unsupported_prefix_raises(self):
        from datafly.adapters.factory import AdapterFactory
        with pytest.raises(ValueError, match="No adapter for prefix"):
            AdapterFactory.create("mysql://user:pass@host/db", "test")

    def test_all_expected_adapters_registered(self):
        from datafly.adapters.factory import AdapterFactory
        expected = ["postgres", "snowflake", "bigquery", "redshift",
                    "mongodb", "salesforce", "dynamodb", "hubspot"]
        for prefix in expected:
            assert prefix in AdapterFactory.ADAPTER_MAP, f"Missing adapter: {prefix}"


# ── Datafly Gateway Tests ─────────────────────────────────────────────────────

class TestDatafly:

    def test_connect_stores_adapter(self, tmp_yaml, mock_adapter):
        from datafly.gateway import Datafly
        with patch("datafly.adapters.factory.AdapterFactory.create", return_value=mock_adapter):
            df = Datafly(context_yaml_path=tmp_yaml, context_db_url="", context_backend="yaml")
            df.connect("postgres://localhost/test", "test")
            assert "test" in df.adapters

    def test_build_context_requires_adapter(self, tmp_yaml):
        from datafly.gateway import Datafly
        df = Datafly(context_yaml_path=tmp_yaml, context_db_url="", context_backend="yaml")
        with pytest.raises(RuntimeError, match="No adapters connected"):
            df.build_context()

    def test_query_returns_query_id(self, tmp_yaml, sample_context, mock_adapter):
        from datafly.gateway import Datafly
        df = Datafly(context_yaml_path=tmp_yaml, context_db_url="", context_backend="yaml")
        df.context_store.save(sample_context)

        agent_mock = MagicMock()
        agent_mock.generate_query.return_value = "SELECT * FROM fct_revenue"
        df.context_agent = agent_mock
        df.adapters["postgres"] = mock_adapter

        result = df.query("Show me revenue", adapter_hint="postgres")
        assert "query_id" in result
        assert len(result["query_id"]) > 0

    def test_status_shows_adapters(self, tmp_yaml, mock_adapter):
        from datafly.gateway import Datafly
        df = Datafly(context_yaml_path=tmp_yaml, context_db_url="", context_backend="yaml")
        df.adapters["postgres"] = mock_adapter
        status = df.status()
        assert "postgres" in status["adapters"]
        assert status["context"]["exists"] is False

    def test_feedback_logs_correction(self, tmp_yaml, sample_context, mock_adapter):
        from datafly.gateway import Datafly
        df = Datafly(context_yaml_path=tmp_yaml, context_db_url="", context_backend="yaml")
        df.context_store.save(sample_context)
        df.adapters["postgres"] = mock_adapter

        # Seed a query in the log
        df._query_log.append({
            "query_id": "abc123",
            "question": "What was revenue?",
            "adapter": "postgres",
            "success": True,
            "query_generated": "SELECT * FROM wrong_table"
        })

        agent_mock = MagicMock()
        df.context_agent = agent_mock

        df.feedback("abc123", "Use fct_revenue not orders table")
        agent_mock.apply_feedback.assert_called_once()
        call_args = str(agent_mock.apply_feedback.call_args)
        assert "revenue" in call_args or "fct_revenue" in call_args


# ── REST API Tests ────────────────────────────────────────────────────────────

class TestAPI:

    @pytest.fixture
    def client(self, tmp_yaml, sample_context, mock_adapter):
        from fastapi.testclient import TestClient
        from datafly.api.server import create_app
        from datafly.gateway import Datafly

        df = Datafly(context_yaml_path=tmp_yaml, context_db_url="", context_backend="yaml")
        df.context_store.save(sample_context)
        df.adapters["postgres"] = mock_adapter

        agent_mock = MagicMock()
        agent_mock.generate_query.return_value = "SELECT * FROM fct_revenue LIMIT 10"
        df.context_agent = agent_mock

        app = create_app(df)
        return TestClient(app)

    def test_health_endpoint(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "adapters" in data
        assert "context" in data

    def test_get_context(self, client):
        resp = client.get("/context")
        assert resp.status_code == 200
        data = resp.json()
        assert "entities" in data
        assert "metrics" in data

    def test_get_review_items(self, client):
        resp = client.get("/context/review")
        assert resp.status_code == 200
        assert "churn_rate" in resp.json()["items"]

    def test_approve_entity(self, client):
        resp = client.post("/context/approve/churn_rate")
        assert resp.status_code == 200

    def test_query_endpoint(self, client):
        resp = client.post("/query", json={"question": "Show me revenue", "adapter_hint": "postgres"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert "rows" in data

    def test_add_tribal_knowledge(self, client):
        resp = client.post("/context/tribal-knowledge",
                           json={"rule": "Always exclude test accounts"})
        assert resp.status_code == 200
