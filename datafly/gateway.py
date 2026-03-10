"""
Datafly — Universal Data Gateway
"""

from __future__ import annotations
import uuid
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class Datafly:
    """
    Main entry point.

    Usage:
        from datafly import Datafly

        df = Datafly()
        df.connect("postgres://user:pass@localhost/mydb", name="prod")
        df.build_context()
        result = df.query("What was revenue last quarter?")
    """

    def __init__(self,
                 context_yaml_path: str = "datafly/context/context.yaml",
                 context_db_url: str = "",
                 context_backend: str = "hybrid",
                 config=None):

        # Support config object OR explicit params
        if config:
            context_yaml_path = config.context_yaml_path
            context_db_url = config.context_db_url
            context_backend = config.context_backend

        from datafly.adapters.base import BaseAdapter
        from datafly.context.agent import ContextAgent
        from datafly.context.store import ContextStore

        self.adapters: dict[str, BaseAdapter] = {}
        self.context_store = ContextStore(
            yaml_path=context_yaml_path,
            db_url=context_db_url,
            backend=context_backend
        )
        self.context_agent = ContextAgent()
        self._query_log: list[dict] = []

    @classmethod
    def from_env(cls, dotenv_path: str = ".env") -> "Datafly":
        """Convenience constructor — loads all config from environment."""
        from datafly.config import DataflyConfig
        config = DataflyConfig.from_env(dotenv_path)
        return cls(config=config)

    def connect(self, connection_string: str, name: str) -> None:
        """Register and connect a data source."""
        from datafly.adapters.factory import AdapterFactory
        adapter = AdapterFactory.create(connection_string, name)
        adapter.connect()
        self.adapters[name] = adapter
        logger.info(f"Connected: {name} ({adapter.adapter_type})")

    def build_context(self, force_rebuild: bool = False) -> dict:
        """
        Introspect all connected adapters and build the semantic context layer.
        Runs the LLM-powered Context Creation Agent.
        """
        if self.context_store.exists() and not force_rebuild:
            logger.info("Context exists. Pass force_rebuild=True to regenerate.")
            return self.context_store.load()

        if not self.adapters:
            raise RuntimeError("No adapters connected. Call connect() first.")

        logger.info(f"Building context from {len(self.adapters)} adapter(s)...")

        schemas, query_history = {}, {}
        for name, adapter in self.adapters.items():
            try:
                logger.info(f"  Introspecting {name}...")
                schemas[name] = adapter.introspect_schema()
            except Exception as e:
                logger.warning(f"  Schema introspection failed for {name}: {e}")
                schemas[name] = {"adapter": name, "adapter_type": adapter.adapter_type,
                                 "tables": {}, "views": {}}
            try:
                query_history[name] = adapter.get_query_history(limit=500)
            except Exception as e:
                logger.warning(f"  Query history unavailable for {name}: {e}")
                query_history[name] = []

        context = self.context_agent.build(
            schemas=schemas,
            query_history=query_history,
            adapter_names=list(self.adapters.keys())
        )
        self.context_store.save(context)
        return context

    def query(self, question: str, adapter_hint: str | None = None) -> dict:
        """
        Route a natural language question through the context-aware gateway.
        Returns result rows + metadata (adapter used, SQL generated, context applied).
        """
        context = self.context_store.load()
        router = QueryRouter(self.adapters, context, self.context_agent)
        result = router.route(question, adapter_hint)

        # Store for feedback loop
        query_id = str(uuid.uuid4())[:8]
        result["query_id"] = query_id
        self._query_log.append({
            "query_id": query_id,
            "timestamp": datetime.utcnow().isoformat(),
            "question": question,
            "adapter": result.get("adapter"),
            "success": result.get("success"),
            "query_generated": result.get("query")
        })
        return result

    def feedback(self, query_id: str, correction: str) -> None:
        """
        Submit a correction for a previous query.
        The correction is applied to the context layer immediately.
        """
        original = next(
            (q for q in self._query_log if q.get("query_id") == query_id), None
        )
        if original:
            full_correction = (
                f"For the question '{original['question']}', "
                f"the generated query was wrong. Correction: {correction}"
            )
        else:
            full_correction = correction

        self.context_agent.apply_feedback(full_correction, self.context_store)
        logger.info(f"Feedback applied for query {query_id}")

    def status(self) -> dict:
        """Quick health check — adapters, context state, review queue."""
        review_items = []
        context_version = None
        if self.context_store.exists():
            try:
                ctx = self.context_store.load()
                review_items = ctx.get("review_required", [])
                context_version = ctx.get("_meta", {}).get("version")
            except Exception:
                pass
        return {
            "adapters": {
                name: {"type": a.adapter_type, "connected": True}
                for name, a in self.adapters.items()
            },
            "context": {
                "exists": self.context_store.exists(),
                "version": context_version,
                "backend": self.context_store.backend,
                "review_items": len(review_items),
            }
        }

    def serve(self, host: str = "0.0.0.0", port: int = 8000) -> None:
        from datafly.api.server import create_app
        import uvicorn
        app = create_app(self)
        uvicorn.run(app, host=host, port=port)

    def serve_mcp(self, port: int = 8080) -> None:
        from datafly.api.mcp import MCPServer
        MCPServer(self).serve(port=port)


class QueryRouter:
    """Routes questions to the right adapter with context injection."""

    def __init__(self, adapters: dict, context: dict, agent):
        self.adapters = adapters
        self.context = context
        self.agent = agent

    def route(self, question: str, adapter_hint: str | None) -> dict:
        adapter_name = adapter_hint or self._resolve_adapter(question)
        adapter = self.adapters.get(adapter_name)

        if not adapter:
            available = list(self.adapters.keys())
            return {
                "success": False,
                "error": f"Adapter '{adapter_name}' not found. Available: {available}"
            }

        enriched = self._inject_context(question)

        try:
            schema = adapter.introspect_schema()
        except Exception:
            schema = {"tables": {}}

        query = self.agent.generate_query(
            question=enriched,
            schema=schema,
            context=self.context,
            adapter_type=adapter.adapter_type
        )

        try:
            rows = adapter.execute(query)
            return {
                "success": True,
                "question": question,
                "adapter": adapter_name,
                "adapter_type": adapter.adapter_type,
                "query": query,
                "rows": rows,
                "row_count": len(rows),
                "context_applied": self._matched_context(question)
            }
        except Exception as e:
            return {
                "success": False,
                "question": question,
                "adapter": adapter_name,
                "query": query,
                "error": str(e)
            }

    def _resolve_adapter(self, question: str) -> str:
        q_lower = question.lower()
        rules = self.context.get("routing_rules", [])
        best_match = None
        best_score = 0
        for rule in rules:
            pattern = rule.get("pattern", "").rstrip("*").lower()
            if pattern and pattern in q_lower:
                score = len(pattern)
                if score > best_score:
                    best_score = score
                    best_match = rule.get("adapter")
        if best_match and best_match in self.adapters:
            return best_match
        if not self.adapters:
            raise RuntimeError("No adapters connected.")
        default = next(iter(self.adapters))
        if len(self.adapters) > 1:
            logger.warning(
                f"No routing rule matched '{question[:50]}'. "
                f"Defaulting to '{default}'. Consider adding a routing rule."
            )
        return default

    def _inject_context(self, question: str) -> str:
        q_lower = question.lower()
        relevant = []
        all_defs = {
            **self.context.get("entities", {}),
            **self.context.get("metrics", {})
        }
        for name, defn in all_defs.items():
            aliases = [a.lower() for a in defn.get("aliases", [])]
            if name.lower() in q_lower or any(a in q_lower for a in aliases):
                sot = defn.get("source_of_truth", "")
                filters = defn.get("filters", [])
                relevant.append(
                    f"- '{name}': {defn.get('description','')} "
                    f"(source: {sot})"
                    + (f" filters: {'; '.join(filters)}" if filters else "")
                    + (f" formula: {defn['formula']}" if defn.get("formula") else "")
                )
        for rule in self.context.get("tribal_knowledge", []):
            words = rule.lower().split()[:4]
            if any(w in q_lower for w in words):
                relevant.append(f"- Rule: {rule}")
        if not relevant:
            return question
        ctx_block = "\n".join(relevant)
        return f"Business context:\n{ctx_block}\n\nQuestion: {question}"

    def _matched_context(self, question: str) -> list[str]:
        q_lower = question.lower()
        return [
            name for name in {
                **self.context.get("entities", {}),
                **self.context.get("metrics", {})
            }
            if name.lower() in q_lower
        ]
