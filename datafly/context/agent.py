"""
Datafly Context Creation Agent — builds semantic model from schema + query history.
"""

from __future__ import annotations
import json
import logging
import anthropic
from datafly.context.store import ContextStore

logger = logging.getLogger(__name__)
client = anthropic.Anthropic()

CONTEXT_BUILD_SYSTEM = """You are a data architect analyzing database schemas and query history
to build a semantic context layer for AI data agents.

Extract business meaning from table/column names, foreign keys, and past SQL queries.
Output ONLY valid JSON. No explanation, no markdown, no code fences.

Required JSON structure:
{
  "entities": {
    "entity_name": {
      "description": "business meaning",
      "source_of_truth": "adapter_name.table_name",
      "aliases": ["alt names"],
      "primary_key": "col",
      "key_columns": ["col1","col2"],
      "confidence": 0.95
    }
  },
  "metrics": {
    "metric_name": {
      "description": "business definition",
      "source_of_truth": "adapter_name.table_name",
      "formula": "SQL expression",
      "filters": ["standard WHERE clauses"],
      "aliases": ["ARR","MRR"],
      "confidence": 0.91,
      "review_flag": false
    }
  },
  "routing_rules": [
    {
      "pattern": "revenue*",
      "adapter": "adapter_name",
      "table": "table_name",
      "reason": "why this rule exists"
    }
  ],
  "relationships": [
    {
      "from": "entity_a",
      "to": "entity_b",
      "join": "a.id = b.a_id",
      "confidence": 0.95
    }
  ],
  "tribal_knowledge": [
    "plain English business rules inferred from query patterns"
  ],
  "review_required": ["list entity/metric names with confidence below 0.7"]
}

Confidence rules:
- 0.9+: Clear naming, consistent usage, explicit FK
- 0.7-0.9: Reasonable inference, some ambiguity
- <0.7: Ambiguous or conflicting — always set review_flag: true"""

SQL_GEN_SYSTEM = """You are an expert SQL and data query generator.
Generate a single query to answer the user's question using the provided schema and business context.
- For SQL databases: return valid SQL
- For MongoDB: return a JSON object with keys: operation, collection, filter, projection, limit
- For DynamoDB: return a JSON object with keys: operation, table, key/filter, limit
- For HubSpot/Salesforce: return a JSON object with keys: object, filters, properties, limit
Return ONLY the query. No explanation."""


class ContextAgent:

    def build(self, schemas: dict, query_history: dict, adapter_names: list[str]) -> dict:
        logger.info("Context Agent: analyzing schemas and query history...")
        analysis_input = self._prepare_input(schemas, query_history, adapter_names)

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            system=CONTEXT_BUILD_SYSTEM,
            messages=[{"role": "user", "content": analysis_input}]
        )

        raw = response.content[0].text.strip()
        # Strip any accidental markdown fences
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        try:
            context = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(f"Context Agent JSON parse failed: {e}")
            context = self._fallback_context(schemas, adapter_names)

        context = self._apply_confidence_flags(context)
        logger.info(
            f"Context built — entities: {len(context.get('entities', {}))}, "
            f"metrics: {len(context.get('metrics', {}))}, "
            f"review required: {len(context.get('review_required', []))}"
        )
        return context

    def generate_query(self, question: str, schema: dict,
                       context: dict, adapter_type: str) -> str:
        """Generate an appropriate query (SQL or JSON) for the target adapter."""
        schema_summary = self._summarize_schema(schema)
        context_summary = self._summarize_context_for_question(context, question)

        adapter_hint = ""
        if adapter_type in ("mongodb", "dynamodb"):
            adapter_hint = f"\nTarget: {adapter_type.upper()} — return JSON query object, NOT SQL."
        elif adapter_type in ("salesforce", "hubspot"):
            adapter_hint = f"\nTarget: {adapter_type.upper()} CRM API — return JSON search object, NOT SQL."

        prompt = f"""Schema:
{schema_summary}

Business Context:
{context_summary}
{adapter_hint}

Question: {question}"""

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=SQL_GEN_SYSTEM,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()

    def apply_feedback(self, correction: str, context_store: ContextStore) -> None:
        context = context_store.load()
        prompt = f"""Current context layer:
{json.dumps(context, indent=2, default=str)}

Human correction: {correction}

Update the context to incorporate this correction.
Return the complete updated context JSON only. No explanation."""

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            system="Update the context layer JSON based on the correction. Return ONLY valid JSON.",
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1].lstrip("json").strip()
        try:
            updated = json.loads(raw)
            context_store.save(updated, source="feedback")
            logger.info("Feedback applied to context layer")
        except json.JSONDecodeError:
            logger.error("Feedback failed — LLM returned invalid JSON")

    def _prepare_input(self, schemas: dict, query_history: dict, adapter_names: list) -> str:
        parts = [f"Build a semantic context layer for these data sources: {', '.join(adapter_names)}\n"]
        for adapter_name, schema in schemas.items():
            parts.append(f"\n=== ADAPTER: {adapter_name} ({schema.get('adapter_type','unknown')}) ===")
            for tname, tinfo in list(schema.get("tables", {}).items())[:30]:
                cols = [f"{c['name']}({c['type']})" for c in tinfo.get("columns", [])[:20]]
                fks = [f"{f['column']}→{f['references']}" for f in tinfo.get("foreign_keys", [])]
                row_count = tinfo.get("row_count_estimate", "?")
                parts.append(f"\nTable: {tname} (~{row_count} rows)")
                parts.append(f"  Columns: {', '.join(cols)}")
                if fks:
                    parts.append(f"  FKs: {', '.join(fks)}")
                # Include column descriptions/labels if present (Salesforce, HubSpot, BQ)
                labeled = [c for c in tinfo.get("columns", []) if c.get("description") or c.get("label")]
                if labeled[:3]:
                    parts.append(f"  Labels: " + ", ".join(
                        f"{c['name']}='{c.get('label') or c.get('description', '')}'"
                        for c in labeled[:3]
                    ))

            history = query_history.get(adapter_name, [])
            if history:
                parts.append(f"\n--- Top queries ({adapter_name}) ---")
                for q in history[:15]:
                    parts.append(f"  {q.get('query', '')[:200]}")

        return "\n".join(parts)

    def _summarize_schema(self, schema: dict) -> str:
        lines = []
        for tname, tinfo in schema.get("tables", {}).items():
            cols = [c['name'] for c in tinfo.get("columns", [])]
            lines.append(f"  {tname}: {', '.join(cols[:15])}")
        return "\n".join(lines)

    def _summarize_context_for_question(self, context: dict, question: str) -> str:
        q_lower = question.lower()
        lines = []
        all_defs = {**context.get("entities", {}), **context.get("metrics", {})}
        for name, defn in all_defs.items():
            aliases = [a.lower() for a in defn.get("aliases", [])]
            if name.lower() in q_lower or any(a in q_lower for a in aliases):
                lines.append(
                    f"  {name}: {defn.get('description','')} "
                    f"→ {defn.get('source_of_truth','')}"
                    + (f" | formula: {defn['formula']}" if defn.get('formula') else "")
                )
        for rule in context.get("tribal_knowledge", []):
            words = rule.lower().split()[:4]
            if any(w in q_lower for w in words):
                lines.append(f"  Rule: {rule}")
        return "\n".join(lines) if lines else "No specific context matched — use schema directly."

    def _apply_confidence_flags(self, context: dict) -> dict:
        review = []
        for section in ("entities", "metrics"):
            for name, defn in context.get(section, {}).items():
                if defn.get("confidence", 1.0) < 0.7:
                    defn["review_flag"] = True
                    if name not in review:
                        review.append(name)
        context["review_required"] = review
        return context

    def _fallback_context(self, schemas: dict, adapter_names: list) -> dict:
        entities = {}
        for adapter_name, schema in schemas.items():
            for tname in schema.get("tables", {}):
                entities[tname] = {
                    "description": f"Table {tname} from {adapter_name}",
                    "source_of_truth": f"{adapter_name}.{tname}",
                    "aliases": [],
                    "confidence": 0.5,
                    "review_flag": True
                }
        return {
            "entities": entities, "metrics": {}, "routing_rules": [],
            "relationships": [], "tribal_knowledge": [],
            "review_required": list(entities.keys())
        }
