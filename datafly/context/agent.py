"""
Datafly Context Creation Agent.

Supports two LLM providers:
  - Anthropic  — via anthropic SDK (ANTHROPIC_API_KEY)
  - OpenRouter — via OpenAI-compatible API (OPENROUTER_API_KEY)

Auto-detects provider from which key is present in environment.
"""

from __future__ import annotations
import json
import logging
import os
from datafly.context.store import ContextStore

logger = logging.getLogger(__name__)


def _strip_markdown_static(text: str) -> str:
    """Remove markdown code fences from LLM output."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # drop opening fence line (```sql, ```json, etc.)
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


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

PLAN_SYSTEM = """You are a data analyst planning how to answer a business question from a database.

Given the schema and business context, reason step by step about:
1. Which tables are needed and why
2. Which columns map to the concepts in the question
3. What joins, filters, aggregations or ordering are required
4. Any data format quirks visible in sample values (e.g. date formats, enum values, booleans)
5. Any ambiguities or assumptions you're making

Output a short, clear reasoning plan. Be specific about table names, column names, and exact values from the samples. This plan will be used to write the query."""

SQL_GEN_SYSTEM = """You are an expert data query generator. You are given a reasoning plan and must produce a single, correct query.

Rules:
- Plain table names only — never prefix with schema or adapter name
- Use exact column values shown in sample_values — if samples show 'FY2025-Q1', use that exact format
- For SQL: return only valid SQL. No explanation. No markdown. No code fences.
- For MongoDB: return JSON only with keys: operation, collection, filter, projection, limit
- For DynamoDB: return JSON only with keys: operation, table, key/filter, limit
- For HubSpot/Salesforce: return JSON only with keys: object, filters, properties, limit"""

REFLECT_SYSTEM = """You are debugging a failed database query. Reason carefully about what went wrong and produce a corrected query.

You will be given:
- The original question
- The schema with real sample values
- The query that was attempted
- The error or empty result it produced

Reason step by step:
1. What did the failed query do wrong? (wrong table, bad filter, format mismatch, wrong join, etc.)
2. What is the correct approach?
3. Write the corrected query

Output ONLY the corrected query. No explanation. No markdown. No code fences."""


def _detect_provider() -> str:
    """
    Auto-detect LLM provider from available environment keys.
    Priority: explicit DATAFLY_LLM_PROVIDER > key presence detection
    """
    explicit = os.getenv("DATAFLY_LLM_PROVIDER", "").strip()
    if explicit:
        return explicit

    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    openrouter_key = os.getenv("OPENROUTER_API_KEY", "").strip()

    # Ignore placeholder values from .env.example
    placeholder = "your_anthropic_key_here"
    anthropic_key = "" if anthropic_key == placeholder else anthropic_key
    placeholder_or = "your_openrouter_key_here"
    openrouter_key = "" if openrouter_key == placeholder_or else openrouter_key

    if anthropic_key and openrouter_key:
        return "anthropic"  # both set — prefer Anthropic
    if openrouter_key:
        return "openrouter"
    if anthropic_key:
        return "anthropic"

    raise ValueError(
        "No LLM API key found. Set one of:\n"
        "  ANTHROPIC_API_KEY=your_key\n"
        "  OPENROUTER_API_KEY=your_key\n"
        "in your .env file."
    )


def _get_llm_client():
    provider = _detect_provider()
    logger.info(f"LLM provider: {provider}")
    if provider == "openrouter":
        return OpenRouterClient()
    return AnthropicClient()


class AnthropicClient:
    """Anthropic Claude via official SDK."""

    def __init__(self):
        import anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = os.getenv("DATAFLY_ANTHROPIC_MODEL", "claude-sonnet-4-6")
        logger.info(f"Anthropic client ready — model: {self.model}")

    def chat(self, system: str, user: str, max_tokens: int = 4000) -> str:
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}]
        )
        return response.content[0].text.strip()


class OpenRouterClient:
    """OpenRouter via OpenAI-compatible API."""

    def __init__(self):
        from openai import OpenAI
        api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
        self._client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
        )
        self.model = os.getenv("DATAFLY_OPENROUTER_MODEL", "anthropic/claude-sonnet-4-6")
        logger.info(f"OpenRouter client ready — model: {self.model}")

    def chat(self, system: str, user: str, max_tokens: int = 4000) -> str:
        response = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ],
            extra_headers={
                "HTTP-Referer": "https://datafly.dev",
                "X-Title": "Datafly"
            }
        )
        return response.choices[0].message.content.strip()


class ContextAgent:

    def __init__(self):
        self._llm = None  # lazy init — only create when first used

    @property
    def llm(self):
        if self._llm is None:
            self._llm = _get_llm_client()
        return self._llm

    def build(self, schemas: dict, query_history: dict, adapter_names: list[str]) -> dict:
        provider = _detect_provider()
        logger.info(f"Context Agent: building from {len(schemas)} adapter(s) using {provider}...")

        analysis_input = self._prepare_input(schemas, query_history, adapter_names)
        raw = self.llm.chat(system=CONTEXT_BUILD_SYSTEM, user=analysis_input, max_tokens=4000)

        # Strip accidental markdown fences
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
                       context: dict, adapter_type: str,
                       examples: list[dict] | None = None,
                       executor=None,
                       max_attempts: int = 3) -> str:
        """
        Agentic query generation: Plan → Generate → Execute → Reflect → Retry.

        If executor (a callable that runs a query and returns rows or raises) is provided,
        the agent will self-correct on errors or empty results up to max_attempts times.
        Without an executor it falls back to single-shot generation.
        """
        schema_summary = self._summarize_schema(schema)
        context_summary = self._summarize_context_for_question(context, question)
        adapter_hint = self._adapter_hint(adapter_type)
        examples_block = self._examples_block(examples)

        base_context = f"""Schema (with real sample values):
{schema_summary}

Business Context:
{context_summary}
{examples_block}
{adapter_hint}

Question: {question}"""

        # ── Step 1: Plan ──────────────────────────────────────────────────────
        plan = self.llm.chat(system=PLAN_SYSTEM, user=base_context, max_tokens=600)
        logger.debug(f"[Plan]\n{plan}")

        # ── Step 2: Generate from plan ────────────────────────────────────────
        gen_prompt = f"""{base_context}

Reasoning plan:
{plan}

Now write the query."""
        query = self.llm.chat(system=SQL_GEN_SYSTEM, user=gen_prompt, max_tokens=800)
        query = _strip_markdown_static(query)
        logger.debug(f"[Query attempt 1]\n{query}")

        if executor is None:
            return query  # no executor — return best-effort

        # ── Step 3: Execute → Reflect → Retry ────────────────────────────────
        last_error = None
        last_rows = None

        for attempt in range(1, max_attempts + 1):
            try:
                rows = executor(query)
                last_rows = rows

                if rows:
                    logger.info(f"[Agent] Query succeeded on attempt {attempt} ({len(rows)} rows)")
                    return query  # ✓ success

                # Empty result — reflect and retry
                issue = "The query executed without error but returned 0 rows."
                logger.info(f"[Agent] Empty result on attempt {attempt} — reflecting...")

            except Exception as e:
                last_error = str(e)
                issue = f"The query raised an error: {last_error}"
                logger.info(f"[Agent] Error on attempt {attempt}: {last_error} — reflecting...")

            if attempt == max_attempts:
                break  # no more retries

            reflect_prompt = f"""Original question: {question}

Schema (with real sample values):
{schema_summary}

Business context:
{context_summary}

Query attempted:
{query}

Problem: {issue}

Reason carefully about what went wrong, then write a corrected query."""

            query = self.llm.chat(system=REFLECT_SYSTEM, user=reflect_prompt, max_tokens=800)
            query = _strip_markdown_static(query)
            logger.debug(f"[Query attempt {attempt + 1}]\n{query}")

        logger.warning(f"[Agent] All {max_attempts} attempts exhausted. Returning last query.")
        return query

    def _adapter_hint(self, adapter_type: str) -> str:
        if adapter_type in ("mongodb", "dynamodb"):
            return f"\nTarget: {adapter_type.upper()} — return JSON query object, NOT SQL."
        if adapter_type in ("salesforce", "hubspot"):
            return f"\nTarget: {adapter_type.upper()} CRM API — return JSON search object, NOT SQL."
        return ""

    def _examples_block(self, examples: list[dict] | None) -> str:
        if not examples:
            return ""
        lines = ["\nExamples of past successful queries on this database:"]
        for ex in examples:
            lines.append(f"  Q: {ex['question']}")
            lines.append(f"  SQL: {ex['sql_query']}")
            lines.append("")
        lines.append("Use these as reference for table names, join patterns, and value formats.")
        return "\n".join(lines)

    def apply_feedback(self, correction: str, context_store: ContextStore) -> None:
        context = context_store.load()
        prompt = f"""Current context layer:
{json.dumps(context, indent=2, default=str)}

Human correction: {correction}

Update the context to incorporate this correction.
Return the complete updated context JSON only. No explanation."""

        raw = self.llm.chat(
            system="Update the context layer JSON based on the correction. Return ONLY valid JSON.",
            user=prompt,
            max_tokens=4000
        )
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
        parts.append("IMPORTANT: Infer all business meaning, formats, and rules from the actual sample values below. Do not guess formats — use what the data shows.\n")
        for adapter_name, schema in schemas.items():
            parts.append(f"\n=== ADAPTER: {adapter_name} ({schema.get('adapter_type','unknown')}) ===")
            for tname, tinfo in list(schema.get("tables", {}).items())[:30]:
                parts.append(f"\nTable: {tname} (~{tinfo.get('row_count_estimate','?')} rows)")
                for col in tinfo.get("columns", [])[:20]:
                    col_str = f"  - {col['name']} ({col['type']})"
                    if col.get("sample_values"):
                        col_str += f"  ← real values: {col['sample_values']}"
                    parts.append(col_str)
                fks = [f"{f['column']}→{f['references']}" for f in tinfo.get("foreign_keys", [])]
                if fks:
                    parts.append(f"  FKs: {', '.join(fks)}")
            history = query_history.get(adapter_name, [])
            if history:
                parts.append(f"\n--- Top queries ({adapter_name}) ---")
                for q in history[:15]:
                    parts.append(f"  {q.get('query','')[:200]}")
        return "\n".join(parts)

    def _summarize_schema(self, schema: dict) -> str:
        """Schema summary for SQL generation — includes real sample values so
        the LLM uses correct formats (e.g. 'FY2025-Q1') not guessed ones."""
        lines = []
        for tname, tinfo in schema.get("tables", {}).items():
            lines.append(f"Table: {tname}")
            for col in tinfo.get("columns", [])[:15]:
                col_str = f"  {col['name']} ({col['type']})"
                if col.get("sample_values"):
                    col_str += f"  e.g. {col['sample_values']}"
                lines.append(col_str)
        return "\n".join(lines)

    def _summarize_context_for_question(self, context: dict, question: str) -> str:
        q_lower = question.lower()
        lines = []
        all_defs = {**context.get("entities", {}), **context.get("metrics", {})}
        for name, defn in all_defs.items():
            aliases = [a.lower() for a in defn.get("aliases", [])]
            if name.lower() in q_lower or any(a in q_lower for a in aliases):
                lines.append(
                    f"  {name}: {defn.get('description','')} → {defn.get('source_of_truth','')}"
                    + (f" | formula: {defn['formula']}" if defn.get("formula") else "")
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
                    "aliases": [], "confidence": 0.5, "review_flag": True
                }
        return {
            "entities": entities, "metrics": {}, "routing_rules": [],
            "relationships": [], "tribal_knowledge": [],
            "review_required": list(entities.keys())
        }
