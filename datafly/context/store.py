"""
Datafly Context Store — Hybrid YAML + Postgres backend.

Architecture:
  - YAML file = source of truth (human-readable, Git-trackable, PR-reviewable)
  - Postgres = runtime cache (fast reads, versioned rows, queryable history)

If no Postgres URL configured, falls back to YAML-only mode.
"""

from __future__ import annotations
import json
import yaml
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS datafly_context (
    id          SERIAL PRIMARY KEY,
    version     INTEGER NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    content     JSONB NOT NULL,
    source      TEXT DEFAULT 'agent',
    is_active   BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS datafly_context_log (
    id          SERIAL PRIMARY KEY,
    created_at  TIMESTAMP DEFAULT NOW(),
    event       TEXT NOT NULL,
    entity_name TEXT,
    detail      JSONB
);
"""


class ContextStore:

    def __init__(self, yaml_path: str = "datafly/context/context.yaml",
                 db_url: str = "", backend: str = "hybrid"):
        self.yaml_path = Path(yaml_path) if yaml_path else None
        self.db_url = db_url
        self.backend = backend
        self._db_conn = None
        if yaml_path:
            Path(yaml_path).parent.mkdir(parents=True, exist_ok=True)
        if db_url:
            self._init_db()

    def _init_db(self) -> None:
        try:
            import psycopg2
            self._db_conn = psycopg2.connect(self.db_url)
            self._db_conn.autocommit = True
            with self._db_conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
            logger.info("Context schema ready in Postgres")
        except Exception as e:
            logger.warning(f"Context DB unavailable: {e}. Using YAML-only.")
            self._db_conn = None

    def _db_available(self) -> bool:
        return self._db_conn is not None

    def exists(self) -> bool:
        if self._db_available():
            with self._db_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM datafly_context WHERE is_active = TRUE LIMIT 1")
                return cur.fetchone() is not None
        if self.yaml_path:
            return self.yaml_path.exists()
        return False

    def save(self, context: dict, source: str = "agent") -> None:
        version = self._next_version()
        context["_meta"] = {
            "generated_at": datetime.utcnow().isoformat(),
            "version": version,
            "source": source,
            "backend": self.backend
        }
        if self.backend in ("yaml", "hybrid") and self.yaml_path:
            self._save_yaml(context)
        if self.backend in ("postgres", "hybrid") and self._db_available():
            self._save_db(context, version, source)
        logger.info(f"Context v{version} saved [{self.backend}]")

    def load(self) -> dict:
        if self._db_available() and self.backend in ("postgres", "hybrid"):
            ctx = self._load_db()
            if ctx:
                return ctx
        if self.yaml_path and self.yaml_path.exists():
            return self._load_yaml()
        raise FileNotFoundError(
            "No context found. Run datafly.build_context() first.\n"
            "Set DATAFLY_CONTEXT_DB_URL or ensure YAML path is writable."
        )

    def _save_yaml(self, context: dict) -> None:
        if self.yaml_path.exists():
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            shutil.copy(self.yaml_path, self.yaml_path.parent / f"context_{ts}.yaml")
        with open(self.yaml_path, "w") as f:
            yaml.dump(context, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    def _load_yaml(self) -> dict:
        with open(self.yaml_path) as f:
            return yaml.safe_load(f) or {}

    def _save_db(self, context: dict, version: int, source: str) -> None:
        with self._db_conn.cursor() as cur:
            cur.execute("UPDATE datafly_context SET is_active = FALSE WHERE is_active = TRUE")
            cur.execute(
                "INSERT INTO datafly_context (version, content, source, is_active) VALUES (%s, %s, %s, TRUE)",
                (version, json.dumps(context), source)
            )
            cur.execute(
                "INSERT INTO datafly_context_log (event, detail) VALUES (%s, %s)",
                ("build", json.dumps({"version": version, "source": source}))
            )

    def _load_db(self) -> Optional[dict]:
        try:
            with self._db_conn.cursor() as cur:
                cur.execute("SELECT content FROM datafly_context WHERE is_active = TRUE LIMIT 1")
                row = cur.fetchone()
                if row:
                    return row[0] if isinstance(row[0], dict) else json.loads(row[0])
        except Exception as e:
            logger.warning(f"DB context load failed: {e}")
        return None

    def get_review_items(self) -> list[str]:
        return self.load().get("review_required", [])

    def approve(self, entity_name: str) -> None:
        context = self.load()
        for section in ("entities", "metrics"):
            if entity_name in context.get(section, {}):
                context[section][entity_name].update({
                    "review_flag": False,
                    "approved_by_human": True,
                    "approved_at": datetime.utcnow().isoformat()
                })
        review = context.get("review_required", [])
        if entity_name in review:
            review.remove(entity_name)
        self.save(context, source="human")

    def add_tribal_knowledge(self, rule: str) -> None:
        context = self.load()
        rules = context.setdefault("tribal_knowledge", [])
        if rule not in rules:
            rules.append(rule)
        self.save(context, source="human")

    def get_history(self, limit: int = 20) -> list[dict]:
        if not self._db_available():
            return []
        with self._db_conn.cursor() as cur:
            cur.execute(
                "SELECT version, created_at, source, is_active FROM datafly_context "
                "ORDER BY version DESC LIMIT %s", (limit,)
            )
            return [{"version": r[0], "created_at": str(r[1]),
                     "source": r[2], "is_active": r[3]} for r in cur.fetchall()]

    def _next_version(self) -> int:
        if self._db_available():
            with self._db_conn.cursor() as cur:
                cur.execute("SELECT MAX(version) FROM datafly_context")
                row = cur.fetchone()
                return (row[0] or 0) + 1
        if self.yaml_path and self.yaml_path.exists():
            try:
                return self._load_yaml().get("_meta", {}).get("version", 0) + 1
            except Exception:
                pass
        return 1
