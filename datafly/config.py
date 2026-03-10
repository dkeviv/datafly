"""
Datafly configuration — loads from env vars, .env file, or explicit config.
Priority: explicit args > env vars > .env file > defaults
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


def _load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader — no dependencies."""
    env_file = Path(path)
    if not env_file.exists():
        return
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key not in os.environ:  # don't override existing env vars
                os.environ[key] = value
    logger.debug(f"Loaded .env from {path}")


@dataclass
class DataflyConfig:
    # Anthropic
    anthropic_api_key: str = ""

    # Context storage backend: "yaml" | "postgres" | "hybrid" (yaml + postgres)
    context_backend: str = "hybrid"
    context_yaml_path: str = "datafly/context/context.yaml"
    context_db_url: str = ""  # Postgres URL for runtime context cache

    # API server
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_key: str = ""  # For securing the REST API

    # MCP
    mcp_port: int = 8080

    # Logging
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, dotenv_path: str = ".env") -> "DataflyConfig":
        _load_dotenv(dotenv_path)
        cfg = cls(
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            context_backend=os.getenv("DATAFLY_CONTEXT_BACKEND", "hybrid"),
            context_yaml_path=os.getenv("DATAFLY_CONTEXT_YAML", "datafly/context/context.yaml"),
            context_db_url=os.getenv("DATAFLY_CONTEXT_DB_URL", ""),
            api_host=os.getenv("DATAFLY_API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("DATAFLY_API_PORT", "8000")),
            api_key=os.getenv("DATAFLY_API_KEY", ""),
            mcp_port=int(os.getenv("DATAFLY_MCP_PORT", "8080")),
            log_level=os.getenv("DATAFLY_LOG_LEVEL", "INFO"),
        )

        if not cfg.anthropic_api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY is required. "
                "Set it in your environment or .env file."
            )

        # Apply to anthropic client env
        os.environ["ANTHROPIC_API_KEY"] = cfg.anthropic_api_key
        logging.basicConfig(level=getattr(logging, cfg.log_level.upper(), logging.INFO))
        return cfg
