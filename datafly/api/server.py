"""
Datafly REST API
"""
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import os


# ── Request Models (must be module-level for Pydantic) ───────────────────────

class ConnectRequest(BaseModel):
    connection_string: str
    name: str

class QueryRequest(BaseModel):
    question: str
    adapter_hint: Optional[str] = None

class FeedbackRequest(BaseModel):
    query_id: str
    correction: str

class TribalKnowledgeRequest(BaseModel):
    rule: str


# ── App Factory ───────────────────────────────────────────────────────────────

def create_app(datafly=None) -> FastAPI:
    app = FastAPI(
        title="Datafly Data Gateway",
        description="Universal data gateway with AI-powered semantic context layer",
        version="0.1.0",
    )

    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_methods=["*"], allow_headers=["*"])

    if datafly is None:
        from datafly.gateway import Datafly
        datafly = Datafly.from_env()

    API_KEY = os.getenv("DATAFLY_API_KEY", "")

    @app.middleware("http")
    async def auth_middleware(request: Request, call_next):
        if API_KEY and request.method not in ("GET", "HEAD", "OPTIONS"):
            key = request.headers.get("X-API-Key", "")
            if key != API_KEY:
                return JSONResponse(status_code=401, content={"detail": "Invalid API key"})
        return await call_next(request)

    @app.get("/health")
    def health():
        return datafly.status()

    @app.post("/connect")
    def connect_adapter(req: ConnectRequest):
        try:
            datafly.connect(req.connection_string, req.name)
            return {"status": "connected", "name": req.name}
        except Exception as e:
            raise HTTPException(400, str(e))

    @app.post("/connect/test")
    def test_connection(req: ConnectRequest):
        """Test a connection string without persisting it."""
        try:
            from datafly.adapters.factory import AdapterFactory
            adapter = AdapterFactory.create(req.connection_string, req.name)
            adapter.connect()
            schema = adapter.introspect_schema()
            tables = list(schema.get("tables", {}).keys())
            adapter.disconnect()
            return {
                "status": "ok",
                "name": req.name,
                "table_count": len(tables),
                "tables": tables[:10]
            }
        except Exception as e:
            raise HTTPException(400, {"status": "error", "message": str(e)})

    @app.delete("/adapters/{name}")
    def disconnect_adapter(name: str):
        """Remove a connected adapter."""
        try:
            if name not in datafly.adapters:
                raise HTTPException(404, f"Adapter '{name}' not found")
            adapter = datafly.adapters.pop(name)
            try:
                adapter.disconnect()
            except Exception:
                pass
            return {"status": "disconnected", "name": name}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.post("/context/build")
    def build_context(force_rebuild: bool = False):
        try:
            ctx = datafly.build_context(force_rebuild=force_rebuild)
            return {
                "status": "built",
                "version": ctx.get("_meta", {}).get("version"),
                "entities": len(ctx.get("entities", {})),
                "metrics": len(ctx.get("metrics", {})),
                "review_required": ctx.get("review_required", [])
            }
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.get("/context")
    def get_context():
        try:
            return datafly.context_store.load()
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))

    @app.get("/context/history")
    def get_context_history():
        return {"history": datafly.context_store.get_history()}

    @app.get("/context/review")
    def get_review_items():
        return {"items": datafly.context_store.get_review_items()}

    @app.post("/context/approve/{entity_name}")
    def approve_entity(entity_name: str):
        datafly.context_store.approve(entity_name)
        return {"status": "approved", "entity": entity_name}

    @app.post("/context/tribal-knowledge")
    def add_tribal_knowledge(req: TribalKnowledgeRequest):
        datafly.context_store.add_tribal_knowledge(req.rule)
        return {"status": "added", "rule": req.rule}

    @app.post("/query")
    def run_query(req: QueryRequest):
        result = datafly.query(req.question, adapter_hint=req.adapter_hint)
        if not result.get("success"):
            raise HTTPException(400, detail=result)
        return result

    @app.post("/feedback")
    def submit_feedback(req: FeedbackRequest):
        datafly.feedback(req.query_id, req.correction)
        return {"status": "applied"}

    @app.get("/adapters")
    def list_adapters():
        return datafly.status()["adapters"]

    return app
