"""
Datafly MCP Server — exposes the gateway as an MCP (Model Context Protocol) endpoint.
Agents connect to this and get context-aware data access with a single tool call.
"""

from __future__ import annotations
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class MCPServer:
    """
    Minimal MCP-compatible server over SSE (Server-Sent Events).
    Exposes three MCP tools:
      - query_data: natural language → context-enriched result
      - get_context: return the current semantic context layer
      - list_adapters: return connected data sources
    """

    def __init__(self, datafly_instance):
        self.datafly = datafly_instance

    def serve(self, port: int = 8080) -> None:
        from fastapi import FastAPI, Request
        from fastapi.responses import StreamingResponse
        import uvicorn, asyncio

        app = FastAPI(title="Datafly MCP Server")

        # MCP manifest — tells agents what tools are available
        @app.get("/.well-known/mcp.json")
        def manifest():
            return {
                "name": "datafly",
                "version": "0.1.0",
                "description": "Universal data gateway with semantic context layer",
                "tools": [
                    {
                        "name": "query_data",
                        "description": (
                            "Ask a natural language question about your data. "
                            "Datafly automatically routes to the right database, "
                            "injects business context, and returns results."
                        ),
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "question": {"type": "string", "description": "Natural language question"},
                                "adapter_hint": {"type": "string", "description": "Optional: force a specific adapter"}
                            },
                            "required": ["question"]
                        }
                    },
                    {
                        "name": "get_context",
                        "description": "Return the current semantic context layer (entities, metrics, routing rules)",
                        "inputSchema": {"type": "object", "properties": {}}
                    },
                    {
                        "name": "list_adapters",
                        "description": "List all connected data sources",
                        "inputSchema": {"type": "object", "properties": {}}
                    }
                ]
            }

        @app.post("/mcp/call")
        async def call_tool(request: Request):
            body = await request.json()
            tool = body.get("tool")
            args = body.get("arguments", {})

            try:
                if tool == "query_data":
                    result = self.datafly.query(
                        question=args["question"],
                        adapter_hint=args.get("adapter_hint")
                    )
                    return {"result": result, "isError": not result.get("success")}

                elif tool == "get_context":
                    ctx = self.datafly.context_store.load()
                    # Return a compact version — full context can be large
                    return {"result": {
                        "entities": list(ctx.get("entities", {}).keys()),
                        "metrics": {
                            k: v.get("description", "")
                            for k, v in ctx.get("metrics", {}).items()
                        },
                        "tribal_knowledge": ctx.get("tribal_knowledge", []),
                        "version": ctx.get("_meta", {}).get("version")
                    }}

                elif tool == "list_adapters":
                    return {"result": self.datafly.status()["adapters"]}

                else:
                    return {"isError": True, "result": f"Unknown tool: {tool}"}

            except Exception:
                logger.exception("MCP tool error while executing tool '%s'", tool)
                return {
                    "isError": True,
                    "result": "Internal server error while executing MCP tool.",
                }

        # SSE endpoint for streaming (used by some MCP clients)
        @app.get("/mcp/sse")
        async def sse_endpoint():
            async def event_stream():
                # Send manifest on connect
                manifest_data = json.dumps({"type": "manifest", "data": manifest()})
                yield f"data: {manifest_data}\n\n"
                # Keep alive
                while True:
                    await asyncio.sleep(30)
                    yield f"data: {json.dumps({'type': 'ping'})}\n\n"
            return StreamingResponse(event_stream(), media_type="text/event-stream")

        logger.info(f"Datafly MCP server starting on port {port}")
        logger.info(f"MCP manifest: http://localhost:{port}/.well-known/mcp.json")
        uvicorn.run(app, host="0.0.0.0", port=port)
