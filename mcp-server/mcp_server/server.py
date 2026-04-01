"""MCP server entry point.

Registers all tools, handles per-request authentication via SSE transport,
and manages the lifecycle of database connections and health checks.
"""

import asyncio
import contextvars
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .auth import AuthError, TokenStore, UserCredentialStore, decode_bearer_token
from .contexts import ContextReadError, ContextReader
from .db import DatabaseManager
from .models import (
    FindDocumentsInput,
    GetDocumentContentInput,
    GetDocumentSummaryInput,
    ListCandidatesInput,
    ListFieldsInput,
)
from .query_dsl import QueryBuildError
from .tools import DocumentTools

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

DATABASE_HOST = os.environ.get("DATABASE_HOST", "mrdocument-db")
DATABASE_PORT = int(os.environ.get("DATABASE_PORT", "5432"))
DATABASE_NAME = os.environ.get("DATABASE_NAME", "mrdocument")
SYNC_ROOT = os.environ.get("SYNC_ROOT", "/sync")
MCP_SUBDIR = os.environ.get("MCP_SUBDIR", "mrdocument")
MCP_PORT = int(os.environ.get("MCP_PORT", "8091"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# Per-request context: stores the authenticated (username, pool) tuple
# so that tool handlers can access it without passing it through the MCP SDK.
# ---------------------------------------------------------------------------

_current_user: contextvars.ContextVar[tuple[str, Any] | None] = contextvars.ContextVar(
    "_current_user", default=None
)

# ---------------------------------------------------------------------------
# Shared state (initialized in lifespan)
# ---------------------------------------------------------------------------

credential_store: UserCredentialStore | None = None
token_store: TokenStore | None = None
db_manager: DatabaseManager | None = None
doc_tools: DocumentTools | None = None

# ---------------------------------------------------------------------------
# MCP Server setup
# ---------------------------------------------------------------------------

mcp_server = Server("mrdocument-mcp")


@mcp_server.list_tools()
async def list_tools():
    """Return the list of available tools."""
    from mcp.types import Tool

    return [
        Tool(
            name="find_documents",
            description=(
                "Search documents using a MongoDB-style query DSL. "
                "Returns records without content and summary fields. "
                "Supports operators: $eq, $ne, $like, $ilike, $in, $contains, "
                "$gt, $gte, $lt, $lte, $exists, $search. "
                "Logical combinators: $and, $or. "
                "Searchable fields: context, original_filename, assigned_filename, "
                "description, summary, content, tags, metadata.<key>, state, "
                "language, date_added, created_at, updated_at. "
                "Full-text search on content: {\"content\": {\"$search\": \"terms\"}}. "
                "Metadata dot-notation: {\"metadata.sender\": {\"$eq\": \"value\"}}."
            ),
            inputSchema=FindDocumentsInput.model_json_schema(),
        ),
        Tool(
            name="get_document_content",
            description="Get the full text content of a document by its UUID.",
            inputSchema=GetDocumentContentInput.model_json_schema(),
        ),
        Tool(
            name="get_document_summary",
            description="Get the AI-generated summary of a document by its UUID.",
            inputSchema=GetDocumentSummaryInput.model_json_schema(),
        ),
        Tool(
            name="list_contexts",
            description=(
                "List all document contexts (categories) available to the user. "
                "Each context defines a set of metadata fields and a folder structure. "
                "Returns name, description, filename pattern, and folder hierarchy."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="list_fields",
            description=(
                "List all metadata fields defined for a given context. "
                "Returns field names, instructions, allow_new_candidates flag, "
                "and candidate count."
            ),
            inputSchema=ListFieldsInput.model_json_schema(),
        ),
        Tool(
            name="list_candidates",
            description=(
                "List all candidates (possible values) for a specific field "
                "within a context. Returns merged candidates from base "
                "configuration and auto-generated values."
            ),
            inputSchema=ListCandidatesInput.model_json_schema(),
        ),
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict):
    """Dispatch a tool call to the appropriate handler."""
    from mcp.types import TextContent

    user_ctx = _current_user.get()
    if user_ctx is None:
        return [TextContent(type="text", text="Error: not authenticated")]

    username, pool = user_ctx

    try:
        result = await _dispatch_tool(name, arguments, username, pool)
        text = json.dumps(result, ensure_ascii=False, default=str)
        return [TextContent(type="text", text=text)]
    except QueryBuildError as e:
        return [TextContent(type="text", text=f"Query error: {e}")]
    except ContextReadError as e:
        return [TextContent(type="text", text=f"Context error: {e}")]
    except ValueError as e:
        return [TextContent(type="text", text=f"Validation error: {e}")]
    except Exception as e:
        logger.exception("Tool %s failed", name)
        return [TextContent(type="text", text=f"Internal error: {e}")]


async def _dispatch_tool(
    name: str, arguments: dict, username: str, pool: Any
) -> Any:
    """Route a tool call to the correct DocumentTools method."""
    if name == "find_documents":
        params = FindDocumentsInput(**arguments)
        return await doc_tools.find_documents(
            pool,
            query=params.query,
            limit=params.limit,
            offset=params.offset,
            order_by=params.order_by,
            order_dir=params.order_dir,
        )
    elif name == "get_document_content":
        params = GetDocumentContentInput(**arguments)
        result = await doc_tools.get_document_content(pool, params.document_id)
        if result is None:
            return {"error": "Document not found"}
        return result
    elif name == "get_document_summary":
        params = GetDocumentSummaryInput(**arguments)
        result = await doc_tools.get_document_summary(pool, params.document_id)
        if result is None:
            return {"error": "Document not found"}
        return result
    elif name == "list_contexts":
        return doc_tools.list_contexts(username)
    elif name == "list_fields":
        params = ListFieldsInput(**arguments)
        return doc_tools.list_fields(username, params.context)
    elif name == "list_candidates":
        params = ListCandidatesInput(**arguments)
        return doc_tools.list_candidates(username, params.context, params.field)
    else:
        raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# SSE transport with per-connection authentication
# ---------------------------------------------------------------------------

sse_transport = SseServerTransport("/messages/")


async def _authenticate(request: Request) -> tuple[str, Any] | JSONResponse:
    """Authenticate from Authorization header.

    Supports two schemes:
      - Bearer {oauth_token} — token from POST /oauth/token
      - Bearer {base64(username:mcp_password)} — direct credentials

    Returns (username, db_pool) on success or a JSONResponse error.
    """
    auth_header = request.headers.get("authorization", "")
    if not auth_header:
        return JSONResponse(
            {"error": "Missing Authorization header"}, status_code=401
        )

    if not auth_header.startswith("Bearer "):
        return JSONResponse(
            {"error": "Authorization header must use Bearer scheme"}, status_code=401
        )

    token = auth_header[len("Bearer "):]

    # Try OAuth token first
    username = token_store.validate(token)

    if username is None:
        # Fall back to base64(username:password) format
        try:
            username, password = decode_bearer_token(auth_header)
        except AuthError as e:
            return JSONResponse({"error": str(e)}, status_code=401)

        if not credential_store.validate(username, password):
            return JSONResponse(
                {"error": f"Invalid credentials for user '{username}'"}, status_code=401
            )

    db_password = credential_store.get_db_password(username)
    if db_password is None:
        return JSONResponse(
            {"error": f"No database credentials found for user '{username}'"}, status_code=503
        )

    pool = await db_manager.get_pool(username, db_password)
    return (username, pool)


async def handle_sse(scope, receive, send):
    """SSE connection endpoint (raw ASGI).

    The MCP SSE transport takes over the ASGI channel directly —
    it cannot go through Starlette's Route which wraps handlers in
    request_response() and expects a Response return value.
    """
    request = Request(scope, receive, send)
    result = await _authenticate(request)
    if isinstance(result, JSONResponse):
        await result(scope, receive, send)
        return
    username, pool = result

    _current_user.set((username, pool))

    async with sse_transport.connect_sse(scope, receive, send) as (read_stream, write_stream):
        await mcp_server.run(
            read_stream,
            write_stream,
            mcp_server.create_initialization_options(),
        )


async def handle_messages(scope, receive, send):
    """Message POST endpoint (raw ASGI)."""
    await sse_transport.handle_post_message(scope, receive, send)


async def handle_oauth_token(request: Request):
    """OAuth 2.0 token endpoint (client_credentials grant).

    Expects form-encoded POST with:
      - grant_type: "client_credentials"
      - client_id: username
      - client_secret: mcp-password
    """
    body = await request.form()
    grant_type = body.get("grant_type", "")
    client_id = body.get("client_id", "")
    client_secret = body.get("client_secret", "")

    if grant_type != "client_credentials":
        return JSONResponse(
            {"error": "unsupported_grant_type"}, status_code=400
        )

    if not client_id or not client_secret:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "client_id and client_secret are required"},
            status_code=400,
        )

    username = client_id.lower()
    if not credential_store.validate(username, client_secret):
        return JSONResponse(
            {"error": "invalid_client"}, status_code=401
        )

    access_token, expires_in = token_store.issue(username)
    return JSONResponse({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    })


async def handle_oauth_metadata(request: Request):
    """OAuth 2.0 Authorization Server Metadata (RFC 8414).

    Claude Desktop discovers the token endpoint from this.
    """
    base_url = os.environ.get("MCP_PUBLIC_URL", f"http://localhost:{MCP_PORT}")
    return JSONResponse({
        "issuer": base_url,
        "token_endpoint": f"{base_url}/oauth/token",
        "grant_types_supported": ["client_credentials"],
        "token_endpoint_auth_methods_supported": ["client_secret_post"],
        "response_types_supported": [],
    })


async def handle_health(request: Request):
    """Health check endpoint."""
    return JSONResponse({"status": "healthy", "service": "mrdocument-mcp"})


# ---------------------------------------------------------------------------
# Starlette app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app):
    """Initialize and tear down shared state."""
    global credential_store, token_store, db_manager, doc_tools

    credential_store = UserCredentialStore(SYNC_ROOT, subdir=MCP_SUBDIR)
    token_store = TokenStore()
    db_manager = DatabaseManager(DATABASE_HOST, DATABASE_PORT, DATABASE_NAME)
    context_reader = ContextReader(SYNC_ROOT, subdir=MCP_SUBDIR)
    doc_tools = DocumentTools(db_manager, context_reader)

    await db_manager.start()
    logger.info(
        "MCP server started (db=%s:%d/%s, sync=%s, users=%s)",
        DATABASE_HOST, DATABASE_PORT, DATABASE_NAME, SYNC_ROOT,
        ", ".join(sorted(credential_store.known_users)) or "none",
    )

    yield

    await db_manager.close()
    logger.info("MCP server stopped")


_inner_app = Starlette(
    debug=False,
    lifespan=lifespan,
    routes=[
        Route("/health", handle_health),
        Route("/.well-known/oauth-authorization-server", handle_oauth_metadata),
        Route("/oauth/token", handle_oauth_token, methods=["POST"]),
    ],
)


async def starlette_app(scope, receive, send):
    """Top-level ASGI app.

    Routes /sse and /messages/ directly as raw ASGI handlers (the MCP
    SSE transport manages its own HTTP lifecycle). Everything else goes
    through Starlette's routing.
    """
    if scope["type"] == "http":
        path = scope.get("path", "")
        if path == "/sse":
            return await handle_sse(scope, receive, send)
        if path.startswith("/messages/"):
            return await handle_messages(scope, receive, send)
    await _inner_app(scope, receive, send)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    """Configure logging."""
    log_dir = os.environ.get("LOG_DIR")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.FileHandler(
            os.path.join(log_dir, "mcp-server.log"),
            encoding="utf-8",
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
    )

    # Suppress noisy uvicorn access logs for health checks
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def main() -> None:
    """Run the MCP server."""
    _setup_logging()
    logger.info("Starting MrDocument MCP server on port %d", MCP_PORT)
    uvicorn.run(
        starlette_app,
        host="0.0.0.0",
        port=MCP_PORT,
        log_level=LOG_LEVEL.lower(),
    )


if __name__ == "__main__":
    main()
