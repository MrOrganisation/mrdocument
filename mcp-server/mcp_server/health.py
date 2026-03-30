"""Health check HTTP endpoint."""

import logging

from fastapi import FastAPI
from pathlib import Path

logger = logging.getLogger(__name__)

app = FastAPI(title="MrDocument MCP Server Health")


def _read_version() -> str:
    version_file = Path("/app/VERSION")
    if version_file.is_file():
        return version_file.read_text().strip()
    return "unknown"


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "mrdocument-mcp",
        "version": _read_version(),
    }
