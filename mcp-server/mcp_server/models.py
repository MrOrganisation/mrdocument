"""Pydantic models for MCP tool inputs."""

from pydantic import BaseModel, Field


FIND_DOCUMENTS_QUERY_DESC = """\
MongoDB-style query object. Supports:
- Operators: $eq, $ne, $like, $ilike, $in, $contains, $gt, $gte, $lt, $lte, $exists, $search
- Logical: $and, $or
- Fields: context, original_filename, assigned_filename, description, summary, \
content, tags, metadata.<key>, state, language, date_added, created_at, updated_at
- Shorthand: {"field": "value"} is equivalent to {"field": {"$eq": "value"}}
- Full-text search: {"content": {"$search": "search terms"}}
- Tags: {"tags": {"$contains": "tag_name"}}
- Metadata keys: {"metadata.sender": {"$ilike": "%schulze%"}}
- Metadata key existence: {"metadata.sender": {"$exists": true}}
- Combining: {"$and": [{...}, {...}]} or {"$or": [{...}, {...}]}
- Multiple conditions on same field: {"date_added": {"$gte": "2024-01-01", "$lt": "2025-01-01"}}

An empty query {} returns all documents (subject to limit).\
"""


class FindDocumentsInput(BaseModel):
    query: dict = Field(
        default_factory=dict,
        description=FIND_DOCUMENTS_QUERY_DESC,
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of results to return",
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Number of results to skip (for pagination)",
    )
    order_by: str = Field(
        default="created_at",
        description="Column to sort by: created_at, updated_at, original_filename, date_added, assigned_filename",
    )
    order_dir: str = Field(
        default="desc",
        description="Sort direction: asc or desc",
    )


class GetDocumentContentInput(BaseModel):
    document_id: str = Field(description="UUID of the document")


class GetDocumentSummaryInput(BaseModel):
    document_id: str = Field(description="UUID of the document")


class ListFieldsInput(BaseModel):
    context: str = Field(description="Context name (e.g. 'privat', 'arbeit')")


class ListCandidatesInput(BaseModel):
    context: str = Field(description="Context name")
    field: str = Field(description="Field name within the context")
