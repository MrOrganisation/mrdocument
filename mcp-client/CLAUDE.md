# MrDocument

You have access to a document management system via the MrDocument MCP server.
Use the MCP tools to search, retrieve, and work with the user's documents.

## Available tools

- **find_documents** — Search documents using a MongoDB-style query DSL. Returns metadata without content/summary.
- **get_document_content** — Get the full text content of a document by UUID.
- **get_document_summary** — Get the AI-generated summary of a document by UUID.
- **list_contexts** — List all document contexts (categories) with their descriptions and filename patterns.
- **list_fields** — List the classification fields for a context (e.g., type, sender).
- **list_candidates** — List the allowed values for a classification field.

## Query DSL reference

The `find_documents` query parameter accepts a MongoDB-style object:

| Intent | Query pattern |
|---|---|
| Full-text search | `{"content": {"$search": "search terms"}}` |
| Filename pattern | `{"original_filename": {"$ilike": "%pattern%"}}` |
| Context filter | `{"context": {"$eq": "context_name"}}` |
| Tag filter | `{"tags": {"$contains": "tag_name"}}` |
| Metadata field | `{"metadata.sender": {"$eq": "value"}}` |
| Date range | `{"date_added": {"$gte": "2024-01-01", "$lt": "2025-01-01"}}` |
| Combine (AND) | `{"$and": [{...}, {...}]}` or multiple fields in one object |
| Combine (OR) | `{"$or": [{...}, {...}]}` |

Operators: `$eq`, `$ne`, `$like`, `$ilike`, `$in`, `$contains`, `$gt`, `$gte`, `$lt`, `$lte`, `$exists`, `$search`

## Guidelines

- Start with `find_documents` to search, then use `get_document_content` or `get_document_summary` for details.
- Prefer `$search` on `content` for broad topic searches — it uses full-text search with language-aware stemming.
- Use `metadata.<key>` for structured lookups (sender, type, date).
- Documents are organized into contexts (e.g., "privat", "arbeit"). Use `list_contexts` to discover them.
- All queries are automatically scoped to the authenticated user via row-level security.

## Skills

Available slash commands:

- `/search-and-export <folder> <description>` — Search documents and export their text content as TXT files into a local folder.
