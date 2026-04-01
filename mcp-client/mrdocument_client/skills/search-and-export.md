---
name: search-and-export
description: Search for documents via the mrdocument MCP server based on a natural language query and export each matching document's text content as a TXT file into a local folder.
disable-model-invocation: true
argument-hint: "<target-folder> <search-description>"
allowed-tools: MCP, Write, Bash
---

Search for documents matching a natural language description, retrieve their full text content via the MCP server, and save each document as a TXT file.

**Input:** `$ARGUMENTS`

The first token is the **target folder path**. Everything after it is the **search description**.

## Step 1 — Parse arguments

Split `$ARGUMENTS` into:
- `TARGET_FOLDER`: the first token (a directory path)
- `SEARCH_DESCRIPTION`: everything after the first token

## Step 2 — Ensure target folder exists

Run:
```bash
mkdir -p <TARGET_FOLDER>
```

## Step 3 — Build a search query

Translate `SEARCH_DESCRIPTION` into a query object for the `find_documents` MCP tool.

### Query DSL reference

The query parameter accepts a MongoDB-style object. Choose operators based on what the user is looking for:

| Intent | Query pattern |
|---|---|
| Full-text search on content | `{"content": {"$search": "search terms"}}` |
| Filename pattern | `{"original_filename": {"$ilike": "%pattern%"}}` or `{"assigned_filename": {"$ilike": "%pattern%"}}` |
| Description/summary keyword | `{"description": {"$ilike": "%keyword%"}}` |
| Specific context/category | `{"context": {"$eq": "context_name"}}` |
| Tag filter | `{"tags": {"$contains": "tag_name"}}` |
| Metadata field | `{"metadata.<key>": {"$eq": "value"}}` or `{"metadata.<key>": {"$ilike": "%value%"}}` |
| Date range | `{"date_added": {"$gte": "2024-01-01", "$lt": "2025-01-01"}}` |
| State filter | `{"state": {"$eq": "is_complete"}}` |
| Combine conditions (AND) | `{"$and": [{...}, {...}]}` or put multiple fields in one object |
| Combine conditions (OR) | `{"$or": [{...}, {...}]}` |

**Operators:** `$eq`, `$ne`, `$like`, `$ilike`, `$in`, `$contains`, `$gt`, `$gte`, `$lt`, `$lte`, `$exists`, `$search`

**Searchable fields:** `context`, `original_filename`, `assigned_filename`, `description`, `summary`, `content`, `tags`, `metadata.<key>`, `state`, `language`, `date_added`, `created_at`, `updated_at`

### Strategy for choosing the right query

- If the user describes a **topic** or **subject**, use `$search` on `content` — this is a full-text search that respects document language.
- If the user mentions a **filename**, use `$ilike` on `original_filename` or `assigned_filename`.
- If the user mentions a **category or context**, use `$eq` on `context`.
- If the user mentions a **tag**, use `$contains` on `tags`.
- If the user mentions a **metadata field** (e.g. sender, recipient, type), use the `metadata.<key>` path.
- If the user mentions a **date range**, use comparison operators on `date_added`.
- Combine multiple criteria with `$and` or `$or` as appropriate.
- When in doubt, prefer `$search` on `content` — it is the broadest and most forgiving search.

## Step 4 — Execute search

Call `find_documents` with the query. Use a reasonable `limit` (default 50).

If no documents match, tell the user and stop.

## Step 5 — Retrieve content and write files

For **each** document returned by `find_documents`:

1. Call `get_document_content` with the document's `id`.
2. Determine the output filename:
   - Use `assigned_filename` if present, otherwise `original_filename`.
   - Replace the file extension with `.txt` (e.g. `invoice.pdf` becomes `invoice.txt`).
   - If the name already ends in `.txt`, keep it as is.
   - If two documents would produce the same filename, append the document id to disambiguate (e.g. `invoice_<uuid>.txt`).
3. Write the content to `TARGET_FOLDER/<filename>` using the Write tool.

## Step 6 — Report

Tell the user:
- How many documents were found
- How many were exported
- List the filenames written
