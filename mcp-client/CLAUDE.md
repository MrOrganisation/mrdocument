# MrDocument

You have access to a document management system via the MrDocument MCP server.

## When the user asks to find or search documents

Use the `/find` skill. It handles schema discovery, query building, and refinement automatically.

## When the user asks to research, collect, or organize documents

The user may ask you to do multi-step document research — for example, finding documents on a topic, reading their content, following references, and organizing everything into folders. Handle this by chaining tool calls:

1. Use the `/find` process (discover schema, build query, search) to locate the initial documents.
2. Call `get_document_content` to read each result.
3. Analyze the content for references to other documents, people, procedures, cases, or topics.
4. Search again for those referenced items using `/find`.
5. Write results to the folder structure the user specified using the Write tool.
6. Create `mkdir -p` for any subfolders as needed.

When writing files, use `assigned_filename` (or `original_filename`) with `.txt` extension. Disambiguate duplicates with `_<uuid>` suffix.

## When you need to build queries manually

The `find_documents` tool accepts a MongoDB-style query object:

| Intent | Query pattern |
|---|---|
| Full-text search | `{"content": {"$search": "search terms"}}` |
| Context filter | `{"context": {"$eq": "context_name"}}` |
| Metadata field | `{"metadata.sender": {"$eq": "value"}}` |
| Metadata (fuzzy) | `{"metadata.sender": {"$ilike": "%partial%"}}` |
| Date range | `{"date_added": {"$gte": "2024-01-01", "$lt": "2025-01-01"}}` |
| Tag filter | `{"tags": {"$contains": "tag_name"}}` |
| Combine (AND) | `{"$and": [{...}, {...}]}` or multiple fields in one object |
| Combine (OR) | `{"$or": [{...}, {...}]}` |

**Always discover the schema first** by calling `list_contexts` → `list_fields` → `list_candidates` before building queries. This gives you the exact context names, metadata field names, and allowed values.

## Skills

- `/find <Beschreibung>` — Dokumente anhand einer Beschreibung suchen.
- `/search-and-export <Ordner> <Beschreibung>` — Dokumente suchen und als TXT-Dateien exportieren.
- `/find-sources <Fall> <Thema>` — Alle Quelldokumente eines Falls zu einem Thema finden.
- `/find-references <Dokument-ID oder Beschreibung>` — Testverfahren und Methoden in Dokumenten identifizieren und Referenzdokumente dazu suchen.
