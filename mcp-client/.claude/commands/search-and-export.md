---
name: search-and-export
description: Search for documents and export their text content as TXT files into a local folder.
argument-hint: "<target-folder> <search-description>"
allowed-tools: MCP, Write, Bash
---

Search for documents matching a natural language description, retrieve their full text content, and save each document as a TXT file.

**Input:** `$ARGUMENTS`

## Step 1 — Parse arguments

Split `$ARGUMENTS` into:
- `TARGET_FOLDER`: the first token (a directory path)
- `SEARCH_DESCRIPTION`: everything after the first token

## Step 2 — Ensure target folder exists

```bash
mkdir -p <TARGET_FOLDER>
```

## Step 3 — Find documents

Follow the /find process for `SEARCH_DESCRIPTION`:

1. Call `list_contexts` to discover available contexts.
2. For relevant contexts, call `list_fields` then `list_candidates` to learn the metadata vocabulary.
3. Build a query using exact metadata values where possible, falling back to `$search` for topic searches.
4. Call `find_documents` with the query. Use `limit: 50`.
5. If no results, broaden and retry (up to 3 attempts).

If no documents match after refinement, tell the user and stop.

## Step 4 — Retrieve content and write files

For **each** document returned:

1. Call `get_document_content` with the document's `id`.
2. Determine the output filename:
   - Use `assigned_filename` if present, otherwise `original_filename`.
   - Replace the file extension with `.txt`.
   - If the name already ends in `.txt`, keep it as is.
   - If two documents would produce the same filename, append `_<uuid>` to disambiguate.
3. Write the content to `TARGET_FOLDER/<filename>`.

## Step 5 — Report

Tell the user:
- How many documents were found
- How many were exported
- List the filenames written
