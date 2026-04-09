---
name: find
description: Search for documents in MrDocument based on a natural language description.
allowed-tools: MCP
argument-hint: "<search description>"
---

Find documents matching a natural language description using the MrDocument MCP server.

**Input:** `$ARGUMENTS`

## Step 1 — Discover schema

Call `list_contexts` to get all available contexts.

For each context whose description seems relevant to `$ARGUMENTS`, call `list_fields` to get its metadata fields.

For each field that seems relevant, call `list_candidates` to get the allowed values.

Collect:
- **Relevant contexts** with their names
- **Relevant metadata fields** with their exact candidate values

## Step 2 — Build query

Map `$ARGUMENTS` to the query DSL using the discovered schema:

- **Category/context words** (e.g., "work", "private", "receipts") → `{"context": {"$eq": "<context_name>"}}` using the exact context name from step 1.
- **Sender/type/classification** (e.g., "from Schulze", "invoices") → `{"metadata.<field>": {"$eq": "<candidate>"}}` using the exact candidate value from step 1. If no exact match, use `{"metadata.<field>": {"$ilike": "%<partial>%"}}`.
- **Topic/subject** (e.g., "about the rental contract") → `{"content": {"$search": "<terms>"}}` as a fallback when no metadata field matches.
- **Date references** (e.g., "from 2024", "last year") → `{"date_added": {"$gte": "<start>", "$lt": "<end>"}}`.
- **Multiple constraints** → combine with `$and`.
- **Ambiguous matches** (e.g., could be multiple types) → combine with `$or`.

Prefer structured metadata queries over full-text `$search` when a metadata field matches.

## Step 3 — Execute

Call `find_documents` with the query. Use `limit: 20` unless the user asks for more.

## Step 4 — Refine if needed

- **No results:** Broaden — try `$ilike` instead of `$eq`, remove the most restrictive filter, or fall back to `$search` on content.
- **Too many results:** Narrow — add another filter from the discovered fields.
- **Try at most 3 query refinements** before reporting what was found (or not found).

## Step 5 — Report

Present the results as a table with columns: id, context, description, date_added, and any relevant metadata fields. If no results after refinement, explain what was tried.
