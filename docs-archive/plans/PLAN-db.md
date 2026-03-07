# PLAN: db_new.py — Schema, DocumentDBv2 Class, Tests

## Implementation Order: 2 (depends on models)

## Schema: `mrdocument.documents_v2`

All Record fields mapped to columns:
- Path lists (`source_paths`, etc.): `JSONB NOT NULL DEFAULT '[]'` — arrays of `{"path": "...", "timestamp": "..."}`
- `metadata`: `JSONB` (nullable)
- `state`: `TEXT NOT NULL DEFAULT 'is_new'` with CHECK constraint
- `duplicate_sources`, `deleted_paths`: `JSONB NOT NULL DEFAULT '[]'`

Indexes on:
- `source_hash`
- `hash` (WHERE NOT NULL)
- `output_filename` (WHERE NOT NULL)
- `state`
- `metadata` (GIN)

Auto-update trigger on `updated_at`.

## Class: `DocumentDBv2`

Follows existing `db.py` pattern: asyncpg pool, connect/disconnect, pool property.

### Conversion helpers (static methods)
- `_path_entries_to_json(entries) -> str`
- `_json_to_path_entries(data) -> list[PathEntry]`
- `_row_to_record(row) -> Record`
- `_record_to_params(record) -> dict`

### CRUD operations
- `create_record(record) -> UUID`
- `get_record(id) -> Optional[Record]`
- `save_record(record)` — full update
- `delete_record(id) -> bool`

### Query operations
- `get_records_by_state(state) -> list[Record]`
- `get_record_by_source_hash(hash) -> Optional[Record]`
- `get_record_by_hash(hash) -> Optional[Record]`
- `get_record_by_output_filename(filename) -> Optional[Record]`
- `get_snapshot() -> list[Record]` — all records
- `get_records_with_temp_fields() -> list[Record]` — WHERE any temp field is non-null OR state=needs_deletion
- `get_records_with_output_filename() -> list[Record]` — WHERE output_filename IS NOT NULL

## Tests (test_db_new.py) — requires PostgreSQL

- Fixture: connect, yield, cleanup (DELETE all), disconnect
- CRUD round-trip, nonexistent get, save updates, delete
- Query by state, source_hash, hash, output_filename
- Snapshot returns all, temp-fields filter, output_filename filter
- JSONB round-trips: PathEntry list, metadata dict
