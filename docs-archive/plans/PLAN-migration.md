# PLAN: migrate.py — V1→V2 Database Migration, Tests

## Implementation Order: 8 (depends on models, db_new; runs once before orchestrator takes over)

## Purpose

One-time migration from `mrdocument.documents` + `mrdocument.file_locations` (v1) to `mrdocument.documents_v2` (v2). After migration, the v2 orchestrator can take over from the v1 watcher without losing history.

---

## Schema comparison

### V1: `mrdocument.documents`

| Column | Type | Maps to v2 |
|---|---|---|
| `id` | UUID PK | `id` |
| `username` | TEXT | dropped (v2 is single-user, root implies user) |
| `original_filename` | TEXT | `original_filename` |
| `original_file_hash` | TEXT | `source_hash` |
| `current_file_path` | TEXT (nullable) | → `current_paths` entry |
| `current_file_hash` | TEXT | `hash` |
| `metadata` | JSONB | `metadata` |
| `context_name` | TEXT | `context` |
| `status` | TEXT | → `state` (mapped) |
| `assigned_filename` | TEXT | `assigned_filename` |
| `created_at` | TIMESTAMPTZ | preserved as PathEntry timestamps |
| `updated_at` | TIMESTAMPTZ | preserved as PathEntry timestamps |
| `processed_at` | TIMESTAMPTZ | not directly mapped |
| `perceptual_hash` | TEXT | dropped (unused in v2) |

### V1: `mrdocument.file_locations`

| Column | Maps to v2 |
|---|---|
| `document_id` | FK → v1 doc id |
| `file_path` | additional `current_paths` entries |
| `file_hash` | (same as `hash` on parent doc) |

### V2: `mrdocument.documents_v2`

New fields not in v1: `source_paths`, `missing_source_paths`, `missing_current_paths`, `output_filename`, all temp fields. These get defaults (empty lists / null).

---

## Status → State mapping

| V1 `status` | V2 `state` | Notes |
|---|---|---|
| `incoming` | `is_new` | Not yet processed |
| `processing` | `needs_processing` | In-flight processing |
| `processed` | `is_complete` | Service done, output exists |
| `reviewed` | `is_complete` | Human-reviewed, output exists |
| `sorted` | `is_complete` | Filed into sorted/ |
| `unsortable` | `has_error` | Could not sort (missing fields) |
| `error` | `has_error` | Processing failed |
| `duplicate` | `is_complete` | Known duplicate, keep tracking |

---

## Function: `migrate_v1_to_v2`

```python
async def migrate_v1_to_v2(
    pool: asyncpg.Pool,
    root: Path,
    username: str,
    dry_run: bool = False,
) -> MigrationResult
```

### Parameters

- `pool` — asyncpg connection pool (shared between v1 and v2, same database)
- `root` — user root directory (for converting absolute paths to relative)
- `username` — which user's records to migrate (v1 is multi-user, v2 is single-user per root)
- `dry_run` — if True, report what would be done without writing to v2

### Returns

```python
@dataclass
class MigrationResult:
    total_v1: int           # total v1 records for username
    migrated: int           # successfully migrated
    skipped_existing: int   # already in v2 (by source_hash)
    skipped_no_hash: int    # v1 record has no original_file_hash
    errors: list[str]       # per-record error messages
```

### Logic

1. Ensure v2 schema exists (call `DocumentDBv2` schema SQL)
2. Fetch all v1 records: `SELECT * FROM mrdocument.documents WHERE username = $1`
3. For each v1 record:
   a. **Skip check**: if a v2 record with same `source_hash` already exists, skip (idempotent)
   b. **Map status → state** (see table above)
   c. **Build source_paths**: `[PathEntry("archive/{original_filename}", created_at)]`
      - Use `created_at` as timestamp (best available approximation)
      - The source file lives in archive/ after processing
   d. **Build current_paths**: from `current_file_path` + `file_locations` entries
      - Convert absolute paths to relative (strip `root` prefix)
      - Only include paths that start with a known location prefix (sorted/, processed/, reviewed/, .output/)
      - Use `updated_at` as timestamp
   e. **Extract fields**: `metadata`, `context` (from `context_name`), `assigned_filename`, `hash` (from `current_file_hash`)
   f. **Create v2 Record** with all mapped fields, `state` from mapping
   g. **Insert** into `documents_v2` (unless dry_run)
4. Return `MigrationResult`

### Path conversion

V1 stores absolute paths (`/sync/username/sorted/work/acme/doc.pdf`).
V2 stores relative paths (`sorted/work/acme/doc.pdf`).

```python
def _abs_to_rel(absolute_path: str, root: Path) -> Optional[str]:
    """Convert absolute path to relative, or None if outside root."""
    try:
        return str(Path(absolute_path).relative_to(root))
    except ValueError:
        return None
```

### File locations merging

For each v1 document, fetch from `file_locations`:
```sql
SELECT file_path, file_hash FROM mrdocument.file_locations WHERE document_id = $1
```

Each becomes a `current_paths` entry (after path conversion). Deduplicate against `current_file_path` (may overlap).

---

## Function: `verify_migration`

```python
async def verify_migration(
    pool: asyncpg.Pool,
    username: str,
) -> VerificationResult
```

Post-migration check: for each v1 record, confirm a matching v2 record exists.

```python
@dataclass
class VerificationResult:
    total_v1: int
    matched: int
    unmatched: list[UUID]   # v1 IDs without v2 counterpart
```

---

## CLI wrapper

```python
async def main():
    """CLI entry point for migration."""
    import argparse
    parser = argparse.ArgumentParser(description="Migrate mrdocument v1 → v2")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()

    pool = await asyncpg.create_pool(args.database_url or os.environ["DATABASE_URL"])

    if args.verify:
        result = await verify_migration(pool, args.username)
        print(f"Verified: {result.matched}/{result.total_v1} matched")
        if result.unmatched:
            print(f"Unmatched v1 IDs: {result.unmatched}")
    else:
        result = await migrate_v1_to_v2(pool, args.root, args.username, dry_run=args.dry_run)
        print(f"Migration: {result.migrated}/{result.total_v1} migrated, "
              f"{result.skipped_existing} skipped (existing), "
              f"{result.skipped_no_hash} skipped (no hash)")
        if result.errors:
            for e in result.errors:
                print(f"  ERROR: {e}")

    await pool.close()
```

---

## Tests: `test_migration.py` — requires PostgreSQL

### Fixtures

```python
@pytest_asyncio.fixture
async def pool():
    """Create asyncpg pool, ensure both schemas, cleanup after."""
    pool = await asyncpg.create_pool(DATABASE_URL)
    # Ensure v1 schema
    async with pool.acquire() as conn:
        await conn.execute(V1_SCHEMA_SQL)
        await conn.execute(V2_SCHEMA_SQL)
    yield pool
    # Cleanup both tables
    await pool.execute("DELETE FROM mrdocument.file_locations")
    await pool.execute("DELETE FROM mrdocument.documents")
    await pool.execute("DELETE FROM mrdocument.documents_v2")
    await pool.close()
```

### Test scenarios

#### Basic migration

1. Insert v1 record (status=sorted, metadata, current_file_path, assigned_filename)
2. `migrate_v1_to_v2(pool, root, username)`
3. Assert v2 record exists with correct: source_hash, state=IS_COMPLETE, context, metadata, assigned_filename, hash, source_paths contains archive entry, current_paths contains sorted path

#### Status mapping — all statuses

1. Insert one v1 record per status (incoming, processing, processed, reviewed, sorted, unsortable, error, duplicate)
2. Migrate
3. Assert each maps to expected v2 state

#### Path conversion — absolute to relative

1. Insert v1 record with `current_file_path = "/sync/testuser/sorted/work/doc.pdf"`
2. Migrate with `root = Path("/sync/testuser")`
3. Assert current_paths entry is `"sorted/work/doc.pdf"`

#### File locations merged into current_paths

1. Insert v1 record + 2 file_locations entries
2. Migrate
3. Assert current_paths has entries for current_file_path + both file_locations (deduplicated)

#### Idempotent — re-run skips existing

1. Migrate a set of records
2. Migrate again
3. Assert result.skipped_existing == count, no duplicates in v2

#### Dry run — no writes

1. Insert v1 records
2. `migrate_v1_to_v2(..., dry_run=True)`
3. Assert result.migrated > 0 but v2 table is empty

#### Skip records without hash

1. Insert v1 record with `original_file_hash = NULL` (shouldn't happen but defensive)
2. Migrate
3. Assert skipped_no_hash == 1

#### Path outside root — skipped gracefully

1. Insert v1 record with `current_file_path = "/other/path/doc.pdf"`
2. Migrate with `root = Path("/sync/testuser")`
3. Assert current_paths is empty (path couldn't be converted), record still migrated

#### Null current_file_path

1. Insert v1 record with `current_file_path = NULL` (orphaned record)
2. Migrate
3. Assert v2 record has empty current_paths

#### Verify migration

1. Migrate some records
2. `verify_migration(pool, username)`
3. Assert all matched

#### Verify migration — missing records

1. Insert v1 records, migrate only some (by deleting from v2)
2. `verify_migration(pool, username)`
3. Assert unmatched contains the deleted v2's source_hash IDs
