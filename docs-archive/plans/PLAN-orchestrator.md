# PLAN: orchestrator.py — Main Loop + Integration, Tests

## Implementation Order: 7 (depends on all previous: models, db_new, step1–step4, sorter.py for ContextConfig)

## Purpose

The orchestrator ties together steps 1–4 into a single polling cycle and wires them to the existing framework. After this, the existing `test_integration.py` tests can run against the v2 pipeline.

## Two deliverables

1. **`orchestrator.py`** — the main polling loop + glue code
2. **`test_orchestrator.py`** — integration tests using real DB + mock mrdocument service + real filesystem

---

## Class: `DocumentWatcherV2`

```python
def __init__(
    self,
    root: Path,
    db: DocumentDBv2,
    service_url: str,
    context_field_names: Optional[dict[str, list[str]]] = None,
    poll_interval: float = 5.0,
    processor_timeout: float = 900.0,
)
async def run_cycle(self) -> None
async def run(self) -> None      # polling loop
```

### Constructor

- Instantiates internal components:
  - `FilesystemDetector(root)` (step1)
  - `Processor(root, service_url, timeout=processor_timeout)` (step3)
  - `FilesystemReconciler(root)` (step4)
- Stores `db`, `context_field_names`, `poll_interval`

### `run_cycle()` — single pass

Implements one complete cycle of the pipeline:

```
1. snapshot = await db.get_snapshot()
2. changes = await detector.detect(snapshot)
3. if no changes: return

4. modified, new_records = preprocess(changes, snapshot, read_sidecar)
   where read_sidecar reads {root}/.output/{name}.meta.json from disk

5. for record in new_records:
       await db.create_record(record)

6. for record in modified:
       await db.save_record(record)

7. to_process = await db.get_records_with_output_filename()
   for record in to_process:
       await processor.process_one(record)

8. all_records = await db.get_snapshot()
   for record in all_records:
       result = reconcile(record, context_field_names)
       if result is None:
           await db.delete_record(record.id)
       else:
           await db.save_record(result)

9. actionable = await db.get_records_with_temp_fields()
   reconciler.reconcile(actionable)

10. for record in actionable:
        record.clear_temporary_fields()
        if record.state == State.NEEDS_DELETION:
            record.state = State.IS_DELETED
        await db.save_record(record)
```

### `run()` — polling loop

```python
async def run(self):
    while True:
        await self.run_cycle()
        await asyncio.sleep(self.poll_interval)
```

### Helper: `_read_sidecar(path: str) -> dict`

Reads `{root}/.output/{filename}.meta.json`, returns parsed JSON.
If file not found or parse error, returns `{}`.
The `path` argument is the `.output/{filename}` relative path; sidecar is `path + ".meta.json"`.

---

## Integration with existing framework

### Context field names

The existing `sorter.py` has `SorterContextManager` which loads context YAML files and builds `ContextConfig` objects with `field_names`. The orchestrator accepts `context_field_names: dict[str, list[str]]` which maps context name → field names.

Helper to bridge:

```python
def context_field_names_from_sorter(context_manager: SorterContextManager) -> dict[str, list[str]]:
    return {name: ctx.field_names for name, ctx in context_manager.contexts.items()}
```

This is a standalone function (not a method) so the orchestrator doesn't depend on sorter internals.

### Assigned filename

The `Processor` (step3) sends the document to mrdocument which returns `metadata.assigned_filename`. This replaces the v1 `_format_filename()` logic — the service now computes the filename.

### Target path computation

`compute_target_path(record)` in step2 uses `record.context` + `record.assigned_filename` → `sorted/{context}/{assigned_filename}`. This is simpler than v1's folder-based hierarchy because the service now embeds the full folder structure in `assigned_filename`.

---

## Tests: `test_orchestrator.py`

Uses real PostgreSQL + mock mrdocument service + real filesystem (tmp_path).

### Fixtures

```python
@pytest_asyncio.fixture
async def db():
    db = DocumentDBv2(DATABASE_URL)
    await db.connect()
    yield db
    # cleanup: DELETE FROM mrdocument.documents_v2
    await db.pool.execute("DELETE FROM mrdocument.documents_v2")
    await db.disconnect()

@pytest.fixture
def user_root(tmp_path):
    """Create standard directory structure."""
    for d in ("archive", "incoming", "reviewed", "processed",
              "trash", ".output", "sorted", "error", "void", "lost"):
        (tmp_path / d).mkdir()
    return tmp_path

@pytest_asyncio.fixture
async def mock_service():
    # Same pattern as test_step3.py MockService
    ...
```

### Test scenarios

#### End-to-end: new file → processed → sorted

1. Place a PDF in `incoming/`
2. `run_cycle()` — detector finds it, preprocess creates record, reconcile sets NEEDS_PROCESSING + output_filename, step3 sends to mock service, reconcile sets target_path, step4 moves files
3. Assert: file in `archive/`, output in `sorted/{context}/{assigned_filename}`, DB record state=IS_COMPLETE

#### Error recovery

1. Place a PDF in `incoming/`
2. Mock service returns 500
3. `run_cycle()` → record gets HAS_ERROR
4. Fix mock service
5. Manual retry (reset state) → subsequent cycle processes correctly

#### Stray detection

1. Place an unknown file in `archive/` (no DB record)
2. `run_cycle()` → file moved to `error/`, no change recorded

#### File removal tracking

1. Create a record with file in `incoming/`
2. Run cycle to process
3. Remove the source file
4. `run_cycle()` → record tracks removal in missing_source_paths

#### Multiple files in one cycle

1. Place 3 files in `incoming/`
2. `run_cycle()` → all 3 processed, sorted, tracked in DB

#### Duplicate detection

1. Place same file (same hash) in `incoming/` twice (different names)
2. `run_cycle()` → second copy recognized as duplicate, moved to `void/`

#### Already-processed file reappears

1. Process a file through full cycle
2. Place same hash in `sorted/`
3. `run_cycle()` → matched to existing record, no reprocessing

#### Needs deletion (trash)

1. Process a file through full cycle
2. Move source to `trash/`
3. `run_cycle()` → record state=NEEDS_DELETION, files moved to `void/`

#### Sidecar read

1. Manually create `.output/uuid` file + `.output/uuid.meta.json`
2. Create matching record with output_filename=uuid
3. `run_cycle()` → preprocess reads sidecar, populates context/metadata/assigned_filename

#### No changes cycle

1. Run cycle with no files
2. Assert: no errors, no DB changes

#### Idempotent cycles

1. Process files through one cycle
2. Run additional cycles
3. Assert: no duplicate processing, state remains consistent
