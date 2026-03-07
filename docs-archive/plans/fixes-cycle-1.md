# Fixes Cycle 1: Integration Test Validation

Fixes applied after running the pre-existing v1 integration tests against the v2 watcher implementation. All fixes are in production code only — no test fixtures or verification logic were changed.

## Results

25 of 28 integration tests pass. The 3 remaining failures are smart folder symlink tests (v1 feature not implemented in v2).

## Fixes

### step3.py — Audio filename double extension

The mrdocument service returns filenames like `arbeit-besprechung.pdf` for audio transcripts. Code appended `.txt` without stripping the existing extension, producing `.pdf.txt`. Fixed with `Path(suggested_filename).stem + ".txt"`.

### step1.py — `.output` stray detection after `output_filename` cleared

After preprocess clears `output_filename` (replacing it with a `current_paths` entry), `.output` files and `.meta.json` sidecars were no longer recognized by `_is_known()`, causing false stray detection on the next cycle. Fixed by also checking `current_paths` entries for `.output` location matching.

### step4.py — `.meta.json` sidecar accumulation

After moving `.output/{uuid}` to `processed/{name}`, the `.meta.json` sidecar was left behind. Added cleanup: delete sidecar after successful move from `.output`.

### step2.py — Same-cycle recovery race condition

When a new source arrives in the same polling cycle as the previous processing completes (output reaches `processed/`), the record state is `NEEDS_PROCESSING` (not yet `IS_COMPLETE`). The recovery check only accepted `IS_MISSING` or `IS_COMPLETE`, so reprocessing never triggered. Fixed by also accepting `NEEDS_PROCESSING` when `output_filename is None` — safe because during active processing `output_filename` is still set.

### orchestrator.py — Operational logging

Added INFO-level logging throughout `run_cycle()`: changes detected, preprocess results, processing actions, reconcile state transitions, filesystem moves. Only fires when there's activity.

### docker-compose.service-mock.yaml — Mock-stt health check tolerance

Increased `start_period`, `timeout`, and `retries` to handle transient gunicorn startup delays.

## Separation of Concerns

None of the changes cross step boundaries. Each fix stays within its step's responsibility. step1 still only detects, step2 still only computes state, step3 still only calls services, step4 still only moves files.
