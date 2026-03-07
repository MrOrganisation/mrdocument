# PLAN: step3.py — Processing Service Calls, Tests

## Implementation Order: 5 (depends on models)

## Class: `Processor`

```python
def __init__(self, root: Path, service_url: str, timeout: float = 900.0)
async def process_one(self, record: Record) -> None
```

## Logic

1. Read source file from `root / record.source_file.path`
2. Determine type: document (PDF, RTF, TXT, EML, HTML, DOCX, images) or audio (FLAC, WAV, MP3, etc.)
3. Call mrdocument service: `POST /process` with multipart form (file, contexts JSON)
4. **On success**: Write result to tmp, atomic rename to `root / .output / {output_filename}`. Write sidecar JSON to `.output/{output_filename}.meta.json` with context, metadata, assigned_filename.
5. **On error**: Create 0-byte file at `root / .output / {output_filename}`

## Reuse from existing code

- Retry logic from `watcher.py` (exponential backoff, 3 retries, 2s initial delay)
- Extension-based type detection from `watcher.py:4087`
- Filename formatting from `sorter.py:656` (`_format_filename`, `_sanitize_filename_part`)

## Tests (test_step3.py)

- **Mock service** using aiohttp test server (pattern from `watcher/test_integration.py`)
- Success: service returns metadata + base64 PDF → file written, sidecar written, content correct
- Error: service returns 500 → 0-byte file written
- Error: service unreachable → 0-byte file written
- Source read: correct file read from source_paths
- Sidecar format: JSON with expected keys
- Atomic write: file created via rename, no partial files
- Audio vs document: different service call parameters
