# PLAN: Audio Processing (STT orchestration) in step3.py

## Implementation Order: 10 (depends on step3, orchestrator, app)

## Purpose

Add audio file support to the v2 pipeline. In v1, the watcher orchestrated a multi-step flow between the STT service (ElevenLabs) and the mrdocument service. The v2 Processor currently sends all files to a single `/process` endpoint, which doesn't handle audio. This plan adds the STT orchestration to `step3.py`.

## Background: v1 audio flow (watcher.py:3503–3948)

1. **Classify audio** (`POST mrdocument_url/classify_audio`) — sends filename + contexts, gets back `context`, `metadata`, `transcription_keyterms`
2. **First STT pass** (`POST stt_url/transcribe`) — sends audio bytes + language + model + diarization settings + keyterms → gets raw transcript JSON
3. **Track ElevenLabs costs** — duration from last segment's end time
4. **Intro two-pass** (if "intro" in filename):
   - **Classify transcript** (`POST mrdocument_url/classify_transcript`) — sends transcript + filename + contexts → gets richer keyterms + `number_of_speakers`
   - **Second STT pass** (`POST stt_url/transcribe`) — sends same audio with improved keyterms + updated diarization speaker count
   - Build `pre_classified` dict from transcript classification
5. **Process transcript** (`POST mrdocument_url/process_transcript`) — sends transcript JSON + filename + contexts + optional `pre_classified` → gets back corrected JSON, PDF bytes, text, metadata

## Config: SttConfig (watcher.py:1138)

Loaded from `{user_root}/stt.yaml`:

```python
@dataclass
class SttConfig:
    language: str = "de-DE"
    elevenlabs_model: str = "scribe_v2"
    enable_diarization: bool = True
    diarization_speaker_count: int = 2
```

If `stt.yaml` doesn't exist, audio processing is disabled for that user.

---

## Changes to step3.py

### New constructor parameter

```python
def __init__(self, root: Path, service_url: str, stt_url: Optional[str] = None, timeout: float = 900.0, ...)
```

`stt_url` is optional — if None, audio files get a 0-byte output (same as current error behavior) with a warning log.

### New method: `_process_audio`

```python
async def _process_audio(
    self, record: Record, file_bytes: bytes, filename: str, content_type: str,
) -> Optional[dict]:
```

Orchestrates the full audio flow:

1. **Load SttConfig** from `self.root / "stt.yaml"`. If missing → return None (log warning).
2. **Load contexts** for classify calls. Use `record.context_field_names` or fall back to `self.context_field_names` (passed from orchestrator via app.py). For the classify endpoints, contexts must be in the format the mrdocument service expects (list of context dicts with field_names). Use the same bridge as orchestrator: read from SorterContextManager at setup time and pass through.
   - Simpler approach: the Processor doesn't need to rebuild full contexts. Just send `filename` and `contexts: []` to `/classify_audio`. If the user has context configs, the orchestrator already loaded `context_field_names`. But `/classify_audio` and `/classify_transcript` need the full context objects (with candidates etc.), not just field names.
   - **Decision**: the Processor receives a `contexts` parameter (the serialized context list for API calls) at construction time, loaded by the orchestrator from `SorterContextManager.get_contexts_for_api()`. This mirrors v1's approach.
3. **Classify audio** — `POST {service_url}/classify_audio` with `{filename, contexts}`. Timeout: 120s, 2 retries. Optional (failure just means no keyterms).
4. **First STT pass** — `POST {stt_url}/transcribe` with multipart form: file, language, elevenlabs_model, enable_diarization, diarization_speaker_count, optional keyterms JSON. Timeout: 1800s. Required (failure → return None).
5. **Validate transcript** — must have non-empty `segments`. Else return None.
6. **Intro two-pass** (if "intro" in filename, case-insensitive):
   - `POST {service_url}/classify_transcript` with `{transcript, filename, contexts}`. Timeout: 300s, 2 retries. Optional.
   - If got keyterms back: second `POST {stt_url}/transcribe` with updated keyterms + speaker count. Optional (fall back to first pass).
   - Build `pre_classified` dict if classification succeeded.
7. **Process transcript** — `POST {service_url}/process_transcript` with `{transcript, filename, contexts, pre_classified?}`. Timeout: 1800s. Required (failure → return None).
8. **Return** the response dict (same shape as `/process` response: `metadata`, `pdf`, etc.).

### Modified `process_one`

Change the existing `process_one` to branch on audio vs document:

```python
ext = source_path.suffix.lower()
if _is_audio(ext):
    result = await self._process_audio(record, file_bytes, source_path.name, content_type)
else:
    result = await self._call_service(file_bytes, source_path.name, content_type, "document")
```

The rest (output writing, sidecar, error handling) stays the same.

### Retry logic

Reuse the existing `_call_service` retry pattern (exponential backoff, configurable retries) but as a lower-level helper for individual HTTP calls:

```python
async def _call_with_retry(
    self, session: aiohttp.ClientSession, method: str, url: str,
    timeout: float = None, max_retries: int = None, **kwargs,
) -> Optional[aiohttp.ClientResponse]:
```

This replaces the v1 `call_service_with_retry` function. Both `_call_service` (documents) and `_process_audio` use it.

---

## Changes to orchestrator.py

### Pass contexts to Processor

The Processor needs the full context objects (not just field names) for the `/classify_audio` and `/classify_transcript` endpoints. Add a `contexts_for_api` parameter:

```python
# In orchestrator __init__:
self.processor = Processor(
    root, service_url, stt_url=stt_url,
    timeout=processor_timeout, contexts=contexts_for_api,
)
```

### New constructor parameter: `stt_url`

```python
def __init__(self, ..., stt_url: Optional[str] = None, contexts_for_api: Optional[list] = None):
```

---

## Changes to app.py

### Read STT_URL from environment

```python
stt_url = os.environ.get("STT_URL")  # Optional, None disables audio
```

### Pass to orchestrator

```python
watcher = DocumentWatcherV2(
    root=user_root, db=db, service_url=mrdocument_url,
    stt_url=stt_url, contexts_for_api=contexts_for_api, ...
)
```

### Load contexts_for_api

In `setup_user()`, after loading `SorterContextManager`:

```python
contexts_for_api = None
if context_manager.load():
    context_field_names = context_field_names_from_sorter(context_manager)
    contexts_for_api = context_manager.get_contexts_for_api()
```

---

## Tests: additions to test_step3.py

### Mock STT server

Add a `MockSTTService` alongside the existing `MockService`:

```python
class MockSTTService:
    def __init__(self):
        self.app = web.Application()
        self.app.router.add_post("/transcribe", self.handle_transcribe)
        self.transcript = {"segments": [{"text": "mock transcript", "start": 0.0, "end": 5.0, "speaker": "SPEAKER_00"}]}

    async def handle_transcribe(self, request):
        return web.json_response({"transcript": self.transcript})
```

### New test cases

- **Audio success**: place .mp3 in incoming, mock STT returns transcript, mock service returns processed result → output file + sidecar written
- **Audio no stt.yaml**: no stt.yaml in user root → 0-byte output, warning logged
- **Audio no stt_url**: Processor created without stt_url → 0-byte output, warning logged
- **Audio STT failure**: mock STT returns 500 → 0-byte output
- **Audio STT empty transcript**: mock STT returns `{"transcript": {"segments": []}}` → 0-byte output
- **Audio intro two-pass**: filename contains "intro", mock service `/classify_transcript` returns keyterms, second STT pass runs → final transcript used
- **Audio intro fallback**: second STT pass fails → first pass transcript used for process_transcript
- **Audio classify_audio failure**: classify returns 500 → STT still called (without keyterms), processing continues
- **Audio process_transcript failure**: STT succeeds but process_transcript returns 500 → 0-byte output

### Existing tests unchanged

All existing document tests must continue to pass. The `Processor` constructor gains optional parameters but defaults keep backward compatibility.

---

## Container changes

None required. The `STT_URL` env var is already set on the watcher container in both:
- `docker-compose.service-mock.yaml` (line 106): `STT_URL: http://mock-stt:8000`
- `docker-compose.yaml` (production): `STT_URL: http://stt:8000`

The mock STT service (`mock_stt.py`) already implements `POST /transcribe` with canned responses.

The monolithic test container (`supervisord.service-mock.conf`) already runs `mock-stt` on port 8001. The entrypoint.sh sets `STT_URL=http://localhost:8001`.

Only one env var addition needed in entrypoint.sh (monolithic container):

```bash
export STT_URL=${STT_URL:-http://localhost:8001}
```

This is already present — the v1 watcher reads it. The v2 app.py just needs to read it too (covered in app.py changes above).
