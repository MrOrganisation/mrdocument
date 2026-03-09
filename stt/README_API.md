# STT API Documentation

HTTP API for speech-to-text transcription using ElevenLabs.

## Starting the Server

```bash
# Using the CLI
stt serve

# With custom host and port
stt serve --host 127.0.0.1 --port 8080
```

The server requires the following environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `ELEVENLABS_API_KEY` | Yes | ElevenLabs API key |

## Endpoints

### Health Check

```
GET /health
```

Returns server status and API key configuration.

**Response:**
```json
{
  "status": "healthy",
  "elevenlabs_key_set": true
}
```

---

### Transcribe

```
POST /transcribe
```

Transcribe an audio file and receive transcription as JSON.

**Request:** `multipart/form-data`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `file` | file | *required* | Audio file (supported: .flac, .wav, .mp3, .ogg, .webm, .mp4, .m4a, .mkv, .avi, .mov) |
| `language` | string | `de-DE` | Language code |
| `elevenlabs_model` | string | `scribe_v2` | ElevenLabs model (`scribe_v1`, `scribe_v1_experimental`, `scribe_v2`) |
| `keyterms` | string | `null` | JSON array of key terms to help transcription accuracy (e.g., `["Dr. Schmidt", "München"]`) |
| `enable_diarization` | boolean | `false` | Enable speaker diarization |
| `diarization_speaker_count` | integer | `2` | Expected number of speakers |
| `enable_word_timestamps` | boolean | `false` | Include word-level timestamps |

**Response:**
```json
{
  "transcript": {
    "language": "de-DE",
    "segments": [
      {
        "text": "Hello world.",
        "start": 0.0,
        "end": 1.5,
        "speaker": "speaker_0",
        "words": [...]
      }
    ]
  }
}
```

**Example:**
```bash
curl -X POST http://localhost:8000/transcribe \
  -F "file=@recording.m4a" \
  -F "language=de-DE" \
  -F "enable_diarization=true" \
  -F "diarization_speaker_count=3"
```

**Example with keyterms:**
```bash
curl -X POST http://localhost:8000/transcribe \
  -F "file=@recording.m4a" \
  -F "language=de-DE" \
  -F 'keyterms=["Dr. Schmidt", "München", "Empedokles"]'
```

---

## Response Fields

### transcript

The complete transcript from ElevenLabs including:
- Segment text
- Timestamps (start/end)
- Speaker labels (if diarization enabled)
- Word-level data (if word timestamps enabled)

---

## Error Responses

| Status | Description |
|--------|-------------|
| 400 | Bad request (unsupported format, invalid parameters, invalid keyterms JSON) |
| 500 | Server error (missing API keys, transcription failed) |

**Error format:**
```json
{
  "detail": "Error message describing the problem"
}
```

---

## Interactive Documentation

When the server is running, interactive API documentation is available at:

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc

---

## Example: Python Client

```python
import json
import requests

url = "http://localhost:8000/transcribe"

with open("recording.m4a", "rb") as f:
    response = requests.post(
        url,
        files={"file": ("recording.m4a", f, "audio/mp4")},
        data={
            "language": "de-DE",
            "enable_diarization": "true",
            "keyterms": json.dumps(["Dr. Schmidt", "München"]),
        },
    )

result = response.json()

# Save transcript JSON
with open("transcript.json", "w") as f:
    json.dump(result["transcript"], f, indent=2, ensure_ascii=False)
```

---

## Example: JavaScript/Node.js Client

```javascript
const fs = require('fs');
const FormData = require('form-data');
const fetch = require('node-fetch');

async function transcribe(audioPath) {
  const form = new FormData();
  form.append('file', fs.createReadStream(audioPath));
  form.append('language', 'de-DE');
  form.append('enable_diarization', 'true');
  form.append('keyterms', JSON.stringify(['Dr. Schmidt', 'München']));

  const response = await fetch('http://localhost:8000/transcribe', {
    method: 'POST',
    body: form,
  });

  const result = await response.json();
  return result;
}
```
