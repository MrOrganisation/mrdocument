# MrDocument

PDF OCR and metadata extraction service. Accepts PDF documents, processes them through OCR, extracts metadata using AI, and returns the OCR'd PDF with a suggested filename based on the extracted metadata.

## Architecture

- `server.py`: REST API server (HTTP, async connections)
- `ocr.py`: Interface to the OCRmyPDF service
- `ai.py`: Interface to Anthropic Claude for metadata extraction

## Installation

```bash
cd mrdocument
poetry install
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OCR_URL` | URL of the OCRmyPDF service | `http://ocrmypdf:5000` |
| `ANTHROPIC_API_KEY` | Anthropic API key | (required) |
| `ANTHROPIC_MODEL` | Claude model to use | `claude-sonnet-4-20250514` |
| `HOST` | Server bind address | `0.0.0.0` |
| `PORT` | Server port | `8000` |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` |

### AI Model Configuration

The AI model settings and prompt are configured in `mrdocument/config.yaml`:

```yaml
model:
  name: claude-sonnet-4-20250514  # Model to use
  max_tokens: 1024                 # Max response tokens

extraction:
  max_input_chars: 50000           # Max document text to send

prompt: |
  Analyze the following document text and extract metadata.
  ...
```

## Running

```bash
export ANTHROPIC_API_KEY="your-api-key"
poetry run mrdocument
```

## API

### Health Check

```
GET /health
```

Returns service health status.

**Response:**
```json
{
  "status": "healthy",
  "service": "mrdocument",
  "ocr_service": "healthy"
}
```

### Process Document

```
POST /process
```

Process a PDF document through OCR and extract metadata.

**Parameters (multipart/form-data):**
- `file` (required): PDF file to process
- `language` (optional): OCR language code (default: `eng`)
- `types` (optional): JSON filter for document types (array or object with candidates/blacklist)
- `senders` (optional): JSON filter for senders (array or object with candidates/blacklist)
- `topics` (optional): JSON filter for topics/dossiers (array or object with candidates/blacklist)
- `primary_language` (optional): Output language for AI responses (e.g., "German", "English")
- `topic_instructions` (optional): Custom instructions for how the topic should be determined (string)
- `contexts` (optional): JSON array of context definitions for two-pass extraction (see Context-Based Extraction below)

When `types`, `senders`, or `topics` filters are provided, the AI will attempt to match to known values. Only if no match is found will a new value be returned. The `topic_instructions` parameter allows you to provide additional guidance to the AI model on how to determine the topic field.

### Context-Based Extraction

The `contexts` parameter enables a two-pass AI extraction process:

1. **First pass**: The AI determines which context the document belongs to
2. **Second pass**: The AI extracts metadata using context-specific instructions

Each context definition must include:
- `name`: Context identifier (e.g., "work", "private", "project-X")
- `description`: Description to help the AI determine if a document belongs to this context
- `instructions` (optional): Object with context-specific instructions for metadata fields:
  - `type_instructions`: How to determine the document type in this context
  - `sender_instructions`: How to determine the sender in this context
  - `topic_instructions`: How to determine the topic in this context
  - `subject_instructions`: How to determine the subject in this context
  - `keywords_instructions`: How to determine keywords in this context

Example contexts JSON:
```json
[
  {
    "name": "work",
    "description": "Work-related documents from the company",
    "instructions": {
      "topic_instructions": "Use the project code or department name as the topic",
      "sender_instructions": "Use the full company/department name"
    }
  },
  {
    "name": "private",
    "description": "Personal documents like bills, letters, contracts",
    "instructions": {
      "topic_instructions": "Group by category (e.g., 'Utilities', 'Insurance', 'Banking')"
    }
  }
]
```

**Response:**
```json
{
  "filename": "invoice-2024-01-15-acme_corporation-order_12345.pdf",
  "pdf": "<base64-encoded-pdf>",
  "metadata": {
    "context": "work",
    "type": "Invoice",
    "date": "2024-01-15",
    "sender": "ACME Corporation",
    "topic": "Office Supplies 2024",
    "subject": "Order 12345",
    "keywords": ["payment", "net30", "supplies"]
  }
}
```

## Usage Examples

### curl

```bash
curl -X POST -F "file=@document.pdf" http://localhost:8000/process
```

With language:
```bash
curl -X POST -F "file=@document.pdf" -F "language=deu" http://localhost:8000/process
```

With known types and senders:
```bash
curl -X POST \
  -F "file=@document.pdf" \
  -F 'types=["Invoice", "Receipt", "Contract", "Letter"]' \
  -F 'senders=["ACME Corp", "Globex Inc", "Initech"]' \
  http://localhost:8000/process
```

With custom topic instructions:
```bash
curl -X POST \
  -F "file=@document.pdf" \
  -F 'topic_instructions=For legal documents, use the case number as the topic. For personal documents, use the subject matter.' \
  http://localhost:8000/process
```

With contexts (two-pass extraction):
```bash
curl -X POST \
  -F "file=@document.pdf" \
  -F 'contexts=[
    {
      "name": "work",
      "description": "Work-related documents from the company",
      "instructions": {
        "topic_instructions": "Use the project code or client name",
        "sender_instructions": "Use the full company/department name"
      }
    },
    {
      "name": "private",
      "description": "Personal documents like bills and letters",
      "instructions": {
        "topic_instructions": "Group by category like Utilities or Insurance"
      }
    }
  ]' \
  http://localhost:8000/process
```

### Python

```python
import requests
import base64
import json

with open('document.pdf', 'rb') as f:
    response = requests.post(
        'http://localhost:8000/process',
        files={'file': f},
        data={
            'language': 'eng',
            'types': json.dumps(['Invoice', 'Receipt', 'Contract']),
            'senders': json.dumps(['ACME Corp', 'Globex Inc']),
            'topic_instructions': 'Determine topic based on the project or matter reference in the document header.',
        }
    )

result = response.json()
suggested_filename = result['filename']
pdf_bytes = base64.b64decode(result['pdf'])

with open(suggested_filename, 'wb') as f:
    f.write(pdf_bytes)
```

With contexts:
```python
import requests
import base64
import json

contexts = [
    {
        "name": "work",
        "description": "Work-related documents from the company",
        "instructions": {
            "topic_instructions": "Use the project code or client name",
            "sender_instructions": "Use the full company/department name"
        }
    },
    {
        "name": "private",
        "description": "Personal documents like bills, letters, and contracts",
        "instructions": {
            "topic_instructions": "Group by category such as Utilities, Insurance, or Banking"
        }
    }
]

with open('document.pdf', 'rb') as f:
    response = requests.post(
        'http://localhost:8000/process',
        files={'file': f},
        data={
            'contexts': json.dumps(contexts),
        }
    )

result = response.json()
print(f"Context: {result['metadata']['context']}")
print(f"Topic: {result['metadata']['topic']}")
suggested_filename = result['filename']
```

## Testing

```bash
poetry run pytest
```

## Filename Format

Generated filenames follow the pattern: `type-date-sender-subject.pdf` (all lowercase)

- Parts are omitted if not extractable from the document
- Whitespace is replaced with `_`
- Hyphens in fields (except date) are replaced with `_`
- Umlauts are transliterated (ä→ae, ö→oe, ü→ue, etc.)
- Special characters unsuitable for filenames are removed
