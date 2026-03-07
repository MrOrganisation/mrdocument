# OCRmyPDF Service

This service provides a REST API for OCR processing of PDF files using OCRmyPDF.

## Endpoints

### Health Check
```
GET /health
```
Returns the health status of the service.

### OCR Processing
```
POST /ocr
```
Process a PDF file with OCR.

**Parameters (multipart/form-data):**
- `file` (required): PDF file to process
- `language` (optional): OCR language code (default: `eng`). Can be comma or plus-separated for multiple languages (e.g., `eng,deu` or `eng+deu`)
- `skip_text` (optional): Skip pages that already have text (default: `false`)
- `force_ocr` (optional): Force OCR on all pages, even if they already have text (default: `true`)
- `optimize` (optional): Optimize PDF size, 0-3 (default: `1`)
- `deskew` (optional): Deskew crooked scans (default: `false`)
- `clean` (optional): Clean pages before OCR (default: `false`)
- `return_text` (optional): Return both PDF (as base64) and extracted text as JSON (default: `false`)
- `text_only` (optional): Return only extracted text as JSON, without PDF (default: `false`)

**Response:**
- Default: Returns the OCR'd PDF file as `application/pdf`
- With `text_only=true`: Returns JSON with `text` and `filename` fields
- With `return_text=true`: Returns JSON with `pdf` (base64-encoded), `text`, and `filename` fields

### Service Information
```
GET /info
```
Returns OCRmyPDF version and available Tesseract languages.

## Usage Examples

### Using curl

Basic OCR:
```bash
curl -X POST -F "file=@document.pdf" \
  https://parmenides.net/ocr/ocr \
  -o output.pdf
```

OCR with German language:
```bash
curl -X POST -F "file=@document.pdf" -F "language=deu" \
  https://parmenides.net/ocr/ocr \
  -o output.pdf
```

OCR with multiple languages and optimization:
```bash
curl -X POST -F "file=@document.pdf" \
  -F "language=eng,deu" \
  -F "optimize=2" \
  -F "deskew=true" \
  https://parmenides.net/ocr/ocr \
  -o output.pdf
```

Get only extracted text (no PDF):
```bash
curl -X POST -F "file=@document.pdf" -F "text_only=true" \
  https://parmenides.net/ocr/ocr
```

Get both PDF and text as JSON:
```bash
curl -X POST -F "file=@document.pdf" -F "return_text=true" \
  https://parmenides.net/ocr/ocr
```

Check available languages:
```bash
curl https://parmenides.net/ocr/info
```

### Using Python

```python
import requests

# Basic OCR
with open('document.pdf', 'rb') as f:
    response = requests.post(
        'https://parmenides.net/ocr/ocr',
        files={'file': f}
    )
    
if response.ok:
    with open('output.pdf', 'wb') as out:
        out.write(response.content)

# OCR with options
with open('document.pdf', 'rb') as f:
    response = requests.post(
        'https://parmenides.net/ocr/ocr',
        files={'file': f},
        data={
            'language': 'eng,deu',
            'optimize': '2',
            'deskew': 'true'
        }
    )

# Get only text
with open('document.pdf', 'rb') as f:
    response = requests.post(
        'https://parmenides.net/ocr/ocr',
        files={'file': f},
        data={'text_only': 'true'}
    )
    result = response.json()
    print(result['text'])

# Get both PDF and text
with open('document.pdf', 'rb') as f:
    response = requests.post(
        'https://parmenides.net/ocr/ocr',
        files={'file': f},
        data={'return_text': 'true'}
    )
    result = response.json()
    
    # Decode PDF from base64
    import base64
    pdf_bytes = base64.b64decode(result['pdf'])
    with open('output.pdf', 'wb') as out:
        out.write(pdf_bytes)
    
    # Access text
    print(result['text'])
```

### Using n8n

1. Use HTTP Request node
2. Set method to POST
3. URL: `http://ocrmypdf:5000/ocr`
4. Body Content Type: Multipart/Form-Data
5. Add form field: `file` with binary data

## Supported Languages

The service includes these Tesseract language packs:
- English (eng)
- German (deu)
- French (fra)
- Spanish (spa)
- Italian (ita)
- Portuguese (por)

Additional languages can be added by modifying `Dockerfile.ocrmypdf`.

## API Access

- Internal (from other containers): `http://ocrmypdf:5000`
- External (via nginx): `https://parmenides.net/ocr/`

## Volumes

- `/input`: Input directory (mounted at `~/data/ocrmypdf/input`)
- `/output`: Output directory (mounted at `~/data/ocrmypdf/output`)

These directories can be used for batch processing or shared access with other containers.
