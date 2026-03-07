# Syncthing Document Processor

Automated document processing pipeline using Syncthing for file synchronization and MrDocument for OCR and metadata extraction.

**Supported formats:**
- PDF files
- EML files (converted to PDF)
- HTML files (converted to PDF)
- DOCX files (native processing)
- Images (JPEG, PNG, GIF, TIFF, BMP, WebP)
- ZIP files (extracted and processed)

**Features:**
- ✅ Multi-user support
- ✅ Context-based extraction with AI
- ✅ Strict enum enforcement via JSON schema
- ✅ Auto-learning of new metadata values
- ✅ Smart filename generation
- ✅ Per-context configuration
- ✅ Folder-based batch processing (skip context determination)

## Architecture

```
Mac Client (Finder)
    ↓ Save document to ~/Synced/incoming/
Syncthing sync
    ↓
Server: /sync/{username}/incoming/
    ↓ Python watcher detects new file
MrDocument service
    ↓ 1. Determine context (AI pass 1)
    ↓ 2. Extract metadata (AI pass 2)
    ↓ 3. OCR if needed
Server: /sync/{username}/processed/result.pdf
    ↓ Syncthing sync back
Mac Client: ~/Synced/processed/result.pdf
```

## File Structure

```
/sync/{username}/
├── contexts.yaml          # REQUIRED: List of context files
├── work.yaml              # Context definition
├── private.yaml           # Another context
├── config.yaml            # User configuration (auto-created)
├── incoming/              # Drop files here
├── processed/             # Results appear here
├── archive/               # Originals after processing
├── error/                 # Failed documents moved here
└── duplicates/            # Duplicate PDFs moved here
```

## Quick Start

### 1. Create contexts.yaml (required)

```yaml
# /sync/alice/contexts.yaml
- work.yaml
- private.yaml
```

### 2. Create context files

**work.yaml:**
```yaml
name: work
description: Work-related business documents

type:
  candidates:
    - "Invoice"
    - "Contract"
    - "Receipt"
  allow_new_items: true

sender:
  candidates:
    - "Acme Corp"
  allow_new_items: true
```

**private.yaml:**
```yaml
name: private
description: Personal documents and bills

type:
  candidates:
    - "Bill"
    - "Statement"
    - "Policy"
  allow_new_items: false  # STRICT: must choose from list
```

### 3. Drop files in incoming/

Documents are automatically processed with context-aware metadata extraction.

### 4. Drop folders for batch processing

Drop a folder named after a context (e.g., `work/`) into `incoming/`:

```
incoming/
└── work/           # Folder name = context name
    ├── doc1.pdf
    ├── doc2.pdf
    └── invoice.eml
```

- All files in the folder are processed using the `work` context (no AI context determination)
- If the folder name doesn't match any context, the entire folder is moved to `error/`
- After processing, the folder is removed (if empty) or moved to `error/` (if files remain)

### 5. Nested folders for pre-determined field values

If your context has a `folders` configuration for sorting, you can use the same structure in reverse to lock field values:

```yaml
# work.yaml
name: work
folders:
  - context
  - sender
  - topic
```

Drop files in a matching nested structure:

```
incoming/
└── work/                    # context = work
    └── acme_corp/           # sender = Acme Corp (matched from candidates)
        └── project_alpha/   # topic = Project Alpha
            └── invoice.pdf
```

**How it works:**
- Folder names are matched against field candidates (using `short` form or `name`)
- Matched fields are locked - the AI won't try to determine them
- Clues associated with the matched candidate are provided to the AI as context
- Remaining fields are extracted normally by the AI
- Files at any level of the structure are processed with all parent folders as locked fields

This is useful when you manually organize files and want to skip AI classification for known values.

## Context Configuration

Each context file defines how documents in that context should be processed.

### Required Fields

```yaml
name: work                              # Unique identifier
description: Work-related documents     # Helps AI determine context
```

### Optional Fields

```yaml
filename_pattern: "{type}-{date}-{sender}-{topic}"  # Override default pattern
```

### Field Configuration

Each metadata field (type, sender, topic, subject, keywords) can have:

```yaml
type:
  candidates:              # List of allowed values
    - "Invoice"
    - name: "Bill (incoming)"   # Object with name/short
      short: "Bill"
  blacklist:               # Values to reject
    - "Unknown"
    - "Document"
  allow_new_items: true    # Can AI invent new values? (default: true)
  instructions: |          # Custom extraction instructions
    Choose based on document purpose.
```

### Candidates Format

**Simple strings:**
```yaml
candidates:
  - "Invoice"
  - "Receipt"
  - "Contract"
```

**Name/short objects** (AI sees descriptive name, metadata stores short):
```yaml
candidates:
  - name: "Invoice (outgoing - sent to client)"
    short: "Invoice"
  - name: "Bill (incoming - owed to vendor)"
    short: "Bill"
```

### Strict Mode

Set `allow_new_items: false` to force AI to choose from candidates:

```yaml
topic:
  candidates:
    - "Utilities"
    - "Insurance"
    - "Banking"
  allow_new_items: false  # AI MUST choose from list (enforced via JSON schema)
```

**Note:** `allow_new_items: false` with empty candidates is an error.

### Auto-Learning

When AI invents a new value (and `allow_new_items: true`):
1. The value is automatically added to the context's `candidates`
2. The context file is updated on disk
3. Future documents can match this value

## User Configuration

### config.yaml

Auto-created on first run. Controls folder names and default filename pattern:

```yaml
# Folder names
incoming_folder: "incoming"
processed_folder: "processed"
archive_folder: "archive"
error_folder: "error"
duplicates_folder: "duplicates"

# Default filename pattern
# Available: {context}, {type}, {date}, {sender}, {topic}, {subject}
filename_pattern: "{type}-{date}-{sender}-{topic}-{subject}"

# Force AI output language (optional)
# primary_language: "German"
```

### Duplicate Detection

Before moving a processed PDF to the `processed/` folder, the watcher compares it against existing files with the same context and date using perceptual image hashing. If a file is determined to be a duplicate, it is moved to the `duplicates/` folder instead.

**How it works:**
1. Extract date from the new document's metadata
2. Find existing PDFs in `processed/` with matching date (and context if `{context}` is in filename pattern)
3. Compare using perceptual hashes (visual similarity) with tolerance for minor differences
4. If duplicate found, save to `duplicates/` instead of `processed/`

**Configuration:**
- Change the duplicates folder name via `duplicates_folder` in config.yaml
- Duplicate detection only applies to PDF outputs (not DOCX or text files)

## How It Works

### Two-Pass AI Extraction

1. **Context Determination (Pass 1)**
   - AI receives document text and list of contexts
   - Must choose from available contexts (enum enforced)
   - Returns context name

2. **Metadata Extraction (Pass 2)**
   - AI uses context-specific configuration
   - Fields with `allow_new_items: false` use enum constraints
   - Fields with `allow_new_items: true` allow free-form with suggestions

### Instruction Hierarchy

For each field, the AI receives:

1. **Base instruction** (automatic):
   - Strict: "You MUST choose from the provided list"
   - Flexible: "Choose from the list if applicable, or create new"

2. **Field instruction** (from config or default):
   - Custom: Your `instructions` value
   - Default: Built-in semantics for that field type

### name→short Mapping

When candidates use `{name, short}` format:
- AI sees: "Invoice (outgoing - sent to client)"
- Metadata stores: "Invoice"
- Filename uses: "invoice"

## Complete Example

### contexts.yaml
```yaml
- work.yaml
- private.yaml
```

### work.yaml
```yaml
name: work
description: Work-related business documents including invoices and contracts

filename_pattern: "{type}-{date}-{sender}-{topic}"

type:
  candidates:
    - name: "Invoice (outgoing)"
      short: "Invoice"
    - name: "Bill (incoming)"
      short: "Bill"
    - "Contract"
    - "Receipt"
  blacklist:
    - "Unknown"
  allow_new_items: true
  instructions: |
    Choose based on document purpose and cash flow direction.

sender:
  candidates:
    - "Acme Corp"
    - "Globex Inc"
  allow_new_items: true

topic:
  candidates:
    - "Project Alpha"
    - "Project Beta"
  allow_new_items: true
  instructions: |
    Use project name or client name.
```

### private.yaml
```yaml
name: private
description: Personal documents like utility bills, insurance, and statements

filename_pattern: "{context}-{type}-{date}-{sender}"

type:
  candidates:
    - "Bill"
    - "Statement"
    - "Policy"
    - "Receipt"
    - "Notice"
  allow_new_items: false  # Strict

topic:
  candidates:
    - "Utilities"
    - "Insurance"
    - "Banking"
    - "Medical"
    - "Taxes"
  allow_new_items: false  # Strict

sender:
  allow_new_items: true  # Can learn new senders
```

## Setup

### 1. Environment

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

### 2. Start Services

```bash
docker compose --profile default up -d ocrmypdf mrdocument syncthing
```

### 3. Create User Folder

```bash
mkdir -p ~/data/syncthing/sync/alice/{incoming,processed,archive}
```

### 4. Create Configuration

Create `contexts.yaml` and at least one context file (see examples above).

### 5. Configure Syncthing

Access web UI at `https://parmenides.net/syncthing/` and share folders.

## Monitoring

```bash
# View logs
docker compose logs -f syncthing

# Watch processing
docker compose logs -f syncthing | grep -E "(Processing|Saved|context)"
```

### Log Output Example

```
[alice] Loaded 2 context(s): work, private
[alice] Processing: invoice.pdf (active: 1)
[alice] Using 2 context(s)
[alice] Context: work, Generated filename: invoice-2024-01-15-acme_corp-project_alpha.pdf
[alice] Saved: invoice-2024-01-15-acme_corp-project_alpha.pdf
[alice] Recorded new sender 'New Client Inc' in context 'work'
```

## Error Handling

| Condition | Result |
|-----------|--------|
| No `contexts.yaml` | Error: "At least one context required" |
| Empty `contexts.yaml` | Error: "At least one context required" |
| `allow_new_items: false` + empty candidates | Error: Configuration error |
| Invalid YAML | Error with details |
| Context file not found | Warning, context skipped |

## Troubleshooting

### Files Not Processing

1. Check contexts.yaml exists and lists valid context files
2. Verify context files have `name` and `description`
3. Check logs: `docker compose logs syncthing`

### Wrong Context Detected

1. Improve context descriptions (be specific)
2. Reduce number of contexts (2-4 optimal)
3. Add distinctive keywords to descriptions

### Values Not Learning

1. Verify `allow_new_items: true` (or not set)
2. Check context file is writable
3. Look for "Recorded new" in logs

## See Also

- `contexts.yaml.example` - Index file example
- `work.yaml.example` - Context file example
- `private.yaml.example` - Another context example
- `MULTI_USER_SETUP.md` - Multi-user configuration
