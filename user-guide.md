# MrDocument -- User Guide

## What is MrDocument?

MrDocument is an AI-powered document management system. You drop files into a folder, and MrDocument automatically:

1. Classifies the document (determines context, type, sender, date, etc.).
2. Renames it using a consistent naming pattern.
3. Sorts it into the correct folder hierarchy.
4. Creates smart folder links for quick access.
5. For audio/video files: transcribes them and processes the transcript.

All you need to do is configure your contexts (what kinds of documents you have) and drop files in.


## Getting Started

### Folder Structure

Your MrDocument root directory contains these folders:

```
mrdocument/
  incoming/       <-- Drop new files here
  processed/      <-- AI-classified files awaiting your review
  reviewed/       <-- Files you've approved, awaiting sorting
  sorted/         <-- Final destination, organized by context
  archive/        <-- Original source files (permanent storage)
  reset/          <-- Drop processed files here to re-sort them
  error/          <-- Files that failed processing
  duplicates/     <-- Duplicate copies of source files
  trash/          <-- Drop files here to delete them
  void/           <-- System trash (deleted/orphaned files)
  missing/        <-- Source files whose processed results are missing
```


## How Files Flow

### The Standard Pipeline

```
incoming/ --> processed/ --> reviewed/ --> sorted/
```

1. **Drop a file into `incoming/`.** Any supported file type works.
2. **MrDocument processes it.** The AI extracts metadata (context, date, type, sender, etc.) and renames the file. The result appears in `processed/`.
3. **Review the file in `processed/`.** Check that the classification and filename are correct. If satisfied, move it to `reviewed/`.
4. **MrDocument sorts it.** The file is automatically moved from `reviewed/` into the correct location under `sorted/`, organized by your configured folder hierarchy.

The original source file is always preserved in `archive/`.

### Quick Sort (Skip Review)

You can bypass the review step by dropping files directly into `sorted/`:

- **Correct context folder:** Drop into `sorted/{context}/`. MrDocument classifies it and places it in the right subfolder.
- **Any location in sorted/:** MrDocument detects the context from the folder path and classifies accordingly.

### What Happens in Each Folder

| Folder | What you do | What MrDocument does |
|--------|------------|---------------------|
| `incoming/` | Drop new files here | Classifies, renames, moves result to `processed/`. Original goes to `archive/`. |
| `processed/` | Review AI classification | Nothing -- waits for you to move to `reviewed/`. |
| `reviewed/` | Move approved files here | Sorts into `sorted/{context}/{subfolders}/`. |
| `sorted/` | Browse your organized files; drop files for quick sort | Classifies and renames in-place; manages smart folder links. |
| `archive/` | Read-only reference | Stores all original source files permanently. |
| `trash/` | Move files here to delete | Moves all associated files to `void/` and removes the record. |
| `reset/` | Drop processed files here | Triggers filename recomputation and re-sorting into `sorted/`. |
| `error/` | Check failed files | Files that couldn't be processed end up here. You can move them back to `incoming/` to retry. |
| `duplicates/` | Check for duplicates | When the same source file appears multiple times, extras go here. |

### Reset (Re-Sort a File)

If you want MrDocument to recompute the filename and re-sort a file:

1. Copy the file from `processed/` to `reset/`.
2. MrDocument recomputes the filename using current metadata and context config.
3. The file is moved from `reset/` to the correct location under `sorted/`.

This is useful after changing your context configuration (e.g., adding new fields or renaming patterns) -- you can reset files to get updated filenames without reprocessing them through the AI.

### File Rename and Recovery

- If you **rename a file in `sorted/`**, MrDocument adopts your new filename.
- If you **move a file to a different context folder** in `sorted/`, MrDocument updates the context.
- If a processed file **disappears** from `sorted/`, MrDocument marks it as missing. If the file reappears, it's automatically recognized.
- If processing **fails**, the source file is moved to `error/`. You can move it back to `incoming/` to retry.


## Supported File Types

### Documents
PDF, DOCX, RTF, TXT, Markdown, EML (email), HTML, and common image formats (JPG, PNG, GIF, TIFF, BMP, WebP).

### Audio and Video
FLAC, WAV, MP3, OGG, WebM, MP4, M4A, MKV, AVI, MOV.

Audio and video files are transcribed using speech-to-text, then the transcript is classified and sorted as a TXT file. A symlink to the original audio file is placed next to the transcript.

### Unsupported Files
Files with unsupported extensions (fonts, spreadsheets, binaries, etc.) are automatically moved to `error/`.


## Configuration Files

### Context Configuration: `sorted/{context}/context.yaml`

This is the main configuration file. Each context represents a category of documents (e.g., "work", "personal", "medical").

```yaml
name: arbeit
description: "Business documents: invoices, contracts, and proposals"

filename: "{context}-{type}-{date}-{sender}"
audio_filename: "{context}-{date}-{sender}-{type}"

fields:
  type:
    instructions: "Determine the document type based on content."
    candidates:
      - "Rechnung"
      - "Vertrag"
      - "Angebot"
    allow_new_candidates: false

  sender:
    instructions: "Determine the sender or organization."
    candidates:
      - "Schulze GmbH"
      - "Fischer AG"
    allow_new_candidates: true

filename_keywords:
  - "schulze"
  - "keller"

folders:
  - "context"
  - "sender"
```

**Fields explained:**

| Field | Purpose |
|-------|---------|
| `name` | Unique context identifier. Must match the folder name in `sorted/`. |
| `description` | Helps the AI understand what this context is about. |
| `filename` | Template for renaming files. Uses `{field_name}` placeholders. |
| `audio_filename` | Alternative template for audio transcripts (optional). |
| `fields` | Metadata fields the AI should extract. Each has instructions and candidate values. |
| `allow_new_candidates` | If `true`, the AI can suggest values not in the candidates list. |
| `filename_keywords` | Keywords to look for in document content and include in the filename. |
| `folders` | Determines the subfolder hierarchy under `sorted/{context}/`. |

**Adding a new context:** Create `sorted/{new_context}/context.yaml`. MrDocument detects it automatically.

**Modifying a context:** Edit the YAML file. MrDocument reloads on the next cycle.

**Removing a context:** Delete the folder from `sorted/`. Existing files are not affected.

### Smart Folders: `sorted/{context}/smartfolders.yaml`

Smart folders create symlinks to documents matching specific conditions, giving you multiple views of the same files.

```yaml
smart_folders:
  rechnungen:
    condition:
      field: "type"
      value: "Rechnung"

  keller_rechnungen:
    condition:
      operator: "and"
      operands:
        - field: "type"
          value: "Rechnung"
        - field: "sender"
          value: "Keller.*"
    filename_regex: "2025"
```

**Conditions:**
- **Simple:** `{field: "type", value: "Rechnung"}` -- matches when the type field equals "Rechnung" (case-insensitive regex).
- **AND:** All sub-conditions must match.
- **OR:** At least one sub-condition must match.
- **NOT:** The sub-condition must not match.
- **filename_regex:** Additional filter on the filename (optional, case-insensitive search).

Smart folders appear as subdirectories containing symlinks. The actual files remain in their original location.

### Root-Level Smart Folders: `smartfolders.yaml`

Root-level smart folders place symlinks at arbitrary locations outside of `sorted/`.

```yaml
smart_folders:
  - name: invoices
    context: arbeit
    path: /home/user/Desktop/Invoices
    condition:
      field: "type"
      value: "Rechnung"

  - name: medical
    context: privat
    path: ./medical_docs
    condition:
      field: "type"
      value: "Arztbrief"
```

- `path` can be absolute or relative to the mrdocument root.
- Each entry must specify which `context` it applies to.
- The directory is created automatically if it doesn't exist.

### STT Configuration: `stt.yaml`

Required for audio/video file processing. Place in the mrdocument root.

```yaml
language: "de-DE"
elevenlabs_model: "scribe_v2"
enable_diarization: true
diarization_speaker_count: 2
```

| Field | Default | Description |
|-------|---------|-------------|
| `language` | `de-DE` | Language for transcription. |
| `elevenlabs_model` | `scribe_v2` | STT model to use. |
| `enable_diarization` | `true` | Identify different speakers. |
| `diarization_speaker_count` | `2` | Expected number of speakers. |

Without this file, audio files are skipped (moved to error/).

### Generated Data: `sorted/{context}/generated.yaml`

This file is managed automatically by MrDocument. It stores new candidates and clues discovered during processing.

- **Candidates:** New field values the AI discovered (when `allow_new_candidates: true`).
- **Clues:** Hints the AI learned about field values (e.g., "invoices from Schulze usually mention 'Projekt Alpha'").

You can edit this file to remove incorrect suggestions, but normally it's managed by the system.


## Audio and Video Files

### Processing Flow

1. Drop an audio/video file into `incoming/`.
2. MrDocument sends it to the speech-to-text service for transcription.
3. For files with "intro" in the filename: a two-pass transcription is performed with improved accuracy.
4. The transcript is classified and sorted as a `.txt` file.
5. A symlink to the original audio file is placed next to the transcript.

### Audio Links

After transcription, you'll see two files in `sorted/`:
```
sorted/arbeit/Schulze_GmbH/
  arbeit-Besprechung-2025-03-01-Schulze_GmbH.txt     <-- Transcript
  arbeit-Besprechung-2025-03-01-Schulze_GmbH.m4a     <-- Symlink to archive/original.m4a
```

The audio link follows the transcript: if you rename or move the transcript, the link is updated automatically.

### Intro Two-Pass

Files with "intro" in their filename get enhanced processing:
1. First transcription pass (standard).
2. AI classifies the transcript to get better context, keyterms, and speaker count.
3. Second transcription pass with improved parameters.

This produces significantly better results for recordings that start with an introduction segment.


## Smart Folders in Detail

### Context-Level Smart Folders

Located inside the sorted folder hierarchy. Example:

```
sorted/arbeit/Schulze_GmbH/
  rechnungen/                          <-- Smart folder
    arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf  --> ../arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf
  arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf     <-- Actual file
  arbeit-Vertrag-2025-02-01-Schulze_GmbH.pdf      <-- Not in rechnungen (type != Rechnung)
```

### Root-Level Smart Folders

Located at arbitrary paths. Example:

```
/home/user/Desktop/Invoices/
  arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf  --> ../../mrdocument/sorted/arbeit/Schulze_GmbH/arbeit-...pdf
```

### Important Notes

- Smart folders contain **only symlinks**. The actual files stay in `sorted/`.
- You can place your own files in smart folder directories -- MrDocument will never touch non-symlink files.
- Broken or stale symlinks are cleaned up automatically.
- If a file no longer matches a smart folder's condition (e.g., metadata changed), its symlink is removed.


## Duplicate Handling

When the same source file appears multiple times:
- The first copy is processed normally.
- Additional copies are moved to `duplicates/`, preserving the original path structure.

Example: If `incoming/invoice.pdf` is a duplicate of an already-processed file:
```
duplicates/incoming/invoice.pdf
```


## Error Recovery

### Failed Processing

If AI processing fails:
1. The source file is moved to `error/`.
2. To retry: move the file from `error/` back to `incoming/`.

### Missing Files

If a processed file disappears from `sorted/`:
1. MrDocument marks the record as "missing".
2. If the file reappears at its expected location, it's automatically recognized.
3. If you drop a new copy of the source into `incoming/`, it's reprocessed.

### Missing Files Recovery

When a processed file goes missing, its source is moved from `archive/` to `missing/` so you can find it easily. To reprocess, move the source file from `missing/` back to `incoming/`.


## Cost Tracking

MrDocument tracks AI API usage costs per user. Cost data is written to:
```
/costs/{username}/mrdocument_costs.json
```

The file contains per-day and total usage:
- Input and output tokens per model.
- USD cost per model.
- Number of documents processed.
- Cost per document.


## Multi-User Support

MrDocument supports multiple users simultaneously. Each user has their own:
- Root directory (e.g., `/sync/alice/mrdocument/`).
- Context configurations.
- Smart folder definitions.
- Database records (isolated by username).
- Cost tracking.

New user directories are discovered automatically when they appear under the sync root.


## Tips

- **Batch processing:** Drop multiple files into `incoming/` at once. MrDocument processes them concurrently (up to 5 by default).
- **Quick classification:** Drop files directly into `sorted/{context}/` to skip the review step.
- **Context from folder:** When you drop a file into `sorted/arbeit/`, MrDocument knows it's a business document and only considers that context.
- **Filename conventions:** The AI respects your filename template. Dates are formatted as YYYY-MM-DD. Special characters and accents are normalized.
- **Config hot reload:** Edit `context.yaml` or `smartfolders.yaml` and MrDocument picks up changes automatically -- no restart needed.
- **Syncthing friendly:** MrDocument uses atomic file operations and ignores Syncthing temporary files, making it safe for synced folders.
