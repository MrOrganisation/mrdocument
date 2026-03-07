# Root-Level Smart Folders — User Guide

## What Are Smart Folders?

Smart folders are directories that automatically contain symbolic links to your documents based on conditions you define. They give you different "views" of your sorted documents without duplicating files.

There are two types:

1. **Context-level smart folders** (existing): Configured per context in `sorted/{context}/smartfolders.yaml`. Creates a subdirectory in each leaf folder of `sorted/` containing links to matching files.

2. **Root-level smart folders** (new): Configured once in a single file at your mrdocument root. Creates a folder at any location you choose — including outside the mrdocument tree — containing links to matching files from across all leaf folders of a context.

## Setting Up Root-Level Smart Folders

Create a file called `smartfolders.yaml` in your mrdocument root directory (the same directory that contains `incoming/`, `sorted/`, etc.).

### Example

```yaml
smart_folders:
  rechnungen_alle:
    context: arbeit
    path: /home/user/Desktop/Rechnungen
    condition:
      field: type
      value: Rechnung

  briefe:
    context: privat
    path: briefe_sammlung
    condition:
      field: type
      value: Brief
    filename_regex: "\.pdf$"
```

This creates:
- `/home/user/Desktop/Rechnungen/` — containing links to all "Rechnung" documents from the "arbeit" context.
- `{mrdocument_root}/briefe_sammlung/` — containing links to all PDF "Brief" documents from the "privat" context.

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `context` | Yes | Which context's documents to include (e.g., `arbeit`, `privat`). |
| `path` | Yes | Where to create the smart folder. Absolute paths work anywhere on your system. Relative paths are relative to the mrdocument root. |
| `condition` | At least one of condition or filename_regex | Match documents by metadata. See "Conditions" below. |
| `filename_regex` | At least one of condition or filename_regex | Match documents by filename (case-insensitive regex search). |

### Conditions

**Simple field match** — matches when the document's metadata field equals the value (case-insensitive regex):
```yaml
condition:
  field: type
  value: Rechnung
```

**AND** — all sub-conditions must match:
```yaml
condition:
  operator: and
  operands:
    - field: type
      value: Rechnung
    - field: sender
      value: Schulze.*
```

**OR** — at least one sub-condition must match:
```yaml
condition:
  operator: or
  operands:
    - field: type
      value: Rechnung
    - field: type
      value: Angebot
```

**NOT** — negates a condition:
```yaml
condition:
  operator: not
  operands:
    - field: type
      value: Vertrag
```

## What Happens

### When a document is processed and sorted
If the document's context and metadata match a root-level smart folder's conditions, a symbolic link is automatically created in the smart folder's directory pointing to the file in `sorted/`.

### When a document's metadata changes
If you move or rename a document in `sorted/` (which updates its metadata), the system re-evaluates all smart folders. Links are added to newly matching folders and removed from non-matching ones.

### When you edit the config
Changes to `smartfolders.yaml` are detected automatically. The system reloads the configuration and re-evaluates all smart folders on the next cycle.

### Name collisions
If two documents from different leaf folders have the same filename, the first one gets the symlink. This is rare in practice since assigned filenames include context and metadata.

### Cleanup
The system periodically cleans up:
- Broken symlinks (target file was deleted or moved) are removed.
- Only symlinks pointing into `sorted/` are touched. If you manually place symlinks or files in the smart folder directory pointing elsewhere, they are left alone.
- Regular files you place in the smart folder directory are never touched.

## Configuration Files

All configuration files live in the mrdocument root directory (the same directory containing `incoming/`, `sorted/`, etc.) unless noted otherwise.

| File | Purpose | Required |
|------|---------|----------|
| `sorted/{context}/context.yaml` | Defines a context: name, metadata fields, filename pattern, folder hierarchy. One file per context. | Yes (at least one context) |
| `sorted/{context}/smartfolders.yaml` | Defines context-level smart folders (symlink subdirectories within leaf folders of `sorted/`). | No |
| `contexts.yaml` | Legacy fallback: index file listing context YAML files in the mrdocument root (e.g., `["work.yaml", "private.yaml"]`). Only used when no `sorted/{context}/context.yaml` files exist. | No (legacy) |
| `smartfolders.yaml` | Root-level smart folders at arbitrary paths. See "Setting Up Root-Level Smart Folders" above. | No |
| `stt.yaml` | Audio transcription settings: language, model, diarization options. Required for audio file processing — without it, audio files are skipped. | Only for audio |

### Context Configuration (`sorted/{context}/context.yaml`)

```yaml
name: arbeit
description: Business documents
filename: "{context}-{type}-{date}-{sender}"
audio_filename: "{context}-{date}-{sender}-{type}"
folders:
  - context
  - sender

fields:
  type:
    instructions: "Determine the document type."
    candidates:
      - "Rechnung"
      - "Vertrag"
    allow_new_candidates: false
  sender:
    instructions: "Determine the sender."
    candidates: []
    allow_new_candidates: true
```

- `filename` / `audio_filename`: Pattern for the assigned filename. Fields in `{braces}` are replaced with metadata values.
- `folders`: Which metadata fields determine the folder hierarchy under `sorted/{context}/`.
- `fields`: Metadata fields the AI extracts. `candidates` lists known values; `allow_new_candidates` controls whether the AI can suggest new ones.

### Context-Level Smart Folders (`sorted/{context}/smartfolders.yaml`)

```yaml
smart_folders:
  rechnungen:
    condition:
      field: type
      value: Rechnung
```

Same condition/filename_regex format as root-level smart folders (see above), but without `context` or `path` — the context is implicit from the directory, and symlinks are placed in leaf-folder subdirectories.

### Audio Configuration (`stt.yaml`)

```yaml
language: de-DE
elevenlabs_model: scribe_v2
enable_diarization: true
diarization_speaker_count: 2
```

If this file is missing, audio files dropped into `incoming/` are skipped entirely.

## Quick Reference: Folder Behavior Summary

| Folder | What to put there | What happens |
|--------|------------------|--------------|
| `incoming/` | Drop new files | Automatically classified, processed, and sorted. Original archived. |
| `sorted/` | Don't touch (managed by system) | Final location of processed documents. You can rename or move files within sorted/ — the system adapts. |
| `reviewed/` | Move files here after manual review | System sorts them into `sorted/` based on their filename. |
| `trash/` | Move files you want deleted | All copies (source + processed) moved to `void/`. |
| `archive/` | Don't touch (managed by system) | Original source files stored here permanently. |
| `processed/` | Don't touch (legacy) | Intermediate output location before sorting. |
| `error/` | Check for problems | Stray files and failed processing results end up here. |
| `void/` | Don't touch | Deleted files archive. Not watched by the system. |
| Smart folder (root-level) | Read-only view | Symbolic links to matching documents. You can add your own files — they won't be touched. |
