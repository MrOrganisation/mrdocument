# MrDocument

AI-powered document management system that automatically classifies, renames, and sorts documents into a structured folder hierarchy.

Drop files into `incoming/`, and MrDocument:
- Extracts metadata (context, type, sender, date) using AI
- Renames files using configurable patterns
- Sorts them into context-specific folder hierarchies
- Creates smart folder symlinks for cross-cutting views
- Transcribes audio/video files via speech-to-text

## Architecture

- **mrdocument-service** -- HTTP API for document classification, OCR, and metadata extraction (Python/FastAPI)
- **mrdocument-watcher** -- Filesystem watcher that orchestrates the pipeline (Rust)
- **PostgreSQL** -- Record persistence and state management
- **STT service** -- Speech-to-text for audio/video files (optional)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- A folder to use as the document root

### Local Setup (Docker)

```bash
# Start all services
docker compose -f docker-compose.yaml up -d

# The watcher monitors /sync/* for user directories.
# Create a user directory and copy example configs:
cp -r examples/ /path/to/sync/username/mrdocument/

# Drop a file
cp invoice.pdf /path/to/sync/username/mrdocument/incoming/
```

### Remote Setup (Syncthing)

MrDocument can run on a remote host with file sync via Syncthing:

1. Install and run the Docker stack on the remote host.
2. Set up Syncthing to sync the user's document root between local machine and remote host.
3. The watcher on the remote host picks up files synced from the local machine.
4. Processed results sync back to the local machine.

This setup lets you drop files on your local machine and have them classified and sorted by a remote server.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL connection string |
| `MRDOCUMENT_URL` | `http://mrdocument-service:8000` | Classification service URL |
| `STT_URL` | (none) | Speech-to-text service URL |
| `WATCHER_CONFIG` | `/app/watcher.yaml` | Watcher timing config |
| `MAX_CONCURRENT_PROCESSING` | `5` | Concurrent document processing limit |

## Usage

### File Flow

```
incoming/ --> processed/ --> reviewed/ --> sorted/
```

1. Drop files in `incoming/`.
2. AI-classified results appear in `processed/`.
3. Review and move to `reviewed/`.
4. MrDocument sorts into `sorted/{context}/{subfolders}/`.

Or drop files directly into `sorted/{context}/` to skip review.

### Key Folders

| Folder | Purpose |
|--------|---------|
| `incoming/` | Drop zone for new files |
| `processed/` | AI-classified files awaiting review |
| `reviewed/` | Approved files, auto-sorted to `sorted/` |
| `sorted/` | Final destination, organized by context |
| `archive/` | Original source files (permanent) |
| `reset/` | Drop processed files here to re-sort with current config |
| `trash/` | Drop files here to delete all associated copies |
| `error/` | Failed processing |

### Configuration

```
mrdocument/                          # user root
  smartfolders.yaml                  # root-level smart folders (optional)
  stt.yaml                          # speech-to-text config (optional)
  sorted/
    arbeit/                          # one directory per context
      context.yaml                   # context definition (required)
      smartfolders.yaml              # smart folder conditions (optional)
    privat/
      context.yaml
      smartfolders.yaml
  incoming/                          # drop files here
  processed/                         # AI-classified, awaiting review
  reviewed/                          # approved, auto-sorted
  archive/                           # original source files
  reset/                             # re-sort with current config
  trash/                             # delete associated files
```

See [`examples/`](examples/) for sample configs. Full documentation in [user-guide.md](user-guide.md) or [user-guide-de.md](user-guide-de.md) (German).

## Development

### Running Tests

```bash
# Unit tests
make test-unit

# Integration tests (starts Docker stack)
make test-integration
```

### Integration Test Framework

Integration tests use a declarative YAML fixture format (`tests/integration/fixture_tests/`). Each fixture defines input actions and expected filesystem tree states. Tests that require DB queries, watcher restarts, or symlink assertions remain as Python in `test_documents.py` and `test_lifecycle.py`.

See [tests-documentation.md](tests-documentation.md) for details.

## Documentation

- [user-guide.md](user-guide.md) -- User guide (English)
- [user-guide-de.md](user-guide-de.md) -- Benutzerhandbuch (Deutsch)
- [specifications.md](specifications.md) -- Watcher specifications
- [implementation.md](implementation.md) -- Implementation details
- [tests-documentation.md](tests-documentation.md) -- Test architecture and coverage
