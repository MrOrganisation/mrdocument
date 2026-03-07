# PLAN: app.py — Runnable Application + Container Integration

## Implementation Order: 9 (depends on orchestrator, migration; final step)

## Purpose

Package the v2 pipeline into a runnable application that replaces the v1 `watcher.py` + `sorter.py` pair in the container. The v1 system uses two separate processes (watcher + sorter) coordinated by watchdog filesystem events. The v2 system uses a single polling loop that handles everything.

---

## Deliverables

1. **`app.py`** — single-file entry point, replaces `watcher.py` + `sorter.py`
2. **Dockerfile.watcher update** — add new v2 files to the container
3. **`entrypoint-watcher.sh` update** — run `app.py` instead of `watcher.py` + `sorter.py`
4. **`supervisord.conf` update** — single process instead of watcher + sorter

---

## `app.py` — Application entry point

### Responsibilities

- Load configuration (env vars + YAML files)
- Connect to PostgreSQL
- Optional: run migration on first start
- Discover watch directories (reuse pattern from `watcher.py`)
- Start one `DocumentWatcherV2` per user root
- Health check server
- Graceful shutdown

### Structure

```python
import asyncio
import logging
import os
import sys
from pathlib import Path

from db_new import DocumentDBv2
from models import State
from orchestrator import DocumentWatcherV2, context_field_names_from_sorter
from sorter import SorterContextManager, UserConfig

async def main():
    # 1. Configuration
    # 2. Logging
    # 3. Database connection
    # 4. Health server
    # 5. Discover user roots
    # 6. Per-user loop
    # 7. Main polling loop
    # 8. Shutdown
```

### 1. Configuration from environment

Same env vars as v1, plus new ones:

| Variable | Default | Purpose |
|---|---|---|
| `MRDOCUMENT_URL` | `http://mrdocument-service:8000` | Service endpoint |
| `DATABASE_URL` | required | PostgreSQL connection string |
| `HEALTH_PORT` | `8080` | Health check port |
| `WATCHER_CONFIG` | `/app/watcher.yaml` | Watcher config path |
| `LOG_LEVEL` | `INFO` | Logging level |
| `LOG_DIR` | (none) | Optional log directory |
| `POLL_INTERVAL` | `5` | Seconds between polling cycles |
| `PROCESSOR_TIMEOUT` | `900` | Service call timeout |
| `AUTO_MIGRATE` | `false` | Run v1→v2 migration on startup |

### 2. Logging

Same setup as v1 `watcher.py`:
```python
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log_dir = os.environ.get("LOG_DIR")
if log_dir:
    fh = logging.FileHandler(f"{log_dir}/watcher-v2.log")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(fh)
```

### 3. Database connection

```python
db = DocumentDBv2(database_url)
await db.connect()
```

Database is required (not optional like v1). If connection fails, log error and exit.

### 4. Auto-migration (optional)

If `AUTO_MIGRATE=true`:
```python
from migrate import migrate_v1_to_v2
for user_root in discovered_roots:
    username = user_root.name
    result = await migrate_v1_to_v2(db.pool, user_root, username)
    logger.info("Migration for %s: %d/%d migrated", username, result.migrated, result.total_v1)
```

Runs once on startup, idempotent (skips already-migrated records).

### 5. Health server

Reuse the same `HealthServer` pattern from `watcher.py`:

```python
class HealthServer:
    def __init__(self, port: int = 8080):
        self._ready = False
        ...

    async def _handle_health(self, request):
        if not self._ready:
            return web.json_response({"status": "not_ready", "service": "watcher-v2"}, status=503)
        return web.json_response({"status": "healthy", "service": "watcher-v2"}, status=200)
```

Copy from `watcher.py` lines 4228-4261 (standalone, no dependencies on v1 code).

### 6. Discover user roots

Reuse `WatcherConfig` from sorter.py (already handles glob expansion):

```python
from sorter import WatcherConfig
watcher_config = WatcherConfig.load(watcher_config_path)
watch_dirs = watcher_config.get_watch_directories()
```

For each watch directory:
- Must contain `contexts.yaml`
- Load `SorterContextManager` to get context field names
- Create directory structure (archive, incoming, sorted, etc.) if missing

Wait-and-retry if no directories found (same as v1 pattern):
```python
while not watch_dirs:
    logger.info("No watch folders found, waiting...")
    await asyncio.sleep(60)
    watch_dirs = watcher_config.get_watch_directories()
```

### 7. Per-user orchestrator setup

For each user root:

```python
context_manager = SorterContextManager(user_root)
context_manager.load()
field_names = context_field_names_from_sorter(context_manager)

watcher = DocumentWatcherV2(
    root=user_root,
    db=db,
    service_url=mrdocument_url,
    context_field_names=field_names,
    poll_interval=poll_interval,
    processor_timeout=processor_timeout,
)
```

### 8. Main loop

```python
health_server.set_ready(True)

watchers = [...]  # list of (user_root, DocumentWatcherV2)

while True:
    for user_root, watcher in watchers:
        try:
            await watcher.run_cycle()
        except Exception as e:
            logger.error("Cycle error for %s: %s", user_root.name, e)

    # Check for new user directories (same as v1 pattern)
    current_dirs = watcher_config.get_watch_directories()
    for new_dir in current_dirs:
        if new_dir not in known_dirs:
            # Set up new orchestrator for this user
            ...

    await asyncio.sleep(poll_interval)
```

Note: Unlike v1 which uses watchdog filesystem events, v2 uses polling. This is simpler and avoids race conditions. The `poll_interval` (default 5s) controls latency.

### 9. Shutdown

```python
try:
    while True:
        ...
except KeyboardInterrupt:
    logger.info("Shutting down...")
finally:
    await health_server.stop()
    await db.disconnect()
```

### 10. Ensure directory structure

On startup, for each user root, create all required directories:

```python
REQUIRED_DIRS = [
    "archive", "incoming", "reviewed", "processed",
    "trash", ".output", "sorted", "error", "void", "lost",
]

for d in REQUIRED_DIRS:
    (user_root / d).mkdir(parents=True, exist_ok=True)
```

---

## Container integration

All container files live under `mrdocument/` (i.e. `~/empedokles/mrdocument`).

### Files to modify

| File | Location | Change |
|---|---|---|
| `Dockerfile.watcher` | `mrdocument/Dockerfile.watcher` | Add v2 COPY lines |
| `entrypoint-watcher.sh` | `mrdocument/watcher/entrypoint-watcher.sh` | Single process |
| `supervisord.conf` | `mrdocument/tests/integration/supervisord.conf` | Replace watcher+sorter |
| `Dockerfile` (integration) | `mrdocument/tests/integration/Dockerfile` | Add v2 COPY lines |

### mrdocument/Dockerfile.watcher changes

Add v2 files to the COPY section:

```dockerfile
# V2 pipeline files
COPY mrdocument/watcher/models.py /app/models.py
COPY mrdocument/watcher/step1.py /app/step1.py
COPY mrdocument/watcher/step2.py /app/step2.py
COPY mrdocument/watcher/step3.py /app/step3.py
COPY mrdocument/watcher/step4.py /app/step4.py
COPY mrdocument/watcher/db_new.py /app/db_new.py
COPY mrdocument/watcher/orchestrator.py /app/orchestrator.py
COPY mrdocument/watcher/app.py /app/app.py
```

Keep v1 files for now (migration reads from v1 DB, needs sorter.py for config parsing).

Note: The production Dockerfile.watcher is at the repo root (`~/empedokles/Dockerfile.watcher`) and uses context `.` — its COPY paths reference `mrdocument/watcher/...`. The updated version should be placed at `mrdocument/Dockerfile.watcher` with the build context set to `mrdocument/` (or keep at repo root if that's the convention).

### mrdocument/watcher/entrypoint-watcher.sh changes

Replace two processes with one:

```sh
# OLD:
# su-exec "$PUID:$PGID" python3 /app/watcher.py &
# su-exec "$PUID:$PGID" python3 /app/sorter.py &

# NEW:
su-exec "$PUID:$PGID" python3 /app/app.py &
APP_PID=$!
```

Remove the second process monitoring. Single process = simpler lifecycle.

### mrdocument/tests/integration/supervisord.conf changes

Replace watcher + sorter programs with single v2 program:

```ini
# OLD:
# [program:watcher]
# command=python3 watcher.py
# [program:sorter]
# command=python3 sorter.py

# NEW:
[program:watcher-v2]
command=python3 app.py
directory=/app/watcher
priority=40
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/%(program_name)s.log
```

### mrdocument/tests/integration/Dockerfile changes

Add v2 files alongside existing watcher copies:

```dockerfile
# V2 pipeline files
COPY mrdocument/watcher/models.py /app/watcher/models.py
COPY mrdocument/watcher/step1.py /app/watcher/step1.py
COPY mrdocument/watcher/step2.py /app/watcher/step2.py
COPY mrdocument/watcher/step3.py /app/watcher/step3.py
COPY mrdocument/watcher/step4.py /app/watcher/step4.py
COPY mrdocument/watcher/db_new.py /app/watcher/db_new.py
COPY mrdocument/watcher/orchestrator.py /app/watcher/orchestrator.py
COPY mrdocument/watcher/app.py /app/watcher/app.py
```

### docker-compose.yaml

No changes needed — the environment variables are the same, the health check endpoint is the same, the port is the same. The entrypoint script handles the process switch.

---

## Tests: `test_app.py`

Integration tests that verify the full application lifecycle. Uses real PostgreSQL + mock mrdocument + real filesystem.

### Fixtures

```python
@pytest_asyncio.fixture
async def db():
    db = DocumentDBv2(DATABASE_URL)
    await db.connect()
    yield db
    await db.pool.execute("DELETE FROM mrdocument.documents_v2")
    await db.disconnect()

@pytest.fixture
def user_root(tmp_path):
    """Full user root with config files and directories."""
    for d in REQUIRED_DIRS:
        (tmp_path / d).mkdir()
    # Write minimal contexts.yaml and context file
    (tmp_path / "contexts.yaml").write_text(yaml.dump(["work.yaml"]))
    (tmp_path / "work.yaml").write_text(yaml.dump({
        "name": "work",
        "filename": "{context}-{type}-{date}-{sender}-{topic}",
        "folders": ["context", "sender", "topic"],
        "fields": {
            "type": {"instructions": "Document type"},
            "sender": {"instructions": "Sender"},
            "topic": {"instructions": "Topic"},
        },
    }))
    return tmp_path
```

### Test scenarios

#### Application startup and health check

1. Create app, start health server
2. Assert `/health` returns 503 before ready
3. Set ready
4. Assert `/health` returns 200

#### Single-cycle end-to-end

1. Set up user_root with context config + mock service
2. Place file in `incoming/`
3. Call `run_cycle()` once (or twice — first detects + processes, second may reconcile)
4. Assert file lifecycle: incoming → archive (source), .output (processed) → sorted (final)
5. Assert DB record in IS_COMPLETE state

#### Multi-user directory discovery

1. Create two user roots under a watch pattern
2. Verify both discovered and both get orchestrators

#### New user directory added dynamically

1. Start with one user root
2. Add second user root during runtime
3. Next polling iteration discovers and starts processing

#### Context config loading

1. Write context YAML files
2. Verify `context_field_names_from_sorter()` extracts field names
3. Verify reconcile uses them for metadata completeness

#### Graceful shutdown

1. Start app loop in background task
2. Cancel the task
3. Assert DB disconnected, health server stopped

#### Error resilience

1. Start cycle with unreachable mrdocument service
2. Assert records get HAS_ERROR, no crash
3. Fix service
4. Next cycle can recover (error records have source_reference to error/)
