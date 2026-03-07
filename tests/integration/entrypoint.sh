#!/bin/bash
set -e

PG_DATA=/var/lib/postgresql/data
PG_BIN=/usr/lib/postgresql/17/bin

# ---------------------------------------------------------------------------
# 0. Ensure log directories exist (/var/log may be a bind mount)
# ---------------------------------------------------------------------------
mkdir -p /var/log/supervisor /var/log/postgresql
chown postgres:postgres /var/log/postgresql

# ---------------------------------------------------------------------------
# 1. Init PostgreSQL data dir if empty
# ---------------------------------------------------------------------------
if [ ! -f "$PG_DATA/PG_VERSION" ]; then
    echo "Initializing PostgreSQL data directory..."
    mkdir -p "$PG_DATA"
    chown postgres:postgres "$PG_DATA"
    su postgres -c "$PG_BIN/initdb -D $PG_DATA"
fi

# ---------------------------------------------------------------------------
# 2. Start PostgreSQL, create DB + user, stop it
# ---------------------------------------------------------------------------
echo "Starting PostgreSQL for initial setup..."
su postgres -c "$PG_BIN/pg_ctl -D $PG_DATA -l /var/log/postgresql/init.log start -w"

# Create user and database if they don't exist
su postgres -c "psql -tc \"SELECT 1 FROM pg_roles WHERE rolname='mrdocument'\" | grep -q 1 || psql -c \"CREATE USER mrdocument WITH PASSWORD 'mrdocument';\""
su postgres -c "psql -tc \"SELECT 1 FROM pg_database WHERE datname='mrdocument'\" | grep -q 1 || psql -c \"CREATE DATABASE mrdocument OWNER mrdocument;\""

echo "Stopping PostgreSQL (supervisord will manage it)..."
su postgres -c "$PG_BIN/pg_ctl -D $PG_DATA stop -w"

# ---------------------------------------------------------------------------
# 3. Write watcher.yaml
# ---------------------------------------------------------------------------
cat > /app/watcher.yaml <<'YAML'
watch_patterns:
  - "/data/*"

timeout: 900
max_concurrency: 4
YAML

# ---------------------------------------------------------------------------
# 4. Ensure all user data dirs exist
# ---------------------------------------------------------------------------
for dir in incoming processed archive error transit reviewed sorted duplicates trash .output void lost; do
    mkdir -p /data/testuser/$dir
done

# ---------------------------------------------------------------------------
# 5. Copy config files to data dir if not already present
# ---------------------------------------------------------------------------
for f in /app/config/*; do
    fname=$(basename "$f")
    if [ ! -f "/data/testuser/$fname" ]; then
        cp "$f" "/data/testuser/$fname"
    fi
done

# ---------------------------------------------------------------------------
# 6. Export env vars for internal service URLs
# ---------------------------------------------------------------------------
export MRDOCUMENT_URL=${MRDOCUMENT_URL:-http://localhost:8000}
export STT_URL=${STT_URL:-http://localhost:8001}
export OCR_URL=${OCR_URL:-http://localhost:5000}
export DATABASE_URL=${DATABASE_URL:-postgresql://mrdocument:mrdocument@localhost:5432/mrdocument}
export WATCHER_CONFIG=${WATCHER_CONFIG:-/app/watcher.yaml}
export HEALTH_PORT=${HEALTH_PORT:-8080}

# ---------------------------------------------------------------------------
# 7. Launch supervisord
# ---------------------------------------------------------------------------
echo "Starting all services via supervisord..."
if [ "${SERVICE_MOCK_MODE:-}" = "1" ]; then
    echo "SERVICE_MOCK_MODE enabled — real service with mock backends"
    exec supervisord -c /etc/supervisord.service-mock.conf
else
    exec supervisord -c /etc/supervisord.conf
fi
