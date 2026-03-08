#!/bin/sh
set -e

PUID=${PUID:-1000}
PGID=${PGID:-1000}

echo "Running as UID=$PUID, GID=$PGID"

if ! getent group "$PGID" > /dev/null 2>&1; then
    addgroup -g "$PGID" watcher
fi
if ! getent passwd "$PUID" > /dev/null 2>&1; then
    adduser -D -u "$PUID" -G "$(getent group "$PGID" | cut -d: -f1)" watcher
fi

if [ "$(stat -c '%u:%g' /app 2>/dev/null)" != "$PUID:$PGID" ]; then
    echo "Fixing ownership of /app..."
    chown -R "$PUID:$PGID" /app 2>/dev/null || true
fi

if [ -d /costs ] && [ -w /costs ]; then
    if [ "$(stat -c '%u:%g' /costs 2>/dev/null)" != "$PUID:$PGID" ]; then
        echo "Fixing ownership of /costs..."
        chown -R "$PUID:$PGID" /costs 2>/dev/null || true
    fi
fi

echo "Starting watcher (Rust)..."
umask 0000
exec su-exec "$PUID:$PGID" /app/watcher
