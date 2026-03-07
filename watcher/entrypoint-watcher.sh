#!/bin/sh
set -e

# Use PUID/PGID from environment, default to 1000
PUID=${PUID:-1000}
PGID=${PGID:-1000}

echo "Running as UID=$PUID, GID=$PGID"

# Create user/group if they don't exist
if ! getent group "$PGID" > /dev/null 2>&1; then
    addgroup -g "$PGID" watcher
fi
if ! getent passwd "$PUID" > /dev/null 2>&1; then
    adduser -D -u "$PUID" -G "$(getent group "$PGID" | cut -d: -f1)" watcher
fi

# Fix ownership of app directory (ignore errors from read-only bind mounts)
if [ "$(stat -c '%u:%g' /app 2>/dev/null)" != "$PUID:$PGID" ]; then
    echo "Fixing ownership of /app..."
    chown -R "$PUID:$PGID" /app 2>/dev/null || true
fi

# Fix ownership of costs directory if it exists and is writable
if [ -d /costs ] && [ -w /costs ]; then
    if [ "$(stat -c '%u:%g' /costs 2>/dev/null)" != "$PUID:$PGID" ]; then
        echo "Fixing ownership of /costs..."
        chown -R "$PUID:$PGID" /costs 2>/dev/null || true
    fi
fi

echo "Starting watcher v2 service..."
# Start v2 app in background, running as specified user
su-exec "$PUID:$PGID" python3 /app/app.py &
APP_PID=$!

# Handle shutdown gracefully
cleanup() {
    echo "Shutting down..."
    kill $APP_PID 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

# Monitor process - if it exits, shut down
while true; do
    if ! kill -0 $APP_PID 2>/dev/null; then
        echo "Watcher v2 exited unexpectedly"
        exit 1
    fi
    sleep 5
done
