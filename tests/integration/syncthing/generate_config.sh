#!/usr/bin/env bash
# Generate Syncthing TLS certificates and config.xml for integration test
# server and client instances.
#
# Usage: bash syncthing/generate_config.sh
#   (run from mrdocument/tests/integration/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_DIR="$SCRIPT_DIR/server"
CLIENT_DIR="$SCRIPT_DIR/client"

IMAGE="syncthing/syncthing:latest"
CURRENT_UID="$(id -u)"
CURRENT_GID="$(id -g)"

echo "=== Generating Syncthing integration-test configs ==="

# Pull the image once
docker pull "$IMAGE" >/dev/null 2>&1 || true

# --- Generate certs for server ---
echo "Generating server certs..."
mkdir -p "$SERVER_DIR"
docker run --rm -u "$CURRENT_UID:$CURRENT_GID" \
    -v "$SERVER_DIR:/config" --entrypoint /bin/syncthing "$IMAGE" \
    generate --home=/config >/dev/null 2>&1
SERVER_ID=$(docker run --rm \
    -v "$SERVER_DIR:/config" --entrypoint /bin/syncthing "$IMAGE" \
    device-id --home=/config 2>/dev/null)
echo "  Server device ID: $SERVER_ID"

# --- Generate certs for client ---
echo "Generating client certs..."
mkdir -p "$CLIENT_DIR"
docker run --rm -u "$CURRENT_UID:$CURRENT_GID" \
    -v "$CLIENT_DIR:/config" --entrypoint /bin/syncthing "$IMAGE" \
    generate --home=/config >/dev/null 2>&1
CLIENT_ID=$(docker run --rm \
    -v "$CLIENT_DIR:/config" --entrypoint /bin/syncthing "$IMAGE" \
    device-id --home=/config 2>/dev/null)
echo "  Client device ID: $CLIENT_ID"

# --- Write server config.xml (overwrites the auto-generated one) ---
echo "Writing server config.xml..."
cat > "$SERVER_DIR/config.xml" <<XMLEOF
<configuration version="37">
    <folder id="mrdocument-testuser" label="MrDocument Test User"
            path="/var/syncthing/testuser" type="sendreceive"
            rescanIntervalS="3600" fsWatcherEnabled="true" fsWatcherDelayS="1"
            ignorePerms="true" autoNormalize="true">
        <device id="$SERVER_ID" introducedBy=""></device>
        <device id="$CLIENT_ID" introducedBy=""></device>
        <minDiskFree unit="%">1</minDiskFree>
    </folder>

    <device id="$SERVER_ID" name="syncthing-server" compression="metadata">
        <address>dynamic</address>
    </device>
    <device id="$CLIENT_ID" name="syncthing-client" compression="metadata">
        <address>tcp://syncthing-client:22000</address>
    </device>

    <gui enabled="true" tls="false" debugging="false">
        <address>0.0.0.0:8384</address>
        <apikey>test-api-key-syncthing</apikey>
    </gui>

    <options>
        <listenAddress>tcp://0.0.0.0:22000</listenAddress>
        <globalAnnounceEnabled>false</globalAnnounceEnabled>
        <localAnnounceEnabled>false</localAnnounceEnabled>
        <relaysEnabled>false</relaysEnabled>
        <natEnabled>false</natEnabled>
        <urAccepted>-1</urAccepted>
        <crashReportingEnabled>false</crashReportingEnabled>
        <startBrowser>false</startBrowser>
    </options>
</configuration>
XMLEOF

# --- Write client config.xml (overwrites the auto-generated one) ---
echo "Writing client config.xml..."
cat > "$CLIENT_DIR/config.xml" <<XMLEOF
<configuration version="37">
    <folder id="mrdocument-testuser" label="MrDocument Test User"
            path="/var/syncthing/testuser" type="sendreceive"
            rescanIntervalS="3600" fsWatcherEnabled="true" fsWatcherDelayS="1"
            ignorePerms="true" autoNormalize="true">
        <device id="$SERVER_ID" introducedBy=""></device>
        <device id="$CLIENT_ID" introducedBy=""></device>
        <minDiskFree unit="%">1</minDiskFree>
    </folder>

    <device id="$SERVER_ID" name="syncthing-server" compression="metadata">
        <address>tcp://syncthing-server:22000</address>
    </device>
    <device id="$CLIENT_ID" name="syncthing-client" compression="metadata">
        <address>dynamic</address>
    </device>

    <gui enabled="true" tls="false" debugging="false">
        <address>0.0.0.0:8384</address>
        <apikey>test-api-key-syncthing</apikey>
    </gui>

    <options>
        <listenAddress>tcp://0.0.0.0:22000</listenAddress>
        <globalAnnounceEnabled>false</globalAnnounceEnabled>
        <localAnnounceEnabled>false</localAnnounceEnabled>
        <relaysEnabled>false</relaysEnabled>
        <natEnabled>false</natEnabled>
        <urAccepted>-1</urAccepted>
        <crashReportingEnabled>false</crashReportingEnabled>
        <startBrowser>false</startBrowser>
    </options>
</configuration>
XMLEOF

# Clean up auto-generated extras we don't need
rm -f "$SERVER_DIR/https-cert.pem" "$SERVER_DIR/https-key.pem"
rm -f "$CLIENT_DIR/https-cert.pem" "$CLIENT_DIR/https-key.pem"

echo "=== Done ==="
echo "  Server: $SERVER_DIR/{cert.pem,key.pem,config.xml}"
echo "  Client: $CLIENT_DIR/{cert.pem,key.pem,config.xml}"
