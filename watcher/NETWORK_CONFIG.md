# Network Configuration

## Architecture

```
Internet
    ↓
VPN (IPSec)
    ↓
nginx (443/tcp) ← SSL/TLS termination
    ↓
    ├─→ /syncthing/ → syncthing:8384 (Web UI)
    ├─→ /nextcloud/ → nextcloud:80
    ├─→ /n8n/ → n8n:5678
    └─→ ... other services

Direct connection (bypasses nginx):
    ↓
syncthing:22000 (TCP/UDP) ← Sync protocol
```

## Exposed Ports

### Public (Firewall Rules Required)

**Port 22000/tcp** - Syncthing sync protocol
- Direct client-to-server communication
- Cannot be proxied through nginx
- Required for file synchronization

**Port 22000/udp** - Syncthing discovery
- NAT traversal
- Local network discovery
- Required for direct connections

**Port 443/tcp** - HTTPS (nginx)
- All web UIs go through this
- SSL/TLS termination
- VPN protected

### Internal (Docker Network Only)

**Port 8384** - Syncthing web UI
- Not exposed to host
- Accessed via nginx at `https://parmenides.net/syncthing/`
- WebSocket support for real-time updates

**Port 8000** - MrDocument API
- Internal network only
- Called by Syncthing watcher

**Port 5000** - OCRmyPDF API
- Internal network only
- Called by MrDocument

## Firewall Configuration

### iptables

```bash
# Allow Syncthing sync (required)
iptables -A INPUT -p tcp --dport 22000 -j ACCEPT
iptables -A INPUT -p udp --dport 22000 -j ACCEPT

# Allow HTTPS (already configured for other services)
iptables -A INPUT -p tcp --dport 443 -j ACCEPT

# Save rules
iptables-save > /etc/iptables/rules.v4
```

### ufw

```bash
ufw allow 22000/tcp comment "Syncthing sync"
ufw allow 22000/udp comment "Syncthing discovery"
ufw allow 443/tcp comment "HTTPS"
```

### firewalld

```bash
firewall-cmd --add-port=22000/tcp --permanent
firewall-cmd --add-port=22000/udp --permanent
firewall-cmd --add-port=443/tcp --permanent
firewall-cmd --reload
```

## Access URLs

### Syncthing Web UI

**Internal (server):**
```bash
curl http://syncthing:8384/rest/system/status
```

**External (via nginx + VPN):**
```
https://parmenides.net/syncthing/
```

**Features:**
- Device management
- Folder configuration
- Sync status monitoring
- Settings and authentication

### Other Services

```
https://parmenides.net/nextcloud/  - NextCloud
https://parmenides.net/n8n/        - n8n automation
https://parmenides.net/git/        - Gitea
https://parmenides.net/ocr/        - OCRmyPDF API
https://parmenides.net/mcp/ole/    - NextCloud MCP
```

## Client Configuration

### Mac Syncthing Setup

**1. Add server device:**
- Server address: `tcp://parmenides.net:22000`
- Device ID: (from server web UI)

**2. Access web UI:**
- Via VPN: `https://parmenides.net/syncthing/`
- Local client: `http://localhost:8384`

### Direct Connection Verification

From Mac terminal:

```bash
# Test sync port
nc -zv parmenides.net 22000
# Connection to parmenides.net port 22000 [tcp] succeeded!

# Test web UI (via VPN)
curl -k https://parmenides.net/syncthing/
# Should return HTML
```

## Security

### VPN Protection

All web interfaces (port 443) protected by VPN:
- Only VPN clients can access
- IPSec tunnel required
- See `host-fw/` for firewall rules

### Syncthing Sync Port

**Port 22000 NOT VPN-protected:**
- Must be accessible for Syncthing protocol
- Uses TLS encryption
- Device authorization required
- Can't route through nginx (protocol requirement)

**Security measures:**
1. Device pairing required (mutual authentication)
2. TLS 1.3 encryption
3. Device certificates
4. No open relay

### Web UI Security

**Authentication:**
- Set password in Syncthing settings
- HTTPS via nginx (SSL certificate)
- VPN access only

**To set password:**
1. Access `https://parmenides.net/syncthing/`
2. Settings → GUI → Username & Password
3. Save

## Nginx Configuration

### Location Block

```nginx
location /syncthing/ {
    proxy_pass http://syncthing:8384/;
    
    # Standard proxy headers
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    
    # WebSocket support (required for Syncthing UI)
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    
    proxy_redirect off;
    proxy_buffering off;
}
```

### Upstream

```nginx
upstream syncthing {
    server syncthing:8384;
}
```

## Troubleshooting

### Web UI Not Accessible

**Via nginx (`https://parmenides.net/syncthing/`):**

1. **Check nginx is running:**
   ```bash
   docker compose ps nginx
   ```

2. **Check Syncthing container:**
   ```bash
   docker compose ps syncthing
   docker compose logs syncthing | grep "GUI"
   ```

3. **Check nginx configuration:**
   ```bash
   docker compose exec nginx nginx -t
   ```

4. **Check nginx logs:**
   ```bash
   docker compose logs nginx | grep syncthing
   ```

5. **Restart nginx:**
   ```bash
   docker compose restart nginx
   ```

### Sync Not Working

**Port 22000 issues:**

1. **Check firewall allows port:**
   ```bash
   # From Mac
   nc -zv parmenides.net 22000
   ```

2. **Check Syncthing listening:**
   ```bash
   docker compose exec syncthing netstat -tlnp | grep 22000
   ```

3. **Check iptables:**
   ```bash
   iptables -L -n | grep 22000
   ```

4. **View Syncthing logs:**
   ```bash
   docker compose logs syncthing | grep -E "(listen|connection|device)"
   ```

### WebSocket Issues

If web UI loads but doesn't update in real-time:

1. **Check nginx WebSocket config:**
   ```bash
   docker compose exec nginx cat /etc/nginx/conf.d/default.conf | grep -A 5 syncthing
   ```

2. **Browser console:** Check for WebSocket connection errors

3. **Reload nginx config:**
   ```bash
   docker compose exec nginx nginx -s reload
   ```

## Network Flow Examples

### User Uploads PDF

```
1. Mac (Finder): Save to ~/Synced/incoming/invoice.pdf
2. Mac Syncthing: Detects file change
3. Connection: Mac → parmenides.net:22000 (direct)
4. Server Syncthing: Receives file
5. Server Filesystem: /sync/alice/incoming/invoice.pdf
6. Python Watcher: Detects file (inotify)
7. Watcher → MrDocument:8000 (internal)
8. MrDocument → OCRmyPDF:5000 (internal)
9. OCRmyPDF → MrDocument (processed PDF)
10. Watcher: Saves to /sync/alice/processed/result.pdf
11. Server Syncthing: Detects new file
12. Connection: parmenides.net:22000 → Mac (direct)
13. Mac Filesystem: ~/Synced/processed/result.pdf
```

### Admin Checks Status

```
1. Mac Browser: https://parmenides.net/syncthing/
2. VPN: Allows connection to port 443
3. nginx:443 → syncthing:8384 (proxy)
4. Browser: Displays Syncthing web UI
5. WebSocket: Real-time updates via nginx proxy
```

## Port Summary Table

| Port  | Protocol | Access    | Purpose                | Proxy  |
|-------|----------|-----------|------------------------|--------|
| 443   | TCP      | Public    | HTTPS (all web UIs)    | nginx  |
| 22000 | TCP      | Public    | Syncthing sync         | Direct |
| 22000 | UDP      | Public    | Syncthing discovery    | Direct |
| 8384  | TCP      | Internal  | Syncthing web UI       | nginx  |
| 8001  | TCP      | Localhost | MrDocument API         | None   |
| 5000  | TCP      | Internal  | OCRmyPDF API           | None   |

## References

- Main setup: `README.md`
- Multi-user: `MULTI_USER_SETUP.md`
- Quick start: `../QUICKSTART_SYNCTHING.md`
- Firewall rules: `../host-fw/`
