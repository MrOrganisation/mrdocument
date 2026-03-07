# Multi-User Setup Guide

Single Syncthing server supporting multiple users.

## Architecture

```
Mac (Alice) ────┐
                │
Mac (Bob) ──────┼──→ Single Syncthing Server
                │    /sync/alice/incoming/
Mac (Carol) ────┘    /sync/alice/processed/
                     /sync/bob/incoming/
                     /sync/bob/processed/
                     /sync/carol/incoming/
                     /sync/carol/processed/
```

## Key Features

✅ **Single server** - One container for all users
✅ **Auto-discovery** - New users detected automatically
✅ **Folder isolation** - Users only see their own folders
✅ **One device ID** - Simplifies management
✅ **One port** - 22000 for all users

## Setup

### 1. Server Setup (One Time)

```bash
# Set API key
cp .env.sample .env
nano .env  # Add ANTHROPIC_API_KEY

# Start services
docker compose --profile default up -d ocrmypdf mrdocument syncthing

# Get Syncthing device ID (share with all users)
docker compose logs syncthing | grep "Device ID"
# Or visit: https://parmenides.net/syncthing/
```

### 2. Add New User (Per User)

#### On Mac (User: Alice)

```bash
# Install Syncthing
brew install syncthing
brew services start syncthing

# Open web UI
open http://localhost:8384

# Add server as remote device
# - Click "Add Remote Device"
# - Paste server device ID
# - Give it a name like "Processing Server"
# - Save

# Create local folders
mkdir -p ~/Synced/incoming ~/Synced/processed

# Share incoming folder with server
# - Click "Add Folder"
# - Folder Path: ~/Synced/incoming
# - Folder ID: alice-incoming
# - Click "Sharing" tab
# - Select "Processing Server"
# - Save

# Share processed folder with server
# - Click "Add Folder"
# - Folder Path: ~/Synced/processed
# - Folder ID: alice-processed
# - Click "Sharing" tab
# - Select "Processing Server"
# - Save
```

#### On Server (Accept Alice's Folders)

```bash
# Open server web UI (via VPN)
open https://parmenides.net/syncthing/

# Accept device (if prompted)
# - Accept Alice's device connection

# Accept folder shares (when prompted)
# For "alice-incoming":
#   - Set path: /sync/alice/incoming
#   - Save
#
# For "alice-processed":
#   - Set path: /sync/alice/processed
#   - Save
```

The watcher will automatically detect the new user and start monitoring!

### 3. Add More Users

Repeat step 2 for Bob, Carol, etc. Each user:
- Shares their `{username}-incoming` folder → `/sync/{username}/incoming`
- Shares their `{username}-processed` folder → `/sync/{username}/processed`

## Folder Structure

### On Server

```
/sync/
├── alice/
│   ├── incoming/      ← Alice drops PDFs here (via Syncthing)
│   └── processed/     ← Results sync back to Alice
├── bob/
│   ├── incoming/
│   └── processed/
└── carol/
    ├── incoming/
    └── processed/
```

### On Mac (Alice)

```
~/Synced/
├── incoming/      ← Drop PDFs here
└── processed/     ← Results appear here
```

## Usage

Each user uses it independently:

**Alice:**
```bash
cp invoice.pdf ~/Synced/incoming/
# Wait 5-10 seconds
ls ~/Synced/processed/
# invoice-2024-01-15-acme_corp.pdf
```

**Bob:**
```bash
cp receipt.pdf ~/Synced/incoming/
# Wait 5-10 seconds
ls ~/Synced/processed/
# receipt-2024-01-15-store_name.pdf
```

Users never see each other's files.

## Monitoring

### View All Users

```bash
docker compose logs -f syncthing
```

Output shows username in logs:
```
[alice] New file detected: invoice.pdf
[alice] Processing: invoice.pdf
[alice] Saved: invoice-2024-01-15-acme_corp.pdf
[bob] New file detected: receipt.pdf
[bob] Processing: receipt.pdf
[bob] Saved: receipt-2024-01-15-store_name.pdf
```

### Server Web UI

Visit `https://parmenides.net/syncthing/` (via VPN):
- See all connected devices (Alice, Bob, Carol)
- View sync status per folder
- Monitor bandwidth usage

## Security & Isolation

### Folder-Level Isolation

✅ **Syncthing protocol:** Alice's Mac only syncs folders shared with it
✅ **Server storage:** Files physically separated (`/sync/alice/` vs `/sync/bob/`)
✅ **Cannot browse:** Users can't navigate to other users' folders via Syncthing

### What Users See

**Alice's Syncthing UI:**
- Her device
- Processing Server device
- Her folders: `alice-incoming`, `alice-processed`
- ❌ Does NOT see Bob or Carol

**Server Syncthing UI:**
- All user devices (Alice, Bob, Carol)
- All folders from all users
- Can manage everything

### Filesystem-Level

On server, root can see all files:
```bash
ls /sync/
# alice/ bob/ carol/
```

If needed, add OS-level permissions:
```bash
# Restrict alice folder to alice only
chown alice:alice /sync/alice/
chmod 700 /sync/alice/
```

## Auto-Discovery

The watcher automatically:
1. Scans `/sync/` on startup
2. Finds all `{username}/incoming/` folders
3. Starts monitoring each one
4. Checks every minute for new users
5. Adds new users without restart

When new user added:
```
Detected 1 new user folder(s)
  Adding: carol
[carol] Scanning for existing PDF files...
  Watching: /sync/carol/incoming/
```

## Troubleshooting

### User's Files Not Processing

1. **Check folder structure on server:**
   ```bash
   docker compose exec syncthing ls -la /sync/
   # Should see: alice/ bob/ carol/
   
   docker compose exec syncthing ls -la /sync/alice/
   # Should see: incoming/ processed/
   ```

2. **Check watcher detected user:**
   ```bash
   docker compose logs syncthing | grep "Discovered user"
   # Discovered user: alice
   # Discovered user: bob
   ```

3. **Verify Syncthing sync:**
   - Open server web UI: https://parmenides.net/syncthing/
   - Check folder status (should be "Up to Date")
   - Verify files are syncing

### User Can't Connect to Server

1. **Check device ID matches:**
   - On Mac: Syncthing UI → Actions → Show ID
   - On server: Settings → Remote Devices
   - Ensure IDs match

2. **Check network:**
   - Verify port 22000 is open on server
   - Test: `telnet your-server 22000`

3. **Check firewall:**
   - Allow TCP/UDP 22000
   - See main README for firewall rules

### Watcher Not Detecting New User

1. **Check folder structure:**
   ```bash
   # Must be exactly: /sync/{username}/incoming/
   docker compose exec syncthing ls -la /sync/bob/
   # Should show: incoming/ processed/
   ```

2. **Restart watcher:**
   ```bash
   docker compose restart syncthing
   ```

3. **Manual trigger:**
   - Drop a PDF in the incoming folder
   - Check logs

## Adding Users: Quick Reference

### Client (Mac)

1. Add server as remote device (device ID)
2. Create `~/Synced/incoming/` and `~/Synced/processed/`
3. Share both folders with server
4. Use Folder IDs: `{username}-incoming`, `{username}-processed`

### Server

1. Accept device connection
2. Accept folder shares
3. Set paths:
   - `{username}-incoming` → `/sync/{username}/incoming`
   - `{username}-processed` → `/sync/{username}/processed`
4. Wait 1 minute (auto-detection)

Done!

## Removing Users

### Remove User from Server

1. **Stop syncing:**
   - Server web UI → Remove user's folders
   - Server web UI → Remove user's device

2. **Remove data:**
   ```bash
   # Backup first (optional)
   tar czf alice-backup.tar.gz ~/data/syncthing/sync/alice/
   
   # Delete user folder
   rm -rf ~/data/syncthing/sync/alice/
   ```

3. **Watcher will automatically stop monitoring** (folder gone)

### User Leaves

User just:
- Stops Syncthing on their Mac
- Or removes the server device from their Syncthing

Files remain on server until manually deleted.

## Cost Sharing

Since all users share one MrDocument service:

**API costs:**
- ~$0.015 per document
- Track by user via logs: `grep "\[alice\]" logs.txt | grep "Processing" | wc -l`

**Infrastructure:**
- One server for all users
- Marginal cost per additional user: ~$0

## Scaling

**Current setup handles:**
- Dozens of users easily
- Hundreds of documents per day
- Limited by: OCR CPU, API rate limits

**Bottlenecks:**
- OCR is CPU-bound (one at a time currently)
- Anthropic API rate limits (check your tier)

**If needed:**
- Async processing already implemented (can process multiple users in parallel)
- Add more OCR workers
- Increase server resources

## Best Practices

1. **User naming:** Use actual usernames or email prefixes (alice, bob, not user1, user2)
2. **Folder IDs:** Keep consistent: `{username}-incoming`, `{username}-processed`
3. **Test first:** Add yourself as test user before adding others
4. **Monitor logs:** Watch for errors in first few days
5. **Set password:** Secure Syncthing web UI with password
6. **Backup config:** `tar czf syncthing-config.tar.gz ~/data/syncthing/config/`

## Summary

- ✅ **One server** for all users
- ✅ **Auto-discovery** of new users
- ✅ **Isolated** per-user folders
- ✅ **Simple** to add new users
- ✅ **Cost-effective** shared infrastructure
- ✅ **Scalable** to dozens of users

Perfect for small teams, families, or departments.
