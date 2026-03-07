# Troubleshooting Guide

## Watcher Not Detecting New Files

### Step 1: Check Container is Running

```bash
docker compose ps syncthing
# Should show: Up (healthy)
```

### Step 2: Check Logs with Debug Level

```bash
docker compose logs -f syncthing
```

Look for:
- `Discovered user: {username}` - Shows user was detected
- `Watching: /sync/{username}/incoming/` - Shows folder is monitored
- `Watchdog event: created` - Shows watchdog detected file
- `New file detected: {filename}` - Shows file passed filters

### Step 3: Verify Folder Structure

```bash
# Check folders exist
docker compose exec syncthing ls -la /sync/

# Check user folders
docker compose exec syncthing ls -la /sync/alice/

# Should show: incoming/ and processed/
```

### Step 4: Test File Detection Manually

```bash
# Create a test file directly in container
docker compose exec syncthing sh -c 'echo "test" > /sync/alice/incoming/test.pdf'

# Check logs immediately
docker compose logs syncthing | tail -20
```

You should see:
```
Watchdog event: created - /sync/alice/incoming/test.pdf
[alice] New file detected: test.pdf
Scheduled processing for: test.pdf
```

### Step 5: Check Syncthing is Syncing

```bash
# Check Syncthing status
curl http://localhost:8384/rest/system/status

# Or via nginx (with VPN)
curl https://parmenides.net/syncthing/rest/system/status
```

### Step 6: Verify File Permissions

```bash
# Check permissions on sync folder
docker compose exec syncthing ls -lan /sync/alice/incoming/

# Should be writable (755 or 777)
```

### Step 7: Manual Test from Mac

```bash
# On Mac, copy a file
cp ~/test.pdf ~/Synced/incoming/

# Watch Syncthing logs
docker compose logs -f syncthing | grep -E "(Syncthing|Watchdog|detected)"
```

## Common Issues

### Issue: "No user folders found"

**Symptoms:**
```
No user folders found!
Expected folder structure:
  /sync/{username}/incoming/
```

**Cause:** Syncthing hasn't created folders yet or folders not shared

**Solution:**
```bash
# Option 1: Create folders manually
docker compose exec syncthing mkdir -p /sync/alice/incoming /sync/alice/processed

# Option 2: Share folders via Syncthing web UI
# 1. Go to https://parmenides.net/syncthing/
# 2. Accept shared folders from clients
# 3. Set paths to /sync/{username}/incoming and /sync/{username}/processed
```

### Issue: Files detected but not processing

**Symptoms:**
```
[alice] New file detected: invoice.pdf
Scheduled processing for: invoice.pdf
# Then nothing...
```

**Cause:** Event loop issue or MrDocument not accessible

**Solution:**
```bash
# Check MrDocument is running
docker compose ps mrdocument
# Should show: Up (healthy)

# Test MrDocument directly
curl http://localhost:8001/health

# Check watcher can reach MrDocument
docker compose exec syncthing curl http://mrdocument-service:8000/health
```

### Issue: Watchdog not detecting files

**Symptoms:**
- No "Watchdog event" messages in logs
- Files appear but no detection

**Cause:** Watchdog not monitoring correctly or inotify issues

**Solution:**
```bash
# Restart container
docker compose restart syncthing

# Check inotify limits (on host)
cat /proc/sys/fs/inotify/max_user_watches
# Should be > 8192

# Increase if needed (on host)
echo 524288 | sudo tee /proc/sys/fs/inotify/max_user_watches

# Make permanent
echo "fs.inotify.max_user_watches=524288" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

### Issue: Syncthing temporary files being processed

**Symptoms:**
```
[alice] New file detected: .syncthing.invoice.pdf.tmp
```

**Cause:** Watcher detecting Syncthing's temporary files

**Note:** These are filtered out automatically, but if you see them being processed:

**Solution:** Already handled in code, but ensure Syncthing finishes writing:
- Syncthing creates `.syncthing.{file}.tmp` files
- Renames to final name when complete
- Watcher ignores `.syncthing.` and `.tmp` files

### Issue: Files processed but disappear

**Symptoms:**
- File detected
- Processing starts
- No result in processed folder

**Cause:** Processing error or output path issue

**Solution:**
```bash
# Check MrDocument logs
docker compose logs mrdocument

# Check OCRmyPDF logs
docker compose logs ocrmypdf

# Verify processed folder exists and is writable
docker compose exec syncthing ls -la /sync/alice/processed/
```

### Issue: "Syncthing exited unexpectedly"

**Symptoms:**
```
Syncthing exited unexpectedly
```

**Cause:** Syncthing crashed or config issue

**Solution:**
```bash
# Check Syncthing logs
docker compose logs syncthing | grep -i error

# Check config
docker compose exec syncthing /bin/syncthing serve --help

# Restart
docker compose restart syncthing
```

## Debug Mode

Enable debug logging:

```bash
# Edit docker-compose.yaml
environment:
  LOG_LEVEL: DEBUG  # Already set

# Restart
docker compose restart syncthing
```

Debug logs show:
- `Watchdog event: created` - Every file system event
- `Ignoring non-PDF file` - Filtered files
- `Ignoring hidden/temp file` - Filtered temps
- `Scheduled processing for` - Queued for processing

## Test Script

Create a test script on the server:

```bash
#!/bin/bash
# test-watcher.sh

USERNAME=${1:-alice}
TESTFILE="test-$(date +%s).pdf"

echo "Creating test file for user: $USERNAME"
docker compose exec syncthing sh -c "echo 'test' > /sync/$USERNAME/incoming/$TESTFILE"

echo "Watching logs..."
docker compose logs -f syncthing | grep -E "($TESTFILE|Watchdog event)" | head -20
```

Usage:
```bash
chmod +x test-watcher.sh
./test-watcher.sh alice
```

## Manual Verification Steps

### 1. Verify container health
```bash
docker compose ps
curl http://localhost:8080/health
```

### 2. Check user folders discovered
```bash
docker compose logs syncthing | grep "Discovered user"
```

### 3. Check folders being watched
```bash
docker compose logs syncthing | grep "Watching:"
```

### 4. Create test file
```bash
docker compose exec syncthing touch /sync/alice/incoming/test.pdf
```

### 5. Watch for detection
```bash
docker compose logs syncthing | tail -50
```

## Performance Issues

### Processing too slow

```bash
# Check CPU usage
docker stats syncthing mrdocument ocrmypdf

# Check processing time
docker compose logs syncthing | grep -E "(Processing|Saved)" | tail -20
```

### Too many files backing up

```bash
# Check pending files
docker compose exec syncthing find /sync/*/incoming/ -name "*.pdf" | wc -l

# Process manually if needed
docker compose restart syncthing
```

## Getting Help

When reporting issues, provide:

1. **Container status:**
   ```bash
   docker compose ps
   ```

2. **Logs (last 100 lines):**
   ```bash
   docker compose logs --tail=100 syncthing
   ```

3. **Folder structure:**
   ```bash
   docker compose exec syncthing tree /sync/
   # or
   docker compose exec syncthing find /sync/ -type d
   ```

4. **Test results:**
   ```bash
   # Output from manual file creation test
   ```

5. **Configuration:**
   ```bash
   docker compose config | grep -A 20 syncthing
   ```
