# Filename Collision Handling

## Problem

When multiple PDFs have similar content (e.g., invoices from the same company on the same date), the AI might generate identical filenames:

**Example:**
- File 1: `invoice.pdf` → AI generates: `invoice-2024-01-15-acme_corp.pdf`
- File 2: `acme-bill.pdf` → AI generates: `invoice-2024-01-15-acme_corp.pdf`

Without collision handling, File 2 would **overwrite** File 1, and File 1 would be **lost forever** (original was already deleted from `incoming/`).

## Solution

The watcher checks if the destination filename already exists. If it does, it appends a short UUID to make it unique.

## Behavior

### No Collision

```
Input:  invoice.pdf
Output: invoice-2024-01-15-acme_corp.pdf
```

### Collision Detected

```
Input:  first-invoice.pdf
Output: invoice-2024-01-15-acme_corp.pdf

Input:  second-invoice.pdf (AI generates same filename)
Output: invoice-2024-01-15-acme_corp-a7b3c4d2.pdf
                                      └─ 8-char UUID
```

## Logs

**No collision:**
```
[alice] Processing: invoice.pdf
[alice] Saved: invoice-2024-01-15-acme_corp.pdf
```

**With collision:**
```
[alice] Processing: similar-invoice.pdf
[alice] File already exists: invoice-2024-01-15-acme_corp.pdf (original: similar-invoice.pdf)
[alice] Renamed to avoid collision: invoice-2024-01-15-acme_corp-a7b3c4d2.pdf
[alice] Saved: invoice-2024-01-15-acme_corp-a7b3c4d2.pdf
```

## UUID Format

- Uses first 8 characters of UUID v4
- Format: `{ai-generated-name}-{uuid}.pdf`
- Example UUID: `a7b3c4d2` (hexadecimal)
- Collision probability: ~1 in 4 billion

## Why UUID?

**Alternatives considered:**

1. **Counter suffix** (`_1`, `_2`, etc.)
   - ❌ Requires checking all existing files
   - ❌ Race condition with concurrent processing
   - ❌ Numbers not very descriptive

2. **Timestamp** (`-20241206-125735`)
   - ❌ Still possible collision if processed in same second
   - ❌ Redundant (date often in filename already)

3. **Original filename** (`-original_name`)
   - ❌ Can be very long
   - ❌ May contain special characters
   - ❌ Still needs counter for multiple files with same original name

4. **UUID** ✅
   - ✅ Guaranteed unique (statistically)
   - ✅ No race conditions
   - ✅ Fixed length (8 chars)
   - ✅ No need to check existing files
   - ✅ Works with concurrent processing

## Examples

### Multiple invoices from same vendor

```
invoice-acme-jan.pdf       → invoice-2024-01-15-acme_corp.pdf
invoice-acme-feb.pdf       → invoice-2024-02-15-acme_corp.pdf
invoice-acme-jan-dup.pdf   → invoice-2024-01-15-acme_corp-f3a8b1c7.pdf ← collision
```

### Concurrent processing

```
File 1 & File 2 arrive simultaneously, both generate: receipt-2024-01-15-store.pdf

Thread 1: Saves receipt-2024-01-15-store.pdf
Thread 2: Detects collision, saves receipt-2024-01-15-store-d4e9f2a1.pdf
```

No race condition because UUID is generated before checking file existence.

## User Experience

**Client sees:**
```
~/Synced/incoming/
  invoice-jan.pdf
  invoice-feb.pdf

↓ After processing ↓

~/Synced/processed/
  invoice-2024-01-15-acme_corp.pdf
  invoice-2024-01-15-acme_corp-a7b3c4d2.pdf
```

The UUID suffix makes it clear there was a collision. User can:
1. Open both files to see which is which
2. Rename manually if desired
3. Check logs to see original filenames

## Monitoring Collisions

Check for collisions in logs:

```bash
# View all collisions
docker compose logs syncthing | grep "File already exists"

# Count collisions
docker compose logs syncthing | grep -c "File already exists"

# View collision resolutions
docker compose logs syncthing | grep "Renamed to avoid collision"
```

## Configuration

Currently no configuration options. Behavior is:
- **Always check** for existing file
- **Always append UUID** if collision detected
- **Always log** warning and resolution

## Future Enhancements

Possible improvements:
1. Make UUID length configurable (default: 8)
2. Option to use full UUID (32 chars)
3. Option to use timestamp instead of UUID
4. Option to overwrite (dangerous, not recommended)
5. Store original filename in metadata file alongside PDF

## Technical Details

**UUID Generation:**
```python
import uuid
unique_id = str(uuid.uuid4())[:8]  # First 8 chars
# Example: "a7b3c4d2"
```

**Filename Construction:**
```python
base_name = "invoice-2024-01-15-acme_corp"
extension = ".pdf"
new_filename = f"{base_name}-{unique_id}{extension}"
# Result: "invoice-2024-01-15-acme_corp-a7b3c4d2.pdf"
```

**Collision Check:**
```python
output_path = output_dir / suggested_filename
if output_path.exists():
    # Append UUID and save with new name
```

## Testing

Test collision handling:

```bash
# Create two files with content that generates same AI filename
# (e.g., two invoices from same company/date)

cp invoice1.pdf ~/Synced/incoming/
cp invoice2.pdf ~/Synced/incoming/

# Watch logs
docker compose logs -f syncthing | grep -E "(Saved|collision|Renamed)"
```

Expected output:
```
[alice] Saved: invoice-2024-01-15-acme_corp.pdf
[alice] File already exists: invoice-2024-01-15-acme_corp.pdf
[alice] Renamed to avoid collision: invoice-2024-01-15-acme_corp-f8a2b3c4.pdf
[alice] Saved: invoice-2024-01-15-acme_corp-f8a2b3c4.pdf
```

## Summary

✅ **No data loss** - Collisions are detected and handled
✅ **Unique filenames** - UUID guarantees uniqueness  
✅ **Concurrent safe** - No race conditions
✅ **Logged** - All collisions logged for review
✅ **Automatic** - No user intervention required
