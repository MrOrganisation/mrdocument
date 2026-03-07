#!/usr/bin/env python3
"""Diagnose orphaned files in archive/ by cross-referencing against the DB.

Usage:
    python diagnose_archive.py <user_root> [--delete]

Requires DATABASE_URL environment variable.

Reports:
  - Archive files with no matching DB record (orphans)
  - DB records whose archive source no longer exists on disk
  - Records in HAS_ERROR or IS_MISSING state (stuck)
"""

import asyncio
import os
import sys
from pathlib import Path


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__.strip())
        sys.exit(1)

    user_root = Path(sys.argv[1])
    delete = "--delete" in sys.argv

    if not user_root.is_dir():
        print(f"ERROR: {user_root} is not a directory", file=sys.stderr)
        sys.exit(1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is required", file=sys.stderr)
        sys.exit(1)

    # Add watcher dir to path for imports
    watcher_dir = Path(__file__).parent
    if str(watcher_dir) not in sys.path:
        sys.path.insert(0, str(watcher_dir))

    from db_new import DocumentDBv2
    from sorter import get_username_from_root

    username = get_username_from_root(user_root)
    db = DocumentDBv2(database_url)
    await db.connect()

    try:
        records = await db.get_snapshot(username)
        archive_dir = user_root / "archive"
        sorted_dir = user_root / "sorted"

        # Build sets for cross-referencing
        db_source_hashes = {}  # source_hash -> record
        db_archive_paths = {}  # archive path -> record
        for r in records:
            db_source_hashes[r.source_hash] = r
            for pe in r.source_paths:
                if pe.path.startswith("archive/"):
                    db_archive_paths[pe.path] = r

        # Count files on disk
        archive_files = []
        if archive_dir.is_dir():
            archive_files = [f for f in archive_dir.iterdir()
                             if f.is_file() and not f.name.startswith(".")]

        sorted_files = []
        if sorted_dir.is_dir():
            sorted_files = [f for f in sorted_dir.rglob("*")
                            if f.is_file() and not f.is_symlink()
                            and not f.name.startswith(".")
                            and f.name not in ("context.yaml", "smartfolders.yaml", "generated.yaml")]

        print(f"User: {username}")
        print(f"DB records: {len(records)}")
        print(f"Archive files on disk: {len(archive_files)}")
        print(f"Sorted files on disk: {len(sorted_files)}")
        print()

        # 1. Orphaned archive files (no matching record)
        orphans = []
        for f in archive_files:
            rel = f"archive/{f.name}"
            if rel not in db_archive_paths:
                orphans.append(f)

        if orphans:
            print(f"ORPHANED ARCHIVE FILES ({len(orphans)}):")
            print("  (Files in archive/ with no matching DB record)")
            for f in sorted(orphans, key=lambda p: p.name):
                size_kb = f.stat().st_size / 1024
                print(f"  {f.name}  ({size_kb:.0f} KB)")
            print()

        # 2. Missing archive files (DB expects them but they're gone)
        missing = []
        for path, record in db_archive_paths.items():
            full = user_root / path
            if not full.exists():
                missing.append((path, record))

        if missing:
            print(f"MISSING ARCHIVE FILES ({len(missing)}):")
            print("  (DB records reference archive files that don't exist on disk)")
            for path, r in missing:
                print(f"  {path}  record={r.id}  state={r.state.value}")
            print()

        # 3. Stuck records
        from models import State
        error_records = [r for r in records if r.state == State.HAS_ERROR]
        missing_records = [r for r in records if r.state == State.IS_MISSING]

        if error_records:
            print(f"HAS_ERROR RECORDS ({len(error_records)}):")
            print("  (Processing failed — archive file exists but no output)")
            for r in error_records:
                print(f"  {r.id}  {r.original_filename}  src={r.source_file.path if r.source_file else 'none'}")
            print()

        if missing_records:
            print(f"IS_MISSING RECORDS ({len(missing_records)}):")
            print("  (Output file disappeared from disk)")
            for r in missing_records:
                cur = r.current_file.path if r.current_file else "none"
                mcur = [pe.path for pe in r.missing_current_paths]
                print(f"  {r.id}  {r.original_filename}  current={cur}  missing={mcur}")
            print()

        # 4. Records with archive source but no sorted/processed output
        no_output = []
        for r in records:
            if r.state == State.IS_COMPLETE:
                continue
            if r.state in (State.HAS_ERROR, State.IS_MISSING):
                continue  # already reported above
            if r.state in (State.IS_DELETED, State.NEEDS_DELETION):
                continue
            has_archive = any(pe.path.startswith("archive/") for pe in r.source_paths)
            has_output = any(
                pe.path.startswith("sorted/") or pe.path.startswith("processed/")
                for pe in r.current_paths
            )
            if has_archive and not has_output:
                no_output.append(r)

        if no_output:
            print(f"ARCHIVE WITHOUT OUTPUT ({len(no_output)}):")
            print("  (Records with archived source but no sorted/processed file)")
            for r in no_output:
                print(f"  {r.id}  {r.original_filename}  state={r.state.value}")
            print()

        # Summary
        total_issues = len(orphans) + len(missing) + len(error_records) + len(missing_records) + len(no_output)
        if total_issues == 0:
            print("No issues found.")
        else:
            print(f"Total issues: {total_issues}")

            if delete and orphans:
                print()
                print(f"Deleting {len(orphans)} orphaned archive files...")
                for f in orphans:
                    f.unlink()
                    print(f"  deleted {f.name}")

    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
