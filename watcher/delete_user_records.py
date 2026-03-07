#!/usr/bin/env python3
"""Delete all mrdocument records for a specific user.

Usage:
    python delete_user_records.py <username> [--dry-run]

Requires DATABASE_URL environment variable.
"""

import asyncio
import os
import sys


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__.strip())
        sys.exit(1)

    username = sys.argv[1]
    dry_run = "--dry-run" in sys.argv

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is required", file=sys.stderr)
        sys.exit(1)

    from db_new import DocumentDBv2
    db = DocumentDBv2(database_url)
    await db.connect()

    try:
        records = await db.get_snapshot(username)
        print(f"Found {len(records)} record(s) for user '{username}'")

        if not records:
            return

        if dry_run:
            for r in records:
                print(f"  [dry-run] would delete {r.id}  {r.original_filename}  state={r.state.value}")
            return

        for r in records:
            await db.delete_record(r.id)
            print(f"  deleted {r.id}  {r.original_filename}  state={r.state.value}")

        print(f"Deleted {len(records)} record(s)")
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
