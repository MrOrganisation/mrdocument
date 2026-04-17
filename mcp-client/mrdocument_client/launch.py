"""Launch Claude Code in an arbitrary directory with the MrDocument bundle wired up."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

BUNDLE_ROOT = Path(__file__).resolve().parent.parent
SYMLINKED = (".claude", "CLAUDE.md")
SYSTEM_PROMPT = BUNDLE_ROOT / "system-prompt.txt"


def _ensure_symlink(source: Path, target: Path) -> None:
    if target.is_symlink():
        try:
            if target.resolve() == source.resolve():
                return
        except OSError:
            pass
        target.unlink()
    elif target.exists():
        sys.exit(f"refusing to overwrite existing {target} (not a symlink)")
    target.symlink_to(source)


def _write_mcp_config(target: Path) -> None:
    config = {
        "mcpServers": {
            "mrdocument": {
                "command": "poetry",
                "args": [
                    "--directory", str(BUNDLE_ROOT),
                    "run", "mrdocument-proxy",
                    "--url", "${MRDOCUMENT_URL:-https://mcp.mrdocument.parmenides.net}",
                    "--user", "${MRDOCUMENT_USER}",
                    "--password-file", "${MRDOCUMENT_PASSWORD_FILE}",
                    "--verbose",
                ],
            }
        }
    }
    (target / ".mcp.json").write_text(json.dumps(config, indent=2) + "\n")


def _has_previous_session(workdir: Path) -> bool:
    encoded = "-" + str(workdir).strip("/").replace("/", "-")
    session_dir = Path.home() / ".claude" / "projects" / encoded
    return session_dir.is_dir() and any(session_dir.glob("*.jsonl"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch Claude Code in <path> with the MrDocument bundle linked in."
    )
    parser.add_argument("path", type=Path, help="Target directory to launch Claude Code in.")
    args = parser.parse_args()

    target = args.path.expanduser().resolve()
    if not target.is_dir():
        sys.exit(f"not a directory: {target}")

    for name in SYMLINKED:
        _ensure_symlink(BUNDLE_ROOT / name, target / name)
    _write_mcp_config(target)

    cmd = ["claude", "--permission-mode", "auto"]
    if _has_previous_session(target):
        cmd.append("--continue")
    cmd.extend(["--append-system-prompt-file", str(SYSTEM_PROMPT)])

    os.chdir(target)
    try:
        os.execvp(cmd[0], cmd)
    except FileNotFoundError:
        sys.exit("claude not found in PATH")


if __name__ == "__main__":
    main()
