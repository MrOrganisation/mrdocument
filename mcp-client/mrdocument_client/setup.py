"""Setup CLI: configures Claude Desktop and Claude Code for MrDocument.

Installs:
  1. Claude Desktop MCP server config (claude_desktop_config.json)
  2. Claude Code MCP server config (claude mcp add)
  3. Skills into ~/.claude/commands/
"""

import json
import os
import platform
import shutil
import subprocess
import sys
from importlib import resources
from pathlib import Path


def _claude_desktop_config_path() -> Path:
    system = platform.system()
    if system == "Darwin":
        return Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    elif system == "Windows":
        return Path(os.environ.get("APPDATA", "")) / "Claude" / "claude_desktop_config.json"
    else:
        # Linux / other
        return Path.home() / ".config" / "claude" / "claude_desktop_config.json"


def _claude_commands_dir() -> Path:
    return Path.home() / ".claude" / "commands"


def _skills_dir() -> Path:
    return Path(__file__).parent / "skills"


def _prompt(message: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{message}{suffix}: ").strip()
    return value or default


def _prompt_password(username: str, password_file_hint: str) -> str:
    """Prompt for the MCP password or path to .mcp-password file."""
    print(f"\nThe MCP password for '{username}' is stored in the .mcp-password")
    print(f"file in the user's sync directory on the server.")
    if password_file_hint:
        print(f"Hint: {password_file_hint}")
    print()

    choice = _prompt("Enter (p)assword directly or (f)ile path", "p")
    if choice.lower().startswith("f"):
        path = _prompt("Path to .mcp-password file")
        path = os.path.expanduser(path)
        if not os.path.isfile(path):
            print(f"  Warning: {path} does not exist yet")
        return f"file:{path}"
    else:
        return _prompt("MCP password")


def setup_desktop(url: str, username: str, password_source: str) -> None:
    """Write Claude Desktop MCP server configuration."""
    config_path = _claude_desktop_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    config = {}
    if config_path.is_file():
        try:
            config = json.loads(config_path.read_text())
        except json.JSONDecodeError:
            pass

    if "mcpServers" not in config:
        config["mcpServers"] = {}

    # Build the command args
    proxy_cmd = shutil.which("mrdocument-proxy")
    if proxy_cmd is None:
        # Fall back to running as module
        proxy_cmd = sys.executable
        args = [proxy_cmd, "-m", "mrdocument_client.proxy"]
    else:
        args = [proxy_cmd]

    args.extend(["--url", url, "--user", username])

    if password_source.startswith("file:"):
        args.extend(["--password-file", password_source[5:]])
    else:
        args.extend(["--password", password_source])

    config["mcpServers"]["mrdocument"] = {
        "command": args[0],
        "args": args[1:],
    }

    config_path.write_text(json.dumps(config, indent=2) + "\n")
    print(f"  Claude Desktop config written to {config_path}")


def setup_claude_code(url: str, username: str, password_source: str) -> None:
    """Register MCP server with Claude Code CLI."""
    claude_cmd = shutil.which("claude")
    if claude_cmd is None:
        print("  Claude Code CLI not found — skipping")
        return

    args = [
        claude_cmd, "mcp", "add", "mrdocument",
        "--transport", "stdio",
        "--",
    ]

    proxy_cmd = shutil.which("mrdocument-proxy")
    if proxy_cmd is None:
        args.extend([sys.executable, "-m", "mrdocument_client.proxy"])
    else:
        args.append(proxy_cmd)

    args.extend(["--url", url, "--user", username])

    if password_source.startswith("file:"):
        args.extend(["--password-file", password_source[5:]])
    else:
        args.extend(["--password", password_source])

    try:
        subprocess.run(args, check=True, timeout=10)
        print("  Registered MCP server with Claude Code")
    except subprocess.CalledProcessError as e:
        print(f"  Warning: claude mcp add failed: {e}")
    except FileNotFoundError:
        print("  Claude Code CLI not found — skipping")


def install_skills() -> None:
    """Install skill .md files into ~/.claude/commands/."""
    commands_dir = _claude_commands_dir()
    commands_dir.mkdir(parents=True, exist_ok=True)

    skills_dir = _skills_dir()
    if not skills_dir.is_dir():
        print("  No skills directory found — skipping")
        return

    installed = 0
    for skill_file in skills_dir.glob("*.md"):
        dest = commands_dir / skill_file.name
        shutil.copy2(skill_file, dest)
        installed += 1
        print(f"  Installed skill: {skill_file.stem}")

    if installed == 0:
        print("  No skills found to install")
    else:
        print(f"  {installed} skill(s) installed to {commands_dir}")


def main() -> None:
    print("MrDocument Client Setup")
    print("=" * 40)
    print()

    url = _prompt(
        "MCP server URL",
        "https://mcp.mrdocument.parmenides.net",
    )
    username = _prompt("Username (e.g., heike)")
    if not username:
        print("Error: username is required")
        sys.exit(1)

    password_source = _prompt_password(
        username,
        f"On the server: /sync/{username}/mrdocument/.mcp-password",
    )

    print()
    print("Configuring Claude Desktop...")
    setup_desktop(url, username, password_source)

    print()
    print("Configuring Claude Code...")
    setup_claude_code(url, username, password_source)

    print()
    print("Installing skills...")
    install_skills()

    print()
    print("Done! Restart Claude Desktop to pick up the new MCP server.")


if __name__ == "__main__":
    main()
