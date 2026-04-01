# MrDocument Client

Local MCP proxy for accessing MrDocument from Claude Code. Runs on your machine (VPN required) and connects to the remote MCP server.

## Prerequisites

- Python 3.10+
- [Poetry](https://python-poetry.org/docs/#installation)
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
- VPN connection to the MrDocument server

## Setup

```bash
cd mcp-client
make setup
```

Add to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
export MRDOCUMENT_USER=heike
export MRDOCUMENT_PASSWORD_FILE=/path/to/.mcp-password
```

The `.mcp-password` file is located in your sync directory on the server (e.g., `/sync/heike/mrdocument/.mcp-password`). Copy it to your local machine.

Restart your shell, then from this directory:

```bash
claude
```

Claude Code automatically picks up the MCP server config, project instructions, and skills from this directory.

## Usage

Once inside Claude Code, the MCP tools are available automatically. Examples:

```
Find all invoices from Schulze GmbH

Search for documents about Mietvertrag from 2024

Show me the summary of document <uuid>
```

### Skills

Use `/search-and-export` to bulk-export document text:

```
/search-and-export ./export all invoices from 2024
```

This searches for matching documents and saves each one as a `.txt` file in the specified folder.

## Claude Desktop

To use with Claude Desktop instead of Claude Code:

```bash
mrdocument-setup
```

This interactive wizard writes the Claude Desktop config with your credentials.
