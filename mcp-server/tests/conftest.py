"""Shared test fixtures."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_sync_root(tmp_path):
    """Create a temporary sync root with user directories."""
    # Create user directory with password files
    # Layout: {sync_root}/{user}/mrdocument/.mcp-password
    user_dir = tmp_path / "testuser" / "mrdocument"
    user_dir.mkdir(parents=True)
    (user_dir / ".mcp-password").write_text("test-mcp-password-456")
    (user_dir / ".db-password").write_text("test-password-123")

    # Create sorted directory with context configs
    sorted_dir = user_dir / "sorted"
    sorted_dir.mkdir()

    privat_dir = sorted_dir / "privat"
    privat_dir.mkdir()
    (privat_dir / "context.yaml").write_text(
        """name: privat
description: Private documents
filename: "{context}-{type}-{date}-{sender}"
audio_filename: "{context}-{date}-{sender}-{type}"
fields:
  type:
    instructions: "Determine the document type"
    candidates:
      - "Arztbrief"
      - "Versicherung"
      - "Kontoauszug"
    allow_new_candidates: false
  sender:
    instructions: "Determine the sender organization"
    candidates:
      - "Allianz"
      - "Sparkasse"
    allow_new_candidates: true
folders:
  - "context"
  - "type"
"""
    )

    # Create generated.yaml for privat context
    (privat_dir / "generated.yaml").write_text(
        """fields:
  sender:
    candidates:
      - "Deutsche Post"
      - name: "Allianz"
        clues:
          - "allianz versicherung"
"""
    )

    arbeit_dir = sorted_dir / "arbeit"
    arbeit_dir.mkdir()
    (arbeit_dir / "context.yaml").write_text(
        """name: arbeit
description: Work documents
filename: "{context}-{type}-{date}-{sender}"
fields:
  type:
    instructions: "Document type"
    candidates:
      - "Rechnung"
      - "Vertrag"
    allow_new_candidates: true
  sender:
    instructions: "Sender"
    candidates:
      - name: "Schulze GmbH"
        short: "schulze"
        clues:
          - "schulze"
      - "Fischer AG"
    allow_new_candidates: true
folders:
  - "context"
  - "sender"
"""
    )

    # Create a second user (no password file — should be skipped by auth)
    other_user = tmp_path / "otheruser" / "mrdocument"
    other_user.mkdir(parents=True)

    return tmp_path
