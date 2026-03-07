"""Live integration tests for MrDocument cost tracking.

Verifies that the service records Anthropic API usage costs when
processing documents. The cost tracker writes to /costs/{username}/
inside the service container, which is volume-mounted to ./costs/ on
the host.

Requires:
    - Generated test documents in ``generated/`` (run ``generate_documents.py``)
    - Docker compose with ``./costs:/costs`` volume on the service
    - MrDocument service running with mock Anthropic adapter
"""

import json
import time
from pathlib import Path

import pytest

from conftest import (
    TestConfig,
    atomic_copy,
    poll_for_file,
)


# Use a unique document that is NOT used by other test classes to avoid
# DB deduplication interference.  This (stem, format) pair must be unique.
COST_DOC = {
    "file_stem": "privat_arztbrief_mueller",
    "fmt": "pdf",
    "context": "privat",
    "date": "2025-10-15",
}


class TestCostTracking:
    """Test that processing a document produces a cost tracking file."""

    def test_process_creates_cost_file(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        """After processing a document, a costs JSON file should appear
        with Anthropic usage data (input_tokens, output_tokens, cost)."""
        doc = COST_DOC
        src = generated_dir / f"{doc['file_stem']}.{doc['fmt']}"
        assert src.exists(), f"Source file missing: {src}"

        # --- Step 1: Copy document to incoming/ ---
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # --- Step 2: Wait for processing to complete (file in processed/) ---
        date = doc["date"]
        ctx = doc["context"]
        pattern = f"{ctx}-*{date}*.pdf"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert result is not None, (
            f"File not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # --- Step 3: Wait for cost file to appear ---
        # The cost tracker flushes every 30s, so we poll with a generous timeout.
        # The costs dir is volume-mounted at ./costs/ on the host.
        # Username is "testuser" (from /sync/testuser in the watcher container).
        costs_user_dir = test_config.costs_dir / "testuser"
        costs_file = costs_user_dir / "mrdocument_costs.json"

        deadline = time.monotonic() + 90  # generous: flush_interval=30s + processing
        while time.monotonic() < deadline:
            if costs_file.exists() and costs_file.stat().st_size > 0:
                break
            time.sleep(2)

        assert costs_file.exists(), (
            f"Cost file not created at {costs_file} within 90s. "
            f"Contents of costs_dir: {list(test_config.costs_dir.rglob('*'))}"
        )

        # --- Step 4: Verify cost file contents ---
        data = json.loads(costs_file.read_text())

        # Should have at least one date key and a "total" key
        assert "total" in data, f"Cost file missing 'total' key: {data.keys()}"

        total = data["total"]
        assert "anthropic" in total, f"No 'anthropic' section in total: {total}"

        anthropic_total = total["anthropic"]
        # At least one model should have been used
        assert len(anthropic_total) > 0, "No model entries in anthropic total"

        # Verify structure of at least one model entry
        for model_name, model_data in anthropic_total.items():
            assert "input_tokens" in model_data, f"Missing input_tokens for {model_name}"
            assert "output_tokens" in model_data, f"Missing output_tokens for {model_name}"
            assert "cost" in model_data, f"Missing cost for {model_name}"
            assert "documents" in model_data, f"Missing documents for {model_name}"
            assert model_data["input_tokens"] > 0, f"input_tokens is 0 for {model_name}"
            assert model_data["output_tokens"] > 0, f"output_tokens is 0 for {model_name}"
            assert model_data["cost"] > 0, f"cost is 0 for {model_name}"
            assert model_data["documents"] > 0, f"documents is 0 for {model_name}"
