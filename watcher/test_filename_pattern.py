"""Tests for conditional filename patterns and {source_filename} placeholder."""

import pytest

from sorter import ContextConfig, FilenameRule, _format_filename


# ---------------------------------------------------------------------------
# ContextConfig.from_dict — conditional filename parsing
# ---------------------------------------------------------------------------


class TestContextConfigFilenameRules:
    def test_simple_string_pattern(self):
        """String filename → no rules, pattern stored directly."""
        cfg = ContextConfig.from_dict({
            "name": "work",
            "filename": "{date}-{type}",
        })
        assert cfg is not None
        assert cfg.filename_pattern == "{date}-{type}"
        assert cfg.filename_rules == []

    def test_conditional_list_parsed(self):
        """List filename → rules stored, default used as filename_pattern."""
        cfg = ContextConfig.from_dict({
            "name": "work",
            "filename": [
                {"match": r".*\.(mp4|mov|wav)$", "pattern": "{date}-{source_filename}"},
                {"pattern": "{date}-{type}-{sender}"},
            ],
        })
        assert cfg is not None
        assert cfg.filename_pattern == "{date}-{type}-{sender}"
        assert len(cfg.filename_rules) == 2
        assert cfg.filename_rules[0].match == r".*\.(mp4|mov|wav)$"
        assert cfg.filename_rules[0].pattern == "{date}-{source_filename}"
        assert cfg.filename_rules[1].match is None
        assert cfg.filename_rules[1].pattern == "{date}-{type}-{sender}"

    def test_conditional_list_no_default_returns_none(self):
        """List with no default entry → None (error)."""
        cfg = ContextConfig.from_dict({
            "name": "work",
            "filename": [
                {"match": r".*\.mp4$", "pattern": "{date}-{source_filename}"},
            ],
        })
        assert cfg is None

    def test_conditional_list_multiple_match_rules(self):
        """Multiple match rules parsed in order."""
        cfg = ContextConfig.from_dict({
            "name": "work",
            "filename": [
                {"match": r"^scan_", "pattern": "{type}-{date}-{source_filename}"},
                {"match": r"^IMG_", "pattern": "{date}-{source_filename}"},
                {"pattern": "{type}-{date}-{sender}-{topic}"},
            ],
        })
        assert cfg is not None
        assert len(cfg.filename_rules) == 3


# ---------------------------------------------------------------------------
# ContextConfig.resolve_filename_pattern
# ---------------------------------------------------------------------------


class TestResolveFilenamePattern:
    def _make_config(self):
        return ContextConfig.from_dict({
            "name": "work",
            "filename": [
                {"match": r".*\.(mp4|mov|wav)$", "pattern": "{context}-{date}-{source_filename}"},
                {"match": r"^scan_", "pattern": "{type}-{date}-{source_filename}"},
                {"pattern": "{type}-{date}-{sender}-{topic}"},
            ],
        })

    def test_matches_first_rule(self):
        cfg = self._make_config()
        assert cfg.resolve_filename_pattern("meeting.mp4") == "{context}-{date}-{source_filename}"

    def test_matches_second_rule(self):
        cfg = self._make_config()
        assert cfg.resolve_filename_pattern("scan_001.pdf") == "{type}-{date}-{source_filename}"

    def test_falls_back_to_default(self):
        cfg = self._make_config()
        assert cfg.resolve_filename_pattern("invoice.pdf") == "{type}-{date}-{sender}-{topic}"

    def test_no_source_filename_returns_default(self):
        cfg = self._make_config()
        assert cfg.resolve_filename_pattern(None) == "{type}-{date}-{sender}-{topic}"

    def test_no_rules_returns_default(self):
        cfg = ContextConfig.from_dict({
            "name": "work",
            "filename": "{date}-{type}",
        })
        assert cfg.resolve_filename_pattern("anything.pdf") == "{date}-{type}"

    def test_first_match_wins(self):
        """File matches multiple rules → first one wins."""
        cfg = ContextConfig.from_dict({
            "name": "work",
            "filename": [
                {"match": r"\.wav$", "pattern": "pattern-a"},
                {"match": r"^recording.*\.wav$", "pattern": "pattern-b"},
                {"pattern": "default"},
            ],
        })
        assert cfg.resolve_filename_pattern("recording.wav") == "pattern-a"


# ---------------------------------------------------------------------------
# _format_filename — {source_filename} placeholder
# ---------------------------------------------------------------------------


class TestFormatFilenameSourceFilename:
    def test_source_filename_resolved(self):
        result = _format_filename(
            {"date": "2025-03-05", "context": "work"},
            "{context}-{date}-{source_filename}",
            source_filename="meeting_recording.mp4",
        )
        assert result == "work-2025-03-05-meeting_recording.pdf"

    def test_source_filename_strips_extension(self):
        result = _format_filename(
            {"date": "2025-01-01"},
            "{date}-{source_filename}",
            source_filename="scan_001.pdf",
        )
        assert result == "2025-01-01-scan_001.pdf"

    def test_source_filename_missing_produces_empty(self):
        result = _format_filename(
            {"date": "2025-01-01", "type": "Invoice"},
            "{type}-{date}-{source_filename}",
            source_filename=None,
        )
        assert result == "invoice-2025-01-01.pdf"

    def test_source_filename_not_in_pattern_ignored(self):
        result = _format_filename(
            {"date": "2025-01-01", "type": "Invoice"},
            "{type}-{date}",
            source_filename="scan_001.pdf",
        )
        assert result == "invoice-2025-01-01.pdf"

    def test_no_source_filename_param(self):
        """Backward compatibility — no source_filename param."""
        result = _format_filename(
            {"date": "2025-01-01", "type": "Invoice"},
            "{type}-{date}",
        )
        assert result == "invoice-2025-01-01.pdf"
