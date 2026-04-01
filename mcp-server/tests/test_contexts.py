"""Tests for the context reader module."""

import pytest

from mcp_server.contexts import ContextReadError, ContextReader


class TestListContexts:
    def test_lists_all_contexts(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        contexts = reader.list_contexts("testuser")
        names = {c["name"] for c in contexts}
        assert names == {"privat", "arbeit"}

    def test_context_has_required_fields(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        contexts = reader.list_contexts("testuser")
        privat = next(c for c in contexts if c["name"] == "privat")
        assert privat["description"] == "Private documents"
        assert privat["filename_pattern"] == "{context}-{type}-{date}-{sender}"
        assert privat["folders"] == ["context", "type"]

    def test_nonexistent_user_returns_empty(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        assert reader.list_contexts("nobody") == []

    def test_user_without_sorted_dir_returns_empty(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        # otheruser has no sorted/ directory
        assert reader.list_contexts("otheruser") == []


class TestListFields:
    def test_lists_fields_for_context(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        fields = reader.list_fields("testuser", "privat")
        names = {f["name"] for f in fields}
        assert names == {"type", "sender"}

    def test_field_has_metadata(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        fields = reader.list_fields("testuser", "privat")
        type_field = next(f for f in fields if f["name"] == "type")
        assert type_field["instructions"] == "Determine the document type"
        assert type_field["allow_new_candidates"] is False
        assert type_field["candidate_count"] == 3  # Arztbrief, Versicherung, Kontoauszug

    def test_field_candidate_count_includes_generated(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        fields = reader.list_fields("testuser", "privat")
        sender_field = next(f for f in fields if f["name"] == "sender")
        # Base: Allianz, Sparkasse. Generated: Deutsche Post (Allianz merges clues only)
        assert sender_field["candidate_count"] == 3

    def test_nonexistent_context_raises(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        with pytest.raises(ContextReadError, match="not found"):
            reader.list_fields("testuser", "nonexistent")


class TestListCandidates:
    def test_returns_base_candidates(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        result = reader.list_candidates("testuser", "privat", "type")
        assert result["context"] == "privat"
        assert result["field"] == "type"
        assert result["allow_new_candidates"] is False
        assert result["candidates"] == ["Arztbrief", "Versicherung", "Kontoauszug"]

    def test_merges_generated_candidates(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        result = reader.list_candidates("testuser", "privat", "sender")
        candidate_names = []
        for c in result["candidates"]:
            if isinstance(c, str):
                candidate_names.append(c)
            elif isinstance(c, dict):
                candidate_names.append(c.get("name", ""))
        assert "Allianz" in candidate_names
        assert "Sparkasse" in candidate_names
        assert "Deutsche Post" in candidate_names

    def test_merges_clues_for_existing_candidate(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        result = reader.list_candidates("testuser", "privat", "sender")
        # Find the Allianz candidate — it should have merged clues
        allianz = None
        for c in result["candidates"]:
            if isinstance(c, dict) and c.get("name") == "Allianz":
                allianz = c
                break
            elif isinstance(c, str) and c == "Allianz":
                # It was upgraded to an object with clues
                pass
        # The base "Allianz" string gets clues merged from generated
        # Check that clues exist somewhere in the result
        all_clues = []
        for c in result["candidates"]:
            if isinstance(c, dict) and "clues" in c:
                all_clues.extend(c["clues"])
        assert "allianz versicherung" in all_clues

    def test_complex_candidates_with_short(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        result = reader.list_candidates("testuser", "arbeit", "sender")
        # Should have Schulze GmbH (with short and clues) and Fischer AG
        names = []
        for c in result["candidates"]:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, dict):
                names.append(c.get("name", ""))
        assert "Schulze GmbH" in names
        assert "Fischer AG" in names

    def test_nonexistent_field_raises(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        with pytest.raises(ContextReadError, match="not found"):
            reader.list_candidates("testuser", "privat", "nonexistent")

    def test_case_insensitive_context_lookup(self, tmp_sync_root):
        reader = ContextReader(str(tmp_sync_root), subdir="")
        # Should work with different casing
        result = reader.list_candidates("testuser", "Privat", "type")
        assert result["context"] == "Privat"
        assert len(result["candidates"]) == 3
