"""Comprehensive tests for the MongoDB-style query DSL parser."""

import pytest

from mcp_server.query_dsl import QueryBuilder, QueryBuildError


class TestSimpleEquality:
    def test_eq_string(self):
        b = QueryBuilder()
        sql, params = b.build({"context": {"$eq": "arbeit"}})
        assert sql == "context = $1"
        assert params == ["arbeit"]

    def test_eq_shorthand(self):
        b = QueryBuilder()
        sql, params = b.build({"context": "arbeit"})
        assert sql == "context = $1"
        assert params == ["arbeit"]

    def test_eq_null(self):
        b = QueryBuilder()
        sql, params = b.build({"description": {"$eq": None}})
        assert sql == "description IS NULL"
        assert params == []

    def test_ne_string(self):
        b = QueryBuilder()
        sql, params = b.build({"state": {"$ne": "is_deleted"}})
        assert sql == "state != $1"
        assert params == ["is_deleted"]

    def test_ne_null(self):
        b = QueryBuilder()
        sql, params = b.build({"content": {"$ne": None}})
        assert sql == "content IS NOT NULL"
        assert params == []


class TestTextOperators:
    def test_like(self):
        b = QueryBuilder()
        sql, params = b.build({"original_filename": {"$like": "%.pdf"}})
        assert sql == "original_filename LIKE $1"
        assert params == ["%.pdf"]

    def test_ilike(self):
        b = QueryBuilder()
        sql, params = b.build({"description": {"$ilike": "%vertrag%"}})
        assert sql == "description ILIKE $1"
        assert params == ["%vertrag%"]

    def test_like_on_non_text_field_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="\\$like not supported"):
            b.build({"tags": {"$like": "%x%"}})

    def test_ilike_on_date_field_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="\\$ilike not supported"):
            b.build({"date_added": {"$ilike": "%x%"}})


class TestInOperator:
    def test_in_array(self):
        b = QueryBuilder()
        sql, params = b.build({"state": {"$in": ["is_complete", "is_missing"]}})
        assert sql == "state = ANY($1)"
        assert params == [["is_complete", "is_missing"]]

    def test_in_non_array_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="\\$in requires an array"):
            b.build({"state": {"$in": "is_complete"}})

    def test_in_empty_array(self):
        b = QueryBuilder()
        sql, params = b.build({"state": {"$in": []}})
        assert sql == "FALSE"


class TestContainsOperator:
    def test_tags_contains_string(self):
        b = QueryBuilder()
        sql, params = b.build({"tags": {"$contains": "invoice"}})
        assert "tags @>" in sql
        assert "::jsonb" in sql
        assert params == ['"invoice"']

    def test_metadata_contains_object(self):
        b = QueryBuilder()
        sql, params = b.build({"metadata": {"$contains": {"type": "Rechnung"}}})
        assert "metadata @>" in sql
        assert "::jsonb" in sql

    def test_contains_on_text_field_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="\\$contains only supported"):
            b.build({"description": {"$contains": "x"}})


class TestComparisonOperators:
    def test_gt(self):
        from datetime import date
        b = QueryBuilder()
        sql, params = b.build({"date_added": {"$gt": "2024-01-01"}})
        assert sql == "date_added > $1"
        assert params == [date(2024, 1, 1)]

    def test_gte(self):
        b = QueryBuilder()
        sql, params = b.build({"created_at": {"$gte": "2024-01-01T00:00:00"}})
        assert sql == "created_at >= $1"

    def test_lt(self):
        b = QueryBuilder()
        sql, params = b.build({"date_added": {"$lt": "2025-01-01"}})
        assert sql == "date_added < $1"

    def test_lte(self):
        b = QueryBuilder()
        sql, params = b.build({"updated_at": {"$lte": "2025-12-31"}})
        assert sql == "updated_at <= $1"

    def test_range_combined(self):
        from datetime import date
        b = QueryBuilder()
        sql, params = b.build(
            {"date_added": {"$gte": "2024-01-01", "$lt": "2025-01-01"}}
        )
        assert "date_added >= $1" in sql
        assert "date_added < $2" in sql
        assert params == [date(2024, 1, 1), date(2025, 1, 1)]


class TestExistsOperator:
    def test_exists_true_on_column(self):
        b = QueryBuilder()
        sql, params = b.build({"description": {"$exists": True}})
        assert sql == "description IS NOT NULL"

    def test_exists_false_on_column(self):
        b = QueryBuilder()
        sql, params = b.build({"summary": {"$exists": False}})
        assert sql == "summary IS NULL"

    def test_exists_true_on_metadata_key(self):
        b = QueryBuilder()
        sql, params = b.build({"metadata.sender": {"$exists": True}})
        assert "metadata ? $1" in sql
        assert params == ["sender"]

    def test_exists_false_on_metadata_key(self):
        b = QueryBuilder()
        sql, params = b.build({"metadata.sender": {"$exists": False}})
        assert "NOT (metadata ? $1)" in sql

    def test_exists_non_bool_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="\\$exists requires a boolean"):
            b.build({"metadata.sender": {"$exists": "yes"}})


class TestSearchOperator:
    def test_search_on_content(self):
        b = QueryBuilder()
        sql, params = b.build({"content": {"$search": "Mietvertrag Berlin"}})
        assert "content_tsv @@ plainto_tsquery(" in sql
        assert "CASE language" in sql
        assert params == ["Mietvertrag Berlin"]

    def test_search_on_non_content_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="\\$search is only supported on 'content'"):
            b.build({"description": {"$search": "test"}})


class TestMetadataFields:
    def test_simple_metadata_key(self):
        b = QueryBuilder()
        sql, params = b.build({"metadata.sender": {"$eq": "Schulze GmbH"}})
        assert sql == "metadata->>'sender' = $1"
        assert params == ["Schulze GmbH"]

    def test_deep_metadata_path(self):
        b = QueryBuilder()
        sql, params = b.build({"metadata.address.city": {"$eq": "Berlin"}})
        assert sql == "metadata->'address'->>'city' = $1"
        assert params == ["Berlin"]

    def test_metadata_ilike(self):
        b = QueryBuilder()
        sql, params = b.build({"metadata.sender": {"$ilike": "%schulze%"}})
        assert sql == "metadata->>'sender' ILIKE $1"

    def test_invalid_metadata_key_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="Invalid metadata key segment"):
            b.build({"metadata.'; DROP TABLE --": {"$eq": "x"}})

    def test_metadata_key_with_spaces_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="Invalid metadata key segment"):
            b.build({"metadata.some key": {"$eq": "x"}})

    def test_metadata_key_with_dash_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="Invalid metadata key segment"):
            b.build({"metadata.some-key": {"$eq": "x"}})


class TestLogicalCombinators:
    def test_and(self):
        b = QueryBuilder()
        sql, params = b.build(
            {"$and": [{"context": {"$eq": "arbeit"}}, {"state": {"$eq": "is_complete"}}]}
        )
        assert "AND" in sql
        assert "context = $1" in sql
        assert "state = $2" in sql
        assert params == ["arbeit", "is_complete"]

    def test_or(self):
        b = QueryBuilder()
        sql, params = b.build(
            {"$or": [{"context": {"$eq": "arbeit"}}, {"context": {"$eq": "privat"}}]}
        )
        assert "OR" in sql
        assert params == ["arbeit", "privat"]

    def test_implicit_and_multiple_keys(self):
        b = QueryBuilder()
        sql, params = b.build(
            {"context": {"$eq": "arbeit"}, "state": {"$eq": "is_complete"}}
        )
        assert "AND" in sql

    def test_nested_or_and(self):
        b = QueryBuilder()
        sql, params = b.build(
            {
                "$or": [
                    {"context": {"$eq": "arbeit"}},
                    {
                        "$and": [
                            {"metadata.type": {"$eq": "Rechnung"}},
                            {"tags": {"$contains": "urgent"}},
                        ]
                    },
                ]
            }
        )
        assert "OR" in sql
        assert "AND" in sql

    def test_and_non_array_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="requires an array"):
            b.build({"$and": {"context": {"$eq": "x"}}})

    def test_or_empty_array_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="must not be empty"):
            b.build({"$or": []})

    def test_excessive_nesting_raises(self):
        b = QueryBuilder()
        # Build deeply nested query
        query = {"context": {"$eq": "x"}}
        for _ in range(15):
            query = {"$and": [query]}
        with pytest.raises(QueryBuildError, match="nesting exceeds maximum"):
            b.build(query)


class TestEmptyQuery:
    def test_empty_dict(self):
        b = QueryBuilder()
        sql, params = b.build({})
        assert sql == ""
        assert params == []


class TestUnknownFieldsAndOperators:
    def test_unknown_field_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="Unknown field"):
            b.build({"nonexistent": {"$eq": "x"}})

    def test_unknown_operator_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="Unknown operator"):
            b.build({"context": {"$regex": ".*"}})

    def test_unknown_top_level_operator_raises(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError, match="Unknown top-level operator"):
            b.build({"$not": [{"context": {"$eq": "x"}}]})


class TestSQLInjectionSafety:
    def test_value_injection_is_parameterized(self):
        b = QueryBuilder()
        sql, params = b.build(
            {"context": {"$eq": "'; DROP TABLE mrdocument.documents_v2; --"}}
        )
        # Value must be in params, not in SQL
        assert "DROP TABLE" not in sql
        assert params == ["'; DROP TABLE mrdocument.documents_v2; --"]

    def test_metadata_key_injection_blocked(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError):
            b.build({"metadata.key' OR '1'='1": {"$eq": "x"}})

    def test_metadata_key_sql_comment_blocked(self):
        b = QueryBuilder()
        with pytest.raises(QueryBuildError):
            b.build({"metadata.key--comment": {"$eq": "x"}})


class TestParameterNumbering:
    def test_sequential_params(self):
        b = QueryBuilder()
        sql, params = b.build(
            {
                "context": {"$eq": "arbeit"},
                "metadata.sender": {"$ilike": "%schulze%"},
                "content": {"$search": "Rechnung"},
            }
        )
        assert "$1" in sql
        assert "$2" in sql
        assert "$3" in sql
        assert len(params) == 3
