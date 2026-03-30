"""
MongoDB-style query DSL parser that produces parameterized PostgreSQL WHERE clauses.

All user-provided values are passed as bind parameters ($1, $2, ...).
Field names are validated against an allowlist.
Metadata key paths are validated with a strict regex.
"""

import re
from datetime import date, datetime
from typing import Any


# Columns that can be queried directly
ALLOWED_COLUMNS: set[str] = {
    "context",
    "original_filename",
    "assigned_filename",
    "description",
    "summary",
    "content",
    "language",
    "state",
    "tags",
    "date_added",
    "created_at",
    "updated_at",
    "metadata",
}

# Text columns that support LIKE/ILIKE
TEXT_COLUMNS: set[str] = {
    "context",
    "original_filename",
    "assigned_filename",
    "description",
    "summary",
    "content",
    "language",
    "state",
}

# Date/timestamp columns that support comparison operators
DATE_COLUMNS: set[str] = {
    "date_added",
    "created_at",
    "updated_at",
}

# Valid operators
OPERATORS: set[str] = {
    "$eq", "$ne", "$like", "$ilike", "$in",
    "$contains", "$gt", "$gte", "$lt", "$lte",
    "$exists", "$search",
}

# Strict regex for metadata key segments: alphanumeric + underscore only
METADATA_KEY_SEGMENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

MAX_NESTING_DEPTH = 10


class QueryBuildError(Exception):
    """Raised when a query DSL cannot be translated to SQL."""


class QueryBuilder:
    """
    Translates a MongoDB-style query DSL into a parameterized PostgreSQL WHERE clause.

    Usage:
        builder = QueryBuilder()
        where_clause, params = builder.build({"context": {"$eq": "arbeit"}})
        # where_clause = "context = $1"
        # params = ["arbeit"]
    """

    def __init__(self) -> None:
        self._params: list[Any] = []
        self._param_counter: int = 0

    def build(self, query: dict) -> tuple[str, list[Any]]:
        """Parse a DSL query dict and return (where_clause, params).

        Returns ("", []) for an empty query.
        """
        if not query:
            return "", []
        clause = self._parse_node(query, depth=0)
        return clause, self._params

    def _add_param(self, value: Any) -> str:
        """Add a parameter and return its placeholder ($N)."""
        self._param_counter += 1
        self._params.append(value)
        return f"${self._param_counter}"

    def _parse_node(self, node: dict, depth: int) -> str:
        """Recursively parse a query node into SQL conditions."""
        if depth > MAX_NESTING_DEPTH:
            raise QueryBuildError(
                f"Query nesting exceeds maximum depth of {MAX_NESTING_DEPTH}"
            )

        if not isinstance(node, dict):
            raise QueryBuildError(f"Expected dict, got {type(node).__name__}")

        conditions: list[str] = []

        for key, value in node.items():
            if key == "$and":
                conditions.append(self._parse_logical("AND", value, depth))
            elif key == "$or":
                conditions.append(self._parse_logical("OR", value, depth))
            elif key.startswith("$"):
                raise QueryBuildError(f"Unknown top-level operator: {key}")
            else:
                # Field condition
                conditions.append(self._parse_field(key, value, depth))

        if not conditions:
            raise QueryBuildError("Empty query node")

        if len(conditions) == 1:
            return conditions[0]
        return "(" + " AND ".join(conditions) + ")"

    def _parse_logical(self, op: str, items: Any, depth: int) -> str:
        """Parse $and / $or arrays."""
        if not isinstance(items, list):
            raise QueryBuildError(f"${op.lower()} requires an array, got {type(items).__name__}")
        if not items:
            raise QueryBuildError(f"${op.lower()} array must not be empty")

        clauses = [self._parse_node(item, depth + 1) for item in items]

        if len(clauses) == 1:
            return clauses[0]
        return "(" + f" {op} ".join(clauses) + ")"

    def _parse_field(self, field: str, spec: Any, depth: int) -> str:
        """Parse a field condition.

        Supports:
          - {"field": {"$op": value}} explicit operator
          - {"field": value} shorthand for $eq
        """
        if isinstance(spec, dict):
            # Multiple operators on the same field are AND-combined
            conditions = []
            for op, val in spec.items():
                if not op.startswith("$"):
                    raise QueryBuildError(
                        f"Expected operator (starting with $) for field '{field}', got '{op}'"
                    )
                conditions.append(self._apply_operator(field, op, val))
            if not conditions:
                raise QueryBuildError(f"Empty operator dict for field '{field}'")
            if len(conditions) == 1:
                return conditions[0]
            return "(" + " AND ".join(conditions) + ")"
        else:
            # Shorthand: {"field": "value"} means $eq
            return self._apply_operator(field, "$eq", spec)

    def _apply_operator(self, field: str, op: str, value: Any) -> str:
        """Apply a single operator to a field, returning a SQL condition."""
        if op not in OPERATORS:
            raise QueryBuildError(f"Unknown operator: {op}")

        sql_field = self._resolve_field(field)
        is_metadata = field.startswith("metadata.") or field == "metadata"
        is_tags = field == "tags"
        is_text = field in TEXT_COLUMNS or is_metadata
        is_date = field in DATE_COLUMNS

        if op == "$eq":
            if value is None:
                return f"{sql_field} IS NULL"
            p = self._add_param(_coerce_date(field, value))
            return f"{sql_field} = {p}"

        elif op == "$ne":
            if value is None:
                return f"{sql_field} IS NOT NULL"
            p = self._add_param(_coerce_date(field, value))
            return f"{sql_field} != {p}"

        elif op == "$like":
            if not is_text:
                raise QueryBuildError(f"$like not supported on field '{field}'")
            p = self._add_param(str(value))
            return f"{sql_field} LIKE {p}"

        elif op == "$ilike":
            if not is_text:
                raise QueryBuildError(f"$ilike not supported on field '{field}'")
            p = self._add_param(str(value))
            return f"{sql_field} ILIKE {p}"

        elif op == "$in":
            if not isinstance(value, list):
                raise QueryBuildError(f"$in requires an array, got {type(value).__name__}")
            if not value:
                # Empty $in matches nothing
                return "FALSE"
            p = self._add_param(value)
            return f"{sql_field} = ANY({p})"

        elif op == "$contains":
            if is_tags:
                # tags @> '"value"'::jsonb
                import json
                p = self._add_param(json.dumps(value))
                return f"tags @> {p}::jsonb"
            elif is_metadata and field == "metadata":
                # metadata @> '{"key": "val"}'::jsonb
                import json
                p = self._add_param(json.dumps(value))
                return f"metadata @> {p}::jsonb"
            else:
                raise QueryBuildError(
                    f"$contains only supported on 'tags' and 'metadata', not '{field}'"
                )

        elif op in ("$gt", "$gte", "$lt", "$lte"):
            sql_op = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}[op]
            p = self._add_param(_coerce_date(field, value))
            return f"{sql_field} {sql_op} {p}"

        elif op == "$exists":
            if not isinstance(value, bool):
                raise QueryBuildError(f"$exists requires a boolean, got {type(value).__name__}")
            if is_metadata and field.startswith("metadata."):
                # Check if key exists in metadata JSONB
                key_path = field.split(".", 1)[1]
                segments = key_path.split(".")
                if len(segments) == 1:
                    p = self._add_param(segments[0])
                    if value:
                        return f"metadata ? {p}"
                    else:
                        return f"NOT (metadata ? {p})"
                else:
                    # Deep path: metadata->'a' ? 'b'
                    container = "metadata"
                    for seg in segments[:-1]:
                        container = f"{container}->'{_safe_key(seg)}'"
                    p = self._add_param(segments[-1])
                    if value:
                        return f"{container} ? {p}"
                    else:
                        return f"NOT ({container} ? {p})"
            else:
                if value:
                    return f"{sql_field} IS NOT NULL"
                else:
                    return f"{sql_field} IS NULL"

        elif op == "$search":
            if field != "content":
                raise QueryBuildError(
                    f"$search is only supported on 'content', not '{field}'"
                )
            p = self._add_param(str(value))
            # Match the tsquery config to the per-row language column,
            # mirroring the trigger that populates content_tsv.
            return (
                "content_tsv @@ plainto_tsquery("
                "CASE language "
                "WHEN 'de' THEN 'german' "
                "WHEN 'en' THEN 'english' "
                "WHEN 'fr' THEN 'french' "
                "WHEN 'es' THEN 'spanish' "
                "WHEN 'it' THEN 'italian' "
                "WHEN 'nl' THEN 'dutch' "
                "WHEN 'pt' THEN 'portuguese' "
                "ELSE 'simple' "
                f"END::regconfig, {p})"
            )

        raise QueryBuildError(f"Unhandled operator: {op}")  # pragma: no cover

    def _resolve_field(self, field_name: str) -> str:
        """Map a DSL field name to a SQL expression.

        - "context" -> "context"
        - "metadata.sender" -> "metadata->>'sender'"
        - "metadata.address.city" -> "metadata->'address'->>'city'"
        """
        if field_name in ALLOWED_COLUMNS:
            return field_name

        if field_name.startswith("metadata."):
            key_path = field_name[len("metadata."):]
            segments = key_path.split(".")
            for seg in segments:
                if not METADATA_KEY_SEGMENT_RE.match(seg):
                    raise QueryBuildError(
                        f"Invalid metadata key segment: '{seg}'. "
                        "Only alphanumeric characters and underscores are allowed."
                    )
            if len(segments) == 1:
                return f"metadata->>'{_safe_key(segments[0])}'"
            else:
                # Deep path: metadata->'a'->'b'->>'c'
                parts = ["metadata"]
                for seg in segments[:-1]:
                    parts.append(f"->'{_safe_key(seg)}'")
                parts.append(f"->>'{_safe_key(segments[-1])}'")
                return "".join(parts)

        raise QueryBuildError(
            f"Unknown field: '{field_name}'. "
            f"Allowed fields: {', '.join(sorted(ALLOWED_COLUMNS))}, metadata.<key>"
        )


def _coerce_date(field: str, value: Any) -> Any:
    """Convert string values to date/datetime for date columns.

    asyncpg requires native Python date/datetime objects for DATE
    and TIMESTAMPTZ columns — it will not accept ISO-format strings.
    """
    if not isinstance(value, str):
        return value
    if field == "date_added":
        try:
            return date.fromisoformat(value)
        except ValueError:
            return value
    if field in ("created_at", "updated_at"):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _safe_key(key: str) -> str:
    """Escape a metadata key for safe inclusion in SQL.

    Since keys are already validated by METADATA_KEY_SEGMENT_RE
    (alphanumeric + underscore only), this is a defense-in-depth measure
    that escapes single quotes.
    """
    return key.replace("'", "''")
