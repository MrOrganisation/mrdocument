//! Database layer for document watcher v2.
//!
//! Uses sqlx with PostgreSQL to manage the `mrdocument.documents_v2` table,
//! providing CRUD and query operations for Record lifecycle tracking.

use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use tracing::{debug, info};
use uuid::Uuid;

use crate::models::{PathEntry, Record, State};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// SQL to create (or ensure) the mrdocument schema, table, indexes, and trigger.
pub const SCHEMA_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS mrdocument;

CREATE TABLE IF NOT EXISTS mrdocument.documents_v2 (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_filename       TEXT NOT NULL,
    source_hash             TEXT NOT NULL,
    -- INVARIANT: every non-NULL value across source_content_hash and
    -- content_hash must be unique among all records and both columns.
    -- Enforced at the application level (is_duplicate_hash, post-backfill dedup).
    source_content_hash     TEXT,

    -- Path lists (JSONB arrays of {"path": "...", "timestamp": "..."})
    source_paths            JSONB NOT NULL DEFAULT '[]',
    current_paths           JSONB NOT NULL DEFAULT '[]',
    missing_source_paths    JSONB NOT NULL DEFAULT '[]',
    missing_current_paths   JSONB NOT NULL DEFAULT '[]',

    -- Content
    context                 TEXT,
    metadata                JSONB,
    assigned_filename       TEXT,
    hash                    TEXT,
    content_hash            TEXT,   -- see INVARIANT above

    -- Processing
    output_filename         TEXT,
    state                   TEXT NOT NULL DEFAULT 'is_new'
                            CHECK (state IN (
                                'is_new', 'needs_processing', 'is_missing',
                                'has_error', 'needs_deletion',
                                'is_deleted', 'is_complete'
                            )),

    -- Temp fields
    target_path             TEXT,
    source_reference        TEXT,
    current_reference       TEXT,
    duplicate_sources       JSONB NOT NULL DEFAULT '[]',
    deleted_paths           JSONB NOT NULL DEFAULT '[]',

    -- Owner
    username                TEXT,

    -- Timestamps
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drop legacy V1 tables if they still exist
DROP TABLE IF EXISTS mrdocument.file_locations;
DROP TABLE IF EXISTS mrdocument.documents;

-- Migrations for content hash columns (idempotent).
-- Must run before CREATE INDEX so the columns exist on pre-existing tables.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'mrdocument' AND table_name = 'documents_v2'
        AND column_name = 'source_content_hash'
    ) THEN
        ALTER TABLE mrdocument.documents_v2 ADD COLUMN source_content_hash TEXT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'mrdocument' AND table_name = 'documents_v2'
        AND column_name = 'content_hash'
    ) THEN
        ALTER TABLE mrdocument.documents_v2 ADD COLUMN content_hash TEXT;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_docs_v2_source_hash
    ON mrdocument.documents_v2(source_hash);

CREATE INDEX IF NOT EXISTS idx_docs_v2_hash
    ON mrdocument.documents_v2(hash)
    WHERE hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_docs_v2_source_content_hash
    ON mrdocument.documents_v2(source_content_hash)
    WHERE source_content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_docs_v2_content_hash
    ON mrdocument.documents_v2(content_hash)
    WHERE content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_docs_v2_output_filename
    ON mrdocument.documents_v2(output_filename)
    WHERE output_filename IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_docs_v2_state
    ON mrdocument.documents_v2(state);

CREATE INDEX IF NOT EXISTS idx_docs_v2_metadata
    ON mrdocument.documents_v2 USING gin(metadata);

CREATE INDEX IF NOT EXISTS idx_docs_v2_username
    ON mrdocument.documents_v2(username)
    WHERE username IS NOT NULL;

-- Auto-update trigger on updated_at
CREATE OR REPLACE FUNCTION mrdocument.update_documents_v2_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'documents_v2_updated_at'
    ) THEN
        CREATE TRIGGER documents_v2_updated_at
            BEFORE UPDATE ON mrdocument.documents_v2
            FOR EACH ROW EXECUTE FUNCTION mrdocument.update_documents_v2_updated_at();
    END IF;
END
$$;
"#;

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

/// PostgreSQL database interface for document watcher v2.
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Connect to the database and ensure the schema exists.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .min_connections(2)
            .max_connections(10)
            .connect(url)
            .await
            .context("Failed to connect to database")?;

        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&pool)
            .await
            .context("Failed to execute schema SQL")?;

        info!("Database connected and schema ready");
        Ok(Self { pool })
    }

    /// Ensure the schema exists (idempotent).
    #[allow(dead_code)]
    pub async fn ensure_schema(&self) -> Result<()> {
        sqlx::raw_sql(SCHEMA_SQL)
            .execute(&self.pool)
            .await
            .context("Failed to execute schema SQL")?;
        Ok(())
    }

    /// Get a reference to the connection pool.
    #[allow(dead_code)]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // =====================================================================
    // Conversion helpers
    // =====================================================================

    /// Serialize a list of PathEntry to a JSON string.
    fn path_entries_to_json(entries: &[PathEntry]) -> serde_json::Value {
        serde_json::Value::Array(
            entries
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "path": e.path,
                        "timestamp": e.timestamp.to_rfc3339(),
                    })
                })
                .collect(),
        )
    }

    /// Deserialize a JSON value to a list of PathEntry.
    fn json_to_path_entries(data: &serde_json::Value) -> Vec<PathEntry> {
        match data.as_array() {
            Some(arr) => arr
                .iter()
                .filter_map(|item| {
                    let path = item.get("path")?.as_str()?.to_string();
                    let ts_str = item.get("timestamp")?.as_str()?;
                    let timestamp: DateTime<Utc> = ts_str.parse().ok()?;
                    Some(PathEntry { path, timestamp })
                })
                .collect(),
            None => Vec::new(),
        }
    }

    /// Convert a database row to a Record.
    fn row_to_record(row: &sqlx::postgres::PgRow) -> Result<Record> {
        let id: Uuid = row.try_get("id")?;
        let original_filename: String = row.try_get("original_filename")?;
        let source_hash: String = row.try_get("source_hash")?;
        let source_content_hash: Option<String> = row.try_get("source_content_hash")?;

        let source_paths_json: serde_json::Value = row.try_get("source_paths")?;
        let current_paths_json: serde_json::Value = row.try_get("current_paths")?;
        let missing_source_paths_json: serde_json::Value = row.try_get("missing_source_paths")?;
        let missing_current_paths_json: serde_json::Value = row.try_get("missing_current_paths")?;

        let context: Option<String> = row.try_get("context")?;
        let metadata: Option<serde_json::Value> = row.try_get("metadata")?;
        let assigned_filename: Option<String> = row.try_get("assigned_filename")?;
        let hash: Option<String> = row.try_get("hash")?;
        let content_hash: Option<String> = row.try_get("content_hash")?;

        let output_filename: Option<String> = row.try_get("output_filename")?;
        let state_str: String = row.try_get("state")?;
        let state: State = state_str
            .parse()
            .map_err(|e: String| anyhow::anyhow!(e))?;

        let target_path: Option<String> = row.try_get("target_path")?;
        let source_reference: Option<String> = row.try_get("source_reference")?;
        let current_reference: Option<String> = row.try_get("current_reference")?;
        let duplicate_sources_json: serde_json::Value = row.try_get("duplicate_sources")?;
        let deleted_paths_json: serde_json::Value = row.try_get("deleted_paths")?;
        let username: Option<String> = row.try_get("username")?;

        let duplicate_sources: Vec<String> = duplicate_sources_json
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let deleted_paths: Vec<String> = deleted_paths_json
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Record {
            id,
            original_filename,
            source_hash,
            source_content_hash,
            source_paths: Self::json_to_path_entries(&source_paths_json),
            current_paths: Self::json_to_path_entries(&current_paths_json),
            missing_source_paths: Self::json_to_path_entries(&missing_source_paths_json),
            missing_current_paths: Self::json_to_path_entries(&missing_current_paths_json),
            context,
            metadata,
            assigned_filename,
            hash,
            content_hash,
            output_filename,
            state,
            target_path,
            source_reference,
            current_reference,
            duplicate_sources,
            deleted_paths,
            username,
        })
    }

    // =====================================================================
    // CRUD operations
    // =====================================================================

    /// Insert a new record. Returns the UUID.
    pub async fn create_record(&self, record: &Record) -> Result<Uuid> {
        let source_paths_json = Self::path_entries_to_json(&record.source_paths);
        let current_paths_json = Self::path_entries_to_json(&record.current_paths);
        let missing_source_json = Self::path_entries_to_json(&record.missing_source_paths);
        let missing_current_json = Self::path_entries_to_json(&record.missing_current_paths);
        let dup_json = serde_json::Value::Array(
            record
                .duplicate_sources
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        );
        let del_json = serde_json::Value::Array(
            record
                .deleted_paths
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        );

        sqlx::query(
            r#"
            INSERT INTO mrdocument.documents_v2 (
                id, original_filename, source_hash, source_content_hash,
                source_paths, current_paths,
                missing_source_paths, missing_current_paths,
                context, metadata, assigned_filename, hash, content_hash,
                output_filename, state,
                target_path, source_reference, current_reference,
                duplicate_sources, deleted_paths,
                username
            ) VALUES (
                $1, $2, $3, $4,
                $5, $6,
                $7, $8,
                $9, $10, $11, $12, $13,
                $14, $15,
                $16, $17, $18,
                $19, $20,
                $21
            )
            "#,
        )
        .bind(record.id)
        .bind(&record.original_filename)
        .bind(&record.source_hash)
        .bind(&record.source_content_hash)
        .bind(&source_paths_json)
        .bind(&current_paths_json)
        .bind(&missing_source_json)
        .bind(&missing_current_json)
        .bind(&record.context)
        .bind(&record.metadata)
        .bind(&record.assigned_filename)
        .bind(&record.hash)
        .bind(&record.content_hash)
        .bind(&record.output_filename)
        .bind(record.state.as_str())
        .bind(&record.target_path)
        .bind(&record.source_reference)
        .bind(&record.current_reference)
        .bind(&dup_json)
        .bind(&del_json)
        .bind(&record.username)
        .execute(&self.pool)
        .await
        .context("Failed to create record")?;

        debug!("Created record {}: {}", record.id, record.original_filename);
        Ok(record.id)
    }

    /// Get a record by ID, or None if not found.
    #[allow(dead_code)]
    pub async fn get_record(&self, record_id: Uuid) -> Result<Option<Record>> {
        let row = sqlx::query("SELECT * FROM mrdocument.documents_v2 WHERE id = $1")
            .bind(record_id)
            .fetch_optional(&self.pool)
            .await
            .context("Failed to get record")?;

        match row {
            Some(r) => Ok(Some(Self::row_to_record(&r)?)),
            None => Ok(None),
        }
    }

    /// Full update of an existing record.
    pub async fn save_record(&self, record: &Record) -> Result<()> {
        let source_paths_json = Self::path_entries_to_json(&record.source_paths);
        let current_paths_json = Self::path_entries_to_json(&record.current_paths);
        let missing_source_json = Self::path_entries_to_json(&record.missing_source_paths);
        let missing_current_json = Self::path_entries_to_json(&record.missing_current_paths);
        let dup_json = serde_json::Value::Array(
            record
                .duplicate_sources
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        );
        let del_json = serde_json::Value::Array(
            record
                .deleted_paths
                .iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        );

        sqlx::query(
            r#"
            UPDATE mrdocument.documents_v2 SET
                original_filename = $2,
                source_hash = $3,
                source_content_hash = $4,
                source_paths = $5,
                current_paths = $6,
                missing_source_paths = $7,
                missing_current_paths = $8,
                context = $9,
                metadata = $10,
                assigned_filename = $11,
                hash = $12,
                content_hash = $13,
                output_filename = $14,
                state = $15,
                target_path = $16,
                source_reference = $17,
                current_reference = $18,
                duplicate_sources = $19,
                deleted_paths = $20
            WHERE id = $1
            "#,
        )
        .bind(record.id)
        .bind(&record.original_filename)
        .bind(&record.source_hash)
        .bind(&record.source_content_hash)
        .bind(&source_paths_json)
        .bind(&current_paths_json)
        .bind(&missing_source_json)
        .bind(&missing_current_json)
        .bind(&record.context)
        .bind(&record.metadata)
        .bind(&record.assigned_filename)
        .bind(&record.hash)
        .bind(&record.content_hash)
        .bind(&record.output_filename)
        .bind(record.state.as_str())
        .bind(&record.target_path)
        .bind(&record.source_reference)
        .bind(&record.current_reference)
        .bind(&dup_json)
        .bind(&del_json)
        .execute(&self.pool)
        .await
        .context("Failed to save record")?;

        debug!("Saved record {}", record.id);
        Ok(())
    }

    /// Delete a record by ID. Returns true if deleted, false if not found.
    pub async fn delete_record(&self, record_id: Uuid) -> Result<bool> {
        let result =
            sqlx::query("DELETE FROM mrdocument.documents_v2 WHERE id = $1")
                .bind(record_id)
                .execute(&self.pool)
                .await
                .context("Failed to delete record")?;

        let deleted = result.rows_affected() > 0;
        if deleted {
            debug!("Deleted record {}", record_id);
        }
        Ok(deleted)
    }

    /// Update only the content hash columns for a record.
    /// Only sets a column if the provided value is Some; leaves existing value otherwise.
    pub async fn update_content_hashes(
        &self,
        record_id: Uuid,
        source_content_hash: Option<&str>,
        content_hash: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE mrdocument.documents_v2
            SET source_content_hash = COALESCE($2, source_content_hash),
                content_hash = COALESCE($3, content_hash)
            WHERE id = $1
            "#,
        )
        .bind(record_id)
        .bind(source_content_hash)
        .bind(content_hash)
        .execute(&self.pool)
        .await
        .context("Failed to update content hashes")?;
        Ok(())
    }

    // =====================================================================
    // Query operations
    // =====================================================================

    /// Get all records, optionally filtered by username, ordered by created_at.
    pub async fn get_snapshot(&self, username: Option<&str>) -> Result<Vec<Record>> {
        let rows = match username {
            Some(u) => {
                sqlx::query(
                    "SELECT * FROM mrdocument.documents_v2 WHERE username = $1 ORDER BY created_at",
                )
                .bind(u)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    "SELECT * FROM mrdocument.documents_v2 ORDER BY created_at",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.iter()
            .map(Self::row_to_record)
            .collect::<Result<Vec<_>>>()
    }

    /// Get records where any temp field is non-null or state=needs_deletion.
    pub async fn get_records_with_temp_fields(
        &self,
        username: Option<&str>,
    ) -> Result<Vec<Record>> {
        let rows = match username {
            Some(u) => {
                sqlx::query(
                    r#"
                    SELECT * FROM mrdocument.documents_v2
                    WHERE username = $1
                      AND (target_path IS NOT NULL
                        OR source_reference IS NOT NULL
                        OR current_reference IS NOT NULL
                        OR duplicate_sources != '[]'::jsonb
                        OR deleted_paths != '[]'::jsonb
                        OR state = 'needs_deletion')
                    "#,
                )
                .bind(u)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT * FROM mrdocument.documents_v2
                    WHERE target_path IS NOT NULL
                       OR source_reference IS NOT NULL
                       OR current_reference IS NOT NULL
                       OR duplicate_sources != '[]'::jsonb
                       OR deleted_paths != '[]'::jsonb
                       OR state = 'needs_deletion'
                    "#,
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.iter()
            .map(Self::row_to_record)
            .collect::<Result<Vec<_>>>()
    }

    /// Get records where output_filename is set.
    pub async fn get_records_with_output_filename(
        &self,
        username: Option<&str>,
    ) -> Result<Vec<Record>> {
        let rows = match username {
            Some(u) => {
                sqlx::query(
                    r#"
                    SELECT * FROM mrdocument.documents_v2
                    WHERE output_filename IS NOT NULL AND username = $1
                    "#,
                )
                .bind(u)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT * FROM mrdocument.documents_v2
                    WHERE output_filename IS NOT NULL
                    "#,
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.iter()
            .map(Self::row_to_record)
            .collect::<Result<Vec<_>>>()
    }

    /// Get a record by source_hash (most recent first).
    #[allow(dead_code)]
    pub async fn get_record_by_source_hash(
        &self,
        source_hash: &str,
    ) -> Result<Option<Record>> {
        let row = sqlx::query(
            r#"
            SELECT * FROM mrdocument.documents_v2
            WHERE source_hash = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(source_hash)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to get record by source_hash")?;

        match row {
            Some(r) => Ok(Some(Self::row_to_record(&r)?)),
            None => Ok(None),
        }
    }

    /// Get a record by hash (most recent first).
    #[allow(dead_code)]
    pub async fn get_record_by_hash(&self, hash_value: &str) -> Result<Option<Record>> {
        let row = sqlx::query(
            r#"
            SELECT * FROM mrdocument.documents_v2
            WHERE hash = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(hash_value)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to get record by hash")?;

        match row {
            Some(r) => Ok(Some(Self::row_to_record(&r)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: CREATE INDEX on `source_content_hash` and `content_hash`
    /// ran before the migration that adds those columns, causing a crash
    /// on databases created before the content hash feature was added.
    #[test]
    fn test_schema_migration_runs_before_index_creation() {
        let sql = SCHEMA_SQL;

        // Find the positions of key statements
        let alter_source_content_hash = sql
            .find("ADD COLUMN source_content_hash")
            .expect("migration for source_content_hash not found in SCHEMA_SQL");
        let alter_content_hash = sql
            .find("ADD COLUMN content_hash")
            .expect("migration for content_hash not found in SCHEMA_SQL");

        let index_source_content_hash = sql
            .find("idx_docs_v2_source_content_hash")
            .expect("index for source_content_hash not found in SCHEMA_SQL");
        let index_content_hash = sql
            .find("idx_docs_v2_content_hash")
            .expect("index for content_hash not found in SCHEMA_SQL");

        assert!(
            alter_source_content_hash < index_source_content_hash,
            "ALTER TABLE ADD COLUMN source_content_hash (pos {}) must come before \
             CREATE INDEX idx_docs_v2_source_content_hash (pos {})",
            alter_source_content_hash,
            index_source_content_hash,
        );
        assert!(
            alter_content_hash < index_content_hash,
            "ALTER TABLE ADD COLUMN content_hash (pos {}) must come before \
             CREATE INDEX idx_docs_v2_content_hash (pos {})",
            alter_content_hash,
            index_content_hash,
        );
    }
}
