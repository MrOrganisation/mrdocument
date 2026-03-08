//! Data models for the document watcher v2.
//!
//! Defines State machine, change tracking types, and the Record struct
//! that represents a document throughout its lifecycle.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Document lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    IsNew,
    NeedsProcessing,
    IsComplete,
    IsMissing,
    HasError,
    NeedsDeletion,
    IsDeleted,
}

impl State {
    /// Return the canonical snake_case string for this state.
    pub fn as_str(&self) -> &'static str {
        match self {
            State::IsNew => "is_new",
            State::NeedsProcessing => "needs_processing",
            State::IsComplete => "is_complete",
            State::IsMissing => "is_missing",
            State::HasError => "has_error",
            State::NeedsDeletion => "needs_deletion",
            State::IsDeleted => "is_deleted",
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for State {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "is_new" => Ok(State::IsNew),
            "needs_processing" => Ok(State::NeedsProcessing),
            "is_complete" => Ok(State::IsComplete),
            "is_missing" => Ok(State::IsMissing),
            "has_error" => Ok(State::HasError),
            "needs_deletion" => Ok(State::NeedsDeletion),
            "is_deleted" => Ok(State::IsDeleted),
            other => Err(format!("unknown state: {}", other)),
        }
    }
}

impl Serialize for State {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for State {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        State::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// EventType
// ---------------------------------------------------------------------------

/// Change event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Addition,
    Removal,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Addition => "addition",
            EventType::Removal => "removal",
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EventType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "addition" => Ok(EventType::Addition),
            "removal" => Ok(EventType::Removal),
            other => Err(format!("unknown event type: {}", other)),
        }
    }
}

impl Serialize for EventType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for EventType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        EventType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// PathEntry
// ---------------------------------------------------------------------------

/// A path with its associated timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathEntry {
    pub path: String,
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// ChangeItem
// ---------------------------------------------------------------------------

/// Represents a single filesystem change event.
#[derive(Debug, Clone)]
pub struct ChangeItem {
    pub event_type: EventType,
    pub path: String,
    pub hash: Option<String>,
    pub size: Option<u64>,
}

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

/// The central document record that tracks a document throughout its lifecycle.
#[derive(Debug, Clone)]
pub struct Record {
    // Identity
    pub id: Uuid,
    pub original_filename: String,
    pub source_hash: String,

    // Paths
    pub source_paths: Vec<PathEntry>,
    pub current_paths: Vec<PathEntry>,
    pub missing_source_paths: Vec<PathEntry>,
    pub missing_current_paths: Vec<PathEntry>,

    // Content
    pub context: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub assigned_filename: Option<String>,
    pub hash: Option<String>,

    // Processing
    pub output_filename: Option<String>,
    pub state: State,

    // Temp fields
    pub target_path: Option<String>,
    pub source_reference: Option<String>,
    pub current_reference: Option<String>,
    pub duplicate_sources: Vec<String>,
    pub deleted_paths: Vec<String>,

    // Owner
    pub username: Option<String>,
}

impl Record {
    /// Create a new Record with the given identity fields; everything else is defaulted.
    pub fn new(original_filename: String, source_hash: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            original_filename,
            source_hash,
            source_paths: Vec::new(),
            current_paths: Vec::new(),
            missing_source_paths: Vec::new(),
            missing_current_paths: Vec::new(),
            context: None,
            metadata: None,
            assigned_filename: None,
            hash: None,
            output_filename: None,
            state: State::IsNew,
            target_path: None,
            source_reference: None,
            current_reference: None,
            duplicate_sources: Vec::new(),
            deleted_paths: Vec::new(),
            username: None,
        }
    }

    // ----- Path helpers (most recent by timestamp) -----

    /// Most recent source [`PathEntry`] by timestamp, or `None`.
    pub fn source_file(&self) -> Option<&PathEntry> {
        self.source_paths
            .iter()
            .max_by_key(|pe| pe.timestamp)
    }

    /// Most recent current [`PathEntry`] by timestamp, or `None`.
    pub fn current_file(&self) -> Option<&PathEntry> {
        self.current_paths
            .iter()
            .max_by_key(|pe| pe.timestamp)
    }

    // ----- decompose_path -----

    /// Decompose a path into `(location, location_path, filename)`.
    ///
    /// # Examples
    /// ```
    /// # use watcher_rs::models::Record;
    /// assert_eq!(
    ///     Record::decompose_path("archive/sub/file.pdf"),
    ///     ("archive".into(), "sub".into(), "file.pdf".into()),
    /// );
    /// assert_eq!(
    ///     Record::decompose_path("archive/file.pdf"),
    ///     ("archive".into(), "".into(), "file.pdf".into()),
    /// );
    /// assert_eq!(
    ///     Record::decompose_path(".output/uuid"),
    ///     (".output".into(), "".into(), "uuid".into()),
    /// );
    /// ```
    pub fn decompose_path(path: &str) -> (String, String, String) {
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.is_empty() {
            return (String::new(), String::new(), String::new());
        }
        let location = parts[0].to_string();
        let filename = parts[parts.len() - 1].to_string();
        let location_path = if parts.len() > 2 {
            parts[1..parts.len() - 1].join("/")
        } else {
            String::new()
        };
        (location, location_path, filename)
    }

    // ----- Derived path properties -----

    /// Location component of the most recent source path.
    pub fn source_location(&self) -> Option<String> {
        self.source_file()
            .map(|sf| Self::decompose_path(&sf.path).0)
    }

    /// Location-path component of the most recent source path.
    pub fn source_location_path(&self) -> Option<String> {
        self.source_file()
            .map(|sf| Self::decompose_path(&sf.path).1)
    }

    /// Filename component of the most recent source path.
    pub fn source_filename(&self) -> Option<String> {
        self.source_file()
            .map(|sf| Self::decompose_path(&sf.path).2)
    }

    /// Location component of the most recent current path.
    pub fn current_location(&self) -> Option<String> {
        self.current_file()
            .map(|cf| Self::decompose_path(&cf.path).0)
    }

    /// Location-path component of the most recent current path.
    pub fn current_location_path(&self) -> Option<String> {
        self.current_file()
            .map(|cf| Self::decompose_path(&cf.path).1)
    }

    /// Filename component of the most recent current path.
    pub fn current_filename(&self) -> Option<String> {
        self.current_file()
            .map(|cf| Self::decompose_path(&cf.path).2)
    }

    /// Reset all temporary fields to their defaults.
    pub fn clear_temporary_fields(&mut self) {
        self.target_path = None;
        self.source_reference = None;
        self.current_reference = None;
        self.duplicate_sources = Vec::new();
        self.deleted_paths = Vec::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_roundtrip() {
        for state in [
            State::IsNew,
            State::NeedsProcessing,
            State::IsComplete,
            State::IsMissing,
            State::HasError,
            State::NeedsDeletion,
            State::IsDeleted,
        ] {
            let s = state.as_str();
            let parsed: State = s.parse().unwrap();
            assert_eq!(parsed, state);
            assert_eq!(state.to_string(), s);
        }
    }

    #[test]
    fn test_decompose_path() {
        assert_eq!(
            Record::decompose_path("archive/sub/file.pdf"),
            ("archive".into(), "sub".into(), "file.pdf".into()),
        );
        assert_eq!(
            Record::decompose_path("archive/file.pdf"),
            ("archive".into(), "".into(), "file.pdf".into()),
        );
        assert_eq!(
            Record::decompose_path(".output/uuid"),
            (".output".into(), "".into(), "uuid".into()),
        );
    }

    #[test]
    fn test_source_file_max_timestamp() {
        let mut rec = Record::new("test.pdf".into(), "abc".into());
        let t1 = "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let t2 = "2024-06-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let t3 = "2024-03-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        rec.source_paths = vec![
            PathEntry { path: "a/first.pdf".into(), timestamp: t1 },
            PathEntry { path: "a/latest.pdf".into(), timestamp: t2 },
            PathEntry { path: "a/middle.pdf".into(), timestamp: t3 },
        ];
        let sf = rec.source_file().unwrap();
        assert_eq!(sf.path, "a/latest.pdf");
        assert_eq!(sf.timestamp, t2);
    }

    #[test]
    fn test_clear_temporary_fields() {
        let mut rec = Record::new("test.pdf".into(), "abc".into());
        rec.target_path = Some("tp".into());
        rec.source_reference = Some("sr".into());
        rec.current_reference = Some("cr".into());
        rec.duplicate_sources = vec!["d".into()];
        rec.deleted_paths = vec!["p".into()];
        rec.clear_temporary_fields();
        assert!(rec.target_path.is_none());
        assert!(rec.source_reference.is_none());
        assert!(rec.current_reference.is_none());
        assert!(rec.duplicate_sources.is_empty());
        assert!(rec.deleted_paths.is_empty());
    }
}
