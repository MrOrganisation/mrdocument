//! Preprocessing and reconciliation for document watcher.
//!
//! Pure functions: no I/O, no DB, no filesystem operations.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::warn;
use uuid::Uuid;

use crate::models::{ChangeItem, EventType, PathEntry, Record, State};

// ---------------------------------------------------------------------------
// Location-aware matching table
// ---------------------------------------------------------------------------

/// Configuration for each top-level location.
///
/// Tuple: `(match_fields_in_priority_order, allows_new_records)`.
/// - `"source_hash"` -- compare `change.hash` vs `record.source_hash` -> `source_paths`
/// - `"hash"` -- compare `change.hash` vs `record.hash` -> `current_paths`
pub static LOCATION_CONFIG: Lazy<HashMap<&'static str, (&'static [&'static str], bool)>> =
    Lazy::new(|| {
        let mut m: HashMap<&str, (&[&str], bool)> = HashMap::new();
        m.insert("incoming", (&["source_hash", "hash"], true));
        m.insert("sorted", (&["hash", "source_hash"], true));
        m.insert("archive", (&["source_hash"], false));
        m.insert("missing", (&["source_hash"], false));
        m.insert("processed", (&["hash"], false));
        m.insert("reviewed", (&["hash"], false));
        m.insert("reset", (&["hash"], false));
        m.insert("trash", (&["source_hash", "hash"], false));
        m
    });

/// Valid locations for `current_paths` entries.
const VALID_CURRENT_LOCATIONS: &[&str] = &[".output", "processed", "reset", "reviewed", "sorted"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn find_by_source_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records.iter().find(|r| r.source_hash == hash_value)
}

fn find_by_source_hash_mut<'a>(
    records: &'a mut [Record],
    hash_value: &str,
) -> Option<&'a mut Record> {
    records.iter_mut().find(|r| r.source_hash == hash_value)
}

fn find_by_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records
        .iter()
        .find(|r| r.hash.as_deref() == Some(hash_value))
}

fn find_by_hash_mut<'a>(records: &'a mut [Record], hash_value: &str) -> Option<&'a mut Record> {
    records
        .iter_mut()
        .find(|r| r.hash.as_deref() == Some(hash_value))
}

fn find_by_output_filename_mut<'a>(
    records: &'a mut [Record],
    filename: &str,
) -> Option<&'a mut Record> {
    records
        .iter_mut()
        .find(|r| r.output_filename.as_deref() == Some(filename))
}

fn is_duplicate_hash(records: &[Record], hash_value: &str, exclude_id: Uuid) -> bool {
    if hash_value.is_empty() {
        return false;
    }
    for r in records {
        if r.id == exclude_id {
            continue;
        }
        if r.source_hash == hash_value {
            return true;
        }
        if r.hash.as_deref() == Some(hash_value) {
            return true;
        }
    }
    false
}

/// Regex matching collision suffixes: `_<8 hex chars>` before the file extension.
static COLLISION_SUFFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"_[0-9a-f]{8}(\.[^.]+)$").unwrap());

/// Check if `actual_path` is `expected_path` or a collision-suffixed variant.
fn is_collision_variant(actual_path: &str, expected_path: &str) -> bool {
    if actual_path == expected_path {
        return true;
    }
    let stripped = COLLISION_SUFFIX_RE.replace(actual_path, "$1");
    stripped == expected_path
}

/// Decompose a relative path into `(location, loc_path, filename)`.
fn decompose_path(path: &str) -> (String, String, String) {
    Record::decompose_path(path)
}

/// Add record index to modified list if not already there.
fn mark_modified(record_id: Uuid, modified_ids: &mut HashSet<Uuid>) {
    modified_ids.insert(record_id);
}

// ---------------------------------------------------------------------------
// compute_target_path
// ---------------------------------------------------------------------------

/// Compute expected target path in `sorted/` for a record.
///
/// Uses `context_folders` config to build subdirectory hierarchy from metadata.
/// Falls back to `sorted/{context}/{filename}` if folders not configured.
pub fn compute_target_path(
    record: &Record,
    context_folders: Option<&HashMap<String, Vec<String>>>,
) -> Option<String> {
    let context = record.context.as_ref()?;
    let assigned = record.assigned_filename.as_ref()?;

    if let Some(folders_map) = context_folders {
        if let Some(folders) = folders_map.get(context) {
            if let Some(ref metadata) = record.metadata {
                let mut parts: Vec<String> = Vec::new();
                for field in folders {
                    if let Some(value) = metadata.get(field) {
                        if let Some(s) = value.as_str() {
                            if !s.is_empty() {
                                parts.push(s.to_string());
                                continue;
                            }
                        } else if !value.is_null() {
                            parts.push(value.to_string());
                            continue;
                        }
                    }
                    break;
                }
                if !parts.is_empty() {
                    return Some(format!("sorted/{}/{}", parts.join("/"), assigned));
                }
            }
        }
    }

    Some(format!("sorted/{}/{}", context, assigned))
}

// ---------------------------------------------------------------------------
// preprocess
// ---------------------------------------------------------------------------

/// Process filesystem changes against existing records.
///
/// The `read_sidecar` parameter is a closure that reads sidecar JSON for
/// an `.output` path, returning a `serde_json::Value`.
///
/// Returns `(modified_records, new_records)`.
pub fn preprocess<F>(
    changes: &[ChangeItem],
    records: &mut Vec<Record>,
    new_records: &mut Vec<Record>,
    read_sidecar: F,
) -> (Vec<Uuid>, Vec<Record>)
where
    F: Fn(&str) -> serde_json::Value,
{
    let mut modified_ids: HashSet<Uuid> = HashSet::new();
    let mut created: Vec<Record> = Vec::new();
    let now = Utc::now();

    for change in changes {
        match change.event_type {
            EventType::Addition => {
                handle_addition(
                    change,
                    records,
                    new_records,
                    &mut created,
                    &read_sidecar,
                    &mut modified_ids,
                    now,
                );
            }
            EventType::Removal => {
                handle_removal(change, records, &mut modified_ids);
            }
        }
    }

    (modified_ids.into_iter().collect(), created)
}

fn handle_addition<F>(
    change: &ChangeItem,
    records: &mut Vec<Record>,
    existing_new: &mut Vec<Record>,
    created: &mut Vec<Record>,
    read_sidecar: &F,
    modified_ids: &mut HashSet<Uuid>,
    now: DateTime<Utc>,
) where
    F: Fn(&str) -> serde_json::Value,
{
    let (location, _, filename) = decompose_path(&change.path);

    // .output handling
    if location == ".output" {
        // Find index first, then check duplicate with immutable borrow
        let idx = records.iter().position(|r| r.output_filename.as_deref() == Some(&filename));
        if let Some(idx) = idx {
            let record_id = records[idx].id;
            if change.size == Some(0) {
                records[idx].state = State::HasError;
                records[idx].output_filename = None;
            } else {
                let sidecar = read_sidecar(&change.path);
                let change_hash = change.hash.as_deref().unwrap_or("");

                if is_duplicate_hash(records, change_hash, record_id) {
                    warn!(
                        "Duplicate hash {} for output {}, discarding result",
                        change_hash, change.path
                    );
                    let record = &mut records[idx];
                    record.deleted_paths.push(change.path.clone());
                    // Move archive source to duplicates, keep non-archive sources
                    let mut remaining = Vec::new();
                    for pe in record.source_paths.drain(..) {
                        let (pe_loc, _, _) = decompose_path(&pe.path);
                        if pe_loc == "archive" {
                            record.duplicate_sources.push(pe.path.clone());
                        } else {
                            remaining.push(pe);
                        }
                    }
                    record.source_paths = remaining;
                    record.state = State::HasError;
                    record.output_filename = None;
                    mark_modified(record_id, modified_ids);
                    return;
                }

                // Preserve pre-set context (e.g., forced from sorted/ directory)
                let record = &mut records[idx];
                if record.context.is_none() {
                    record.context = sidecar
                        .get("context")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
                record.metadata = sidecar.get("metadata").cloned();
                record.assigned_filename = sidecar
                    .get("assigned_filename")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                record.hash = change.hash.clone();
                record
                    .current_paths
                    .push(PathEntry { path: change.path.clone(), timestamp: now });
                record.output_filename = None;
            }
            mark_modified(record_id, modified_ids);
        }
        return;
    }

    // Non-.output locations
    let config = match LOCATION_CONFIG.get(location.as_str()) {
        Some(c) => *c,
        None => return,
    };

    let (match_fields, allows_new) = config;
    let change_hash = change.hash.as_deref().unwrap_or("");

    // Try to match against existing records
    let mut matched_id: Option<Uuid> = None;
    let mut matched_field: Option<&str> = None;

    for field in match_fields {
        if change_hash.is_empty() {
            continue;
        }
        match *field {
            "source_hash" => {
                // Search in records, then existing_new, then created
                if let Some(r) = find_by_source_hash(records, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_source_hash(existing_new, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_source_hash(created, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
            }
            "hash" => {
                if let Some(r) = find_by_hash(records, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_hash(existing_new, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_hash(created, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
            }
            _ => {}
        }
    }

    if let (Some(id), Some(field)) = (matched_id, matched_field) {
        // Find the record in one of the three lists and update it
        let record = records
            .iter_mut()
            .find(|r| r.id == id)
            .or_else(|| existing_new.iter_mut().find(|r| r.id == id))
            .or_else(|| created.iter_mut().find(|r| r.id == id));

        if let Some(record) = record {
            let path_list = if field == "hash" {
                &mut record.current_paths
            } else {
                &mut record.source_paths
            };

            let already_tracked = path_list.iter().any(|pe| pe.path == change.path);
            if !already_tracked {
                path_list.push(PathEntry { path: change.path.clone(), timestamp: now });
                mark_modified(record.id, modified_ids);
            }
        }
    } else if allows_new {
        let mut new_record = Record::new(filename.clone(), change_hash.to_string());
        new_record.source_paths.push(PathEntry {
            path: change.path.clone(),
            timestamp: now,
        });

        // Pre-set context from directory for sorted/ files
        if location == "sorted" {
            let (_, loc_path, _) = decompose_path(&change.path);
            if !loc_path.is_empty() {
                new_record.context = loc_path.split('/').next().map(|s| s.to_string());
            }
        }

        created.push(new_record);
    }
}

fn handle_removal(change: &ChangeItem, records: &mut [Record], modified_ids: &mut HashSet<Uuid>) {
    for record in records.iter_mut() {
        // Check source_paths
        if let Some(idx) = record
            .source_paths
            .iter()
            .position(|pe| pe.path == change.path)
        {
            let pe = record.source_paths.remove(idx);
            record.missing_source_paths.push(pe);
            mark_modified(record.id, modified_ids);
            return;
        }

        // Check current_paths
        if let Some(idx) = record
            .current_paths
            .iter()
            .position(|pe| pe.path == change.path)
        {
            let pe = record.current_paths.remove(idx);
            record.missing_current_paths.push(pe);
            mark_modified(record.id, modified_ids);
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// reconcile
// ---------------------------------------------------------------------------

/// Reconcile a record's state based on its paths and fields.
///
/// Returns the modified record, or `None` if the record should be deleted.
///
/// # Arguments
///
/// * `record` - The record to reconcile (modified in place).
/// * `context_field_names` - Maps context name to its `field_names` for
///   metadata completeness checks.
/// * `context_folders` - Maps context name to folder field list.
/// * `recompute_filename` - Callback that recomputes `assigned_filename` for a
///   record from its metadata and context config. Used by `reset/`.
pub fn reconcile<'a>(
    record: &'a mut Record,
    context_field_names: Option<&HashMap<String, Vec<String>>>,
    context_folders: Option<&HashMap<String, Vec<String>>>,
    recompute_filename: Option<&dyn Fn(&Record) -> Option<String>>,
) -> Option<&'a mut Record> {
    record.clear_temporary_fields();

    // ------------------------------------------------------------------
    // Phase 1: source_paths
    // ------------------------------------------------------------------
    if !record.source_paths.is_empty() {
        let sf_path = record.source_file().unwrap().path.clone();
        let (s_loc, _, sf_filename) = decompose_path(&sf_path);

        if s_loc == "trash" {
            record.state = State::NeedsDeletion;
            return Some(record);
        }

        if record.state == State::IsNew {
            record.output_filename = Some(Uuid::new_v4().to_string());
            record.state = State::NeedsProcessing;
            record.source_reference = Some(sf_path.clone());
            let now = Utc::now();
            record.source_paths.push(PathEntry {
                path: format!("archive/{}", sf_filename),
                timestamp: now,
            });
            return Some(record);
        }

        // IS_COMPLETE: new source is a duplicate -- never reprocess.
        if record.state == State::IsComplete && s_loc != "archive" && s_loc != "missing" {
            for pe in &record.source_paths {
                let (pe_loc, _, _) = decompose_path(&pe.path);
                if pe_loc != "archive" && pe_loc != "missing" {
                    record.duplicate_sources.push(pe.path.clone());
                }
            }
            record.source_paths.retain(|pe| {
                let (pe_loc, _, _) = decompose_path(&pe.path);
                pe_loc == "archive" || pe_loc == "missing"
            });
            return Some(record);
        }

        if s_loc != "archive" && s_loc != "missing" && record.state != State::HasError {
            record.source_reference = Some(sf_path.clone());
            let now = Utc::now();
            let mut new_paths: Vec<PathEntry> = Vec::new();
            for pe in &record.source_paths {
                let (pe_loc, _, _) = decompose_path(&pe.path);
                if pe_loc == "archive" || pe_loc == "missing" {
                    new_paths.push(pe.clone());
                } else if pe.path != sf_path {
                    record.duplicate_sources.push(pe.path.clone());
                }
            }
            new_paths.push(PathEntry {
                path: format!("archive/{}", sf_filename),
                timestamp: now,
            });
            record.source_paths = new_paths;
        }

        // Recovery: record with new source in processable location -> reprocess.
        let recoverable = record.state == State::IsMissing
            || (record.state == State::NeedsProcessing && record.output_filename.is_none());

        if recoverable && s_loc != "archive" && s_loc != "missing" && s_loc != "error" {
            record.output_filename = Some(Uuid::new_v4().to_string());
            record.state = State::NeedsProcessing;
            // Clear stale processing results
            record.context = None;
            record.metadata = None;
            record.assigned_filename = None;
            record.hash = None;
            record.missing_current_paths.clear();
            if !record.current_paths.is_empty() {
                for pe in &record.current_paths {
                    record.deleted_paths.push(pe.path.clone());
                }
                record.current_paths.clear();
            }
            return Some(record);
        }
    }

    // ------------------------------------------------------------------
    // Phase 2: has_error
    // ------------------------------------------------------------------
    if record.state == State::HasError {
        let s_loc = record.source_location();

        // Orphan record: source already in error/ and no current paths
        if s_loc.as_deref() == Some("error") && record.current_paths.is_empty() {
            return None;
        }

        // No source paths at all (file was deleted externally) -- clean up
        if s_loc.is_none() && record.current_paths.is_empty() {
            return None;
        }

        // Recovery: new source in a processable location -> retry
        if s_loc.as_deref() == Some("incoming") || s_loc.as_deref() == Some("sorted") {
            record.output_filename = Some(Uuid::new_v4().to_string());
            record.state = State::NeedsProcessing;
            let sf_path = record.source_file().unwrap().path.clone();
            let (_, _, sf_fn) = decompose_path(&sf_path);
            record.source_reference = Some(sf_path.clone());
            let now = Utc::now();
            // Clean up stale source_paths (old archive entries etc.)
            let mut new_paths: Vec<PathEntry> = record
                .source_paths
                .iter()
                .filter(|pe| pe.path == sf_path)
                .cloned()
                .collect();
            new_paths.push(PathEntry {
                path: format!("archive/{}", sf_fn),
                timestamp: now,
            });
            record.source_paths = new_paths;
            // Clear stale processing state
            record.context = None;
            record.metadata = None;
            record.assigned_filename = None;
            record.hash = None;
            if !record.current_paths.is_empty() {
                for pe in &record.current_paths {
                    record.deleted_paths.push(pe.path.clone());
                }
                record.current_paths.clear();
            }
            return Some(record);
        }

        if s_loc.as_deref() == Some("archive") {
            let sf_path = record.source_file().unwrap().path.clone();
            record.source_reference = Some(sf_path);
            record.duplicate_sources.clear();
        }

        if !record.current_paths.is_empty() {
            for pe in &record.current_paths {
                record.deleted_paths.push(pe.path.clone());
            }
            record.current_paths.clear();
        }

        return Some(record);
    }

    // ------------------------------------------------------------------
    // Phase 3: current_paths
    // ------------------------------------------------------------------

    // Trash detection via current_paths (processed file in trash/)
    if record
        .current_paths
        .iter()
        .any(|pe| decompose_path(&pe.path).0 == "trash")
    {
        record.state = State::NeedsDeletion;
        return Some(record);
    }

    // Early returns for special states
    if record.state == State::IsNew {
        record.output_filename = Some(Uuid::new_v4().to_string());
        record.state = State::NeedsProcessing;
        return Some(record);
    }

    if record.state == State::IsDeleted {
        return None;
    }

    if record.state == State::NeedsProcessing && record.current_paths.is_empty() {
        return Some(record);
    }

    // Missing detection
    if record.current_paths.is_empty() {
        if !record.missing_current_paths.is_empty() {
            record.state = State::IsMissing;
            // Move source from archive to missing/
            if !record.source_paths.is_empty() {
                let sf_path = record.source_file().unwrap().path.clone();
                let (sf_loc, _, _) = decompose_path(&sf_path);
                if sf_loc == "archive" {
                    record.source_reference = Some(sf_path);
                }
            }
        }
        return Some(record);
    }

    // Reappearance
    if record.state == State::IsMissing {
        record.state = State::IsComplete;
        // If source was moved to missing/, restore it to archive/
        if let Some(sf) = record.source_file() {
            let (sf_loc, _, _) = decompose_path(&sf.path);
            if sf_loc == "missing" {
                record.source_reference = Some(sf.path.clone());
            }
        }
    }

    // Invalid location cleanup
    let mut valid: Vec<PathEntry> = Vec::new();
    for pe in &record.current_paths {
        let (loc, _, _) = decompose_path(&pe.path);
        if VALID_CURRENT_LOCATIONS.contains(&loc.as_str()) {
            valid.push(pe.clone());
        } else {
            record.deleted_paths.push(pe.path.clone());
        }
    }
    record.current_paths = valid;

    if record.current_paths.is_empty() {
        return Some(record);
    }

    // Deduplicate: keep most recent
    if record.current_paths.len() > 1 {
        let mut sorted_paths = record.current_paths.clone();
        sorted_paths.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        for pe in &sorted_paths[..sorted_paths.len() - 1] {
            record.deleted_paths.push(pe.path.clone());
        }
        record.current_paths = vec![sorted_paths.last().unwrap().clone()];
    }

    // Single current path handling
    let current = record.current_paths[0].clone();
    let (c_loc, _, _) = decompose_path(&current.path);

    match c_loc.as_str() {
        ".output" => {
            if record.assigned_filename.is_some() {
                // If source was in sorted/, return to sorted/ instead of processed/
                let came_from_sorted = record
                    .source_paths
                    .iter()
                    .chain(record.missing_source_paths.iter())
                    .any(|pe| decompose_path(&pe.path).0 == "sorted");

                if came_from_sorted {
                    let target = compute_target_path(record, context_folders);
                    record.target_path = Some(target.unwrap_or_else(|| {
                        format!(
                            "processed/{}",
                            record.assigned_filename.as_deref().unwrap_or("")
                        )
                    }));
                } else {
                    record.target_path = Some(format!(
                        "processed/{}",
                        record.assigned_filename.as_deref().unwrap_or("")
                    ));
                }
                record.current_reference = Some(current.path.clone());
            }
        }

        "processed" => {
            record.state = State::IsComplete;
        }

        "reviewed" => {
            let target = compute_target_path(record, context_folders);
            if let Some(t) = target {
                record.target_path = Some(t);
                record.current_reference = Some(current.path.clone());
            }
        }

        "reset" => {
            if let Some(recompute) = recompute_filename {
                if let Some(new_name) = recompute(record) {
                    record.assigned_filename = Some(new_name);
                }
            }
            let target = compute_target_path(record, context_folders);
            if let Some(t) = target {
                record.target_path = Some(t);
                record.current_reference = Some(current.path.clone());
            }
            record.state = State::IsComplete;
        }

        "sorted" => {
            // Adopt user changes: if user renamed or moved the file, update record
            let current_filename = current.path.rsplit('/').next().unwrap_or("");
            if let Some(ref assigned) = record.assigned_filename {
                if current_filename != assigned {
                    let expected =
                        compute_target_path(record, context_folders).unwrap_or_default();
                    if !is_collision_variant(&current.path, &expected) {
                        record.assigned_filename = Some(current_filename.to_string());
                    }
                }
            }

            let (_, loc_path, _) = decompose_path(&current.path);
            if !loc_path.is_empty() {
                let dir_context = loc_path.split('/').next().unwrap_or("");
                if !dir_context.is_empty()
                    && record.context.as_deref() != Some(dir_context)
                {
                    record.context = Some(dir_context.to_string());
                }
            }

            // Metadata completeness check
            if let (Some(cfn), Some(ref ctx)) = (context_field_names, &record.context) {
                if let Some(field_names) = cfn.get(ctx.as_str()) {
                    let metadata = record.metadata.get_or_insert_with(|| {
                        serde_json::Value::Object(serde_json::Map::new())
                    });
                    if let Some(obj) = metadata.as_object_mut() {
                        for fn_name in field_names {
                            if !obj.contains_key(fn_name) {
                                obj.insert(fn_name.clone(), serde_json::Value::Null);
                            }
                        }
                    }
                }
            }

            record.state = State::IsComplete;
        }

        _ => {}
    }

    Some(record)
}
