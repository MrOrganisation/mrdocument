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

/// Folder field candidate info: `context → field → (candidates, allow_new)`.
pub type FolderFieldCandidates = HashMap<String, HashMap<String, (Vec<String>, bool)>>;

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

#[allow(dead_code)]
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

#[allow(dead_code)]
fn find_by_hash_mut<'a>(records: &'a mut [Record], hash_value: &str) -> Option<&'a mut Record> {
    records
        .iter_mut()
        .find(|r| r.hash.as_deref() == Some(hash_value))
}

fn find_by_source_content_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records
        .iter()
        .find(|r| r.source_content_hash.as_deref() == Some(hash_value))
}

fn find_by_content_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records
        .iter()
        .find(|r| r.content_hash.as_deref() == Some(hash_value))
}

#[allow(dead_code)]
fn find_by_output_filename_mut<'a>(
    records: &'a mut [Record],
    filename: &str,
) -> Option<&'a mut Record> {
    records
        .iter_mut()
        .find(|r| r.output_filename.as_deref() == Some(filename))
}

/// Check whether a file with the given hashes would violate the uniqueness
/// invariant: every non-NULL `source_content_hash` / `content_hash` value
/// must be unique across both columns and all records.  Regular hashes
/// (`source_hash`, `hash`) are also checked for exact-file dedup.
fn is_duplicate_hash(
    records: &[Record],
    hash_value: &str,
    content_hash: Option<&str>,
    exclude_id: Uuid,
) -> bool {
    if hash_value.is_empty() {
        return false;
    }
    for r in records {
        if r.id == exclude_id {
            continue;
        }
        // Check regular hashes
        if r.source_hash == hash_value {
            return true;
        }
        if r.hash.as_deref() == Some(hash_value) {
            return true;
        }
        // Check content hashes (unique across both columns)
        if let Some(ch) = content_hash {
            if !ch.is_empty() {
                if r.source_content_hash.as_deref() == Some(ch) {
                    return true;
                }
                if r.content_hash.as_deref() == Some(ch) {
                    return true;
                }
            }
        }
    }
    false
}

/// Regex matching collision suffixes: `_<8 hex chars>` before the file extension.
static COLLISION_SUFFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"_[0-9a-f]{8}(\.[^.]+)$").unwrap());

/// Check if `actual_path` is `expected_path` or a collision-suffixed variant.
pub(crate) fn is_collision_variant(actual_path: &str, expected_path: &str) -> bool {
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

/// Merge service metadata into existing, keeping non-null pre-set values.
///
/// Fields already present with non-null values in `existing` are preserved.
/// New fields from `incoming` are added.
pub fn merge_metadata(
    existing: Option<&serde_json::Value>,
    incoming: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    match (existing, incoming) {
        (Some(existing), Some(incoming)) => {
            let mut merged = incoming;
            if let (Some(e_obj), Some(m_obj)) =
                (existing.as_object(), merged.as_object_mut())
            {
                for (k, v) in e_obj {
                    if !v.is_null() {
                        m_obj.insert(k.clone(), v.clone());
                    }
                }
            }
            Some(merged)
        }
        (None, incoming) => incoming,
        (existing, None) => existing.cloned(),
    }
}

/// Add record index to modified list if not already there.
fn mark_modified(record_id: Uuid, modified_ids: &mut HashSet<Uuid>) {
    modified_ids.insert(record_id);
}

// ---------------------------------------------------------------------------
// compute_target_path
// ---------------------------------------------------------------------------

/// Compute locked_fields from a record's source paths and context_folders config.
///
/// Looks for a `sorted/` source path and extracts subfolder field values
/// beyond the context (first) component.  Returns a JSON object in the
/// format expected by the service: `{ "field": { "value": "..." }, ... }`.
pub fn compute_locked_fields(
    record: &Record,
    context_folders: Option<&HashMap<String, Vec<String>>>,
) -> Option<serde_json::Value> {
    let folders_map = context_folders?;
    let context = record.context.as_ref()?;
    let folders = folders_map.get(context)?;

    // Find a sorted/ source path (check source_paths then missing_source_paths)
    let sorted_path = record
        .source_paths
        .iter()
        .chain(record.missing_source_paths.iter())
        .find(|pe| decompose_path(&pe.path).0 == "sorted")?;

    let (_, loc_path, _) = decompose_path(&sorted_path.path);
    if loc_path.is_empty() {
        return None;
    }
    let parts: Vec<&str> = loc_path.split('/').collect();

    let mut locked = serde_json::Map::new();
    for (i, field) in folders.iter().enumerate() {
        if field == "context" {
            continue;
        }
        if let Some(value) = parts.get(i) {
            if !value.is_empty() {
                locked.insert(
                    field.clone(),
                    serde_json::json!({"value": *value}),
                );
            }
        }
    }

    if locked.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(locked))
    }
}

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
                    // Always use record.context for "context" field (authoritative)
                    if field == "context" {
                        parts.push(context.clone());
                        continue;
                    }
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
/// Returns `(modified_records, new_records, rejected_paths)`.
/// Rejected paths are `(source_rel_path, error_dest_rel_path)` pairs for
/// files in `sorted/` with an invalid context or invalid folder candidate.
pub fn preprocess<F>(
    changes: &[ChangeItem],
    records: &mut Vec<Record>,
    new_records: &mut Vec<Record>,
    read_sidecar: F,
    context_folders: Option<&HashMap<String, Vec<String>>>,
    folder_field_candidates: Option<&FolderFieldCandidates>,
) -> (Vec<Uuid>, Vec<Record>, Vec<(String, String)>)
where
    F: Fn(&str) -> serde_json::Value,
{
    let mut modified_ids: HashSet<Uuid> = HashSet::new();
    let mut created: Vec<Record> = Vec::new();
    let mut rejected: Vec<(String, String)> = Vec::new();
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
                    context_folders,
                    folder_field_candidates,
                    &mut rejected,
                );
            }
            EventType::Removal => {
                handle_removal(change, records, &mut modified_ids);
            }
        }
    }

    (modified_ids.into_iter().collect(), created, rejected)
}

fn handle_addition<F>(
    change: &ChangeItem,
    records: &mut Vec<Record>,
    existing_new: &mut Vec<Record>,
    created: &mut Vec<Record>,
    read_sidecar: &F,
    modified_ids: &mut HashSet<Uuid>,
    now: DateTime<Utc>,
    context_folders: Option<&HashMap<String, Vec<String>>>,
    folder_field_candidates: Option<&FolderFieldCandidates>,
    rejected: &mut Vec<(String, String)>,
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
                let change_content_hash = change.content_hash.as_deref();

                if is_duplicate_hash(records, change_hash, change_content_hash, record_id) {
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
                record.metadata = merge_metadata(
                    record.metadata.as_ref(),
                    sidecar.get("metadata").cloned(),
                );
                record.assigned_filename = sidecar
                    .get("assigned_filename")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                record.hash = change.hash.clone();
                record.content_hash = change.content_hash.clone();
                record
                    .current_paths
                    .push(PathEntry { path: change.path.clone(), timestamp: now });
                record.output_filename = None;
            }
            mark_modified(record_id, modified_ids);
        }
        return;
    }

    // reclassify/ handling: reset an existing record for reprocessing
    if location == "reclassify" {
        let change_hash = change.hash.as_deref().unwrap_or("");
        if change_hash.is_empty() {
            rejected.push((change.path.clone(), format!("error/{}", filename)));
            return;
        }

        // Match against existing records: source_hash first, then hash
        #[derive(PartialEq)]
        enum ReclassifyMatch {
            SourceHash,
            Hash,
        }
        let mut matched: Option<(usize, ReclassifyMatch)> = None;

        for (i, r) in records.iter().enumerate() {
            if r.state == State::NeedsDeletion || r.state == State::IsDeleted {
                continue;
            }
            if r.source_hash == change_hash {
                matched = Some((i, ReclassifyMatch::SourceHash));
                break;
            }
        }
        if matched.is_none() {
            for (i, r) in records.iter().enumerate() {
                if r.state == State::NeedsDeletion || r.state == State::IsDeleted {
                    continue;
                }
                if r.hash.as_deref() == Some(change_hash) {
                    matched = Some((i, ReclassifyMatch::Hash));
                    break;
                }
            }
        }

        let (idx, match_type) = match matched {
            Some(m) => m,
            None => {
                rejected.push((change.path.clone(), format!("error/{}", filename)));
                return;
            }
        };

        let record = &mut records[idx];
        let record_id = record.id;

        // Check if a live source file exists (any source_paths entry not in reclassify/)
        let live_source = record
            .source_paths
            .iter()
            .find(|pe| decompose_path(&pe.path).0 != "reclassify")
            .cloned();

        if live_source.is_some() {
            // Case 1: Original source exists — reclassify file is just a trigger
            record.deleted_paths.push(change.path.clone());
        } else if match_type == ReclassifyMatch::SourceHash {
            // Case 2: No live source, but reclassify file IS the original
            record.source_paths.push(PathEntry {
                path: change.path.clone(),
                timestamp: now,
            });
        } else {
            // Case 3: No live source + hash-only match — can't recover original
            warn!(
                "Reclassify file {} matched record {} by hash but no original source exists",
                change.path, record_id
            );
            rejected.push((change.path.clone(), format!("error/{}", filename)));
            return;
        }

        // Mark all current_paths for deletion
        for pe in record.current_paths.drain(..) {
            record.deleted_paths.push(pe.path);
        }
        record.missing_current_paths.clear();

        // Keep only the chosen source, mark rest for deletion
        let keep_path = if let Some(ref ls) = live_source {
            ls.path.clone()
        } else {
            change.path.clone()
        };
        let mut kept = Vec::new();
        for pe in record.source_paths.drain(..) {
            if pe.path == keep_path {
                kept.push(pe);
            } else {
                record.deleted_paths.push(pe.path.clone());
                record.missing_source_paths.push(pe);
            }
        }
        record.source_paths = kept;

        // Reset record for reprocessing
        record.context = None;
        record.metadata = None;
        record.assigned_filename = None;
        record.hash = None;
        record.content_hash = None;
        record.target_path = None;
        record.source_reference = None;
        record.current_reference = None;

        if let Some(ref ls) = live_source {
            // Case 1: source exists outside reclassify/ — just a trigger.
            // If the source ended up in missing/ (race: sorted file deleted
            // before reclassify was detected), move it back to archive/.
            let (ls_loc, _, _) = decompose_path(&ls.path);
            if ls_loc == "missing" {
                record.source_reference = Some(ls.path.clone());
                record.state = State::IsNew;
                record.output_filename = None;
            } else {
                // Source already in archive — go straight to NeedsProcessing
                // to avoid reconcile's IsNew handler which would set source_reference
                // and try to move the archive file to itself.
                record.output_filename = Some(Uuid::new_v4().to_string());
                record.state = State::NeedsProcessing;
            }
        } else {
            // Case 2: source in reclassify/ — use IsNew so reconcile moves it
            // to archive and assigns output_filename.
            record.output_filename = None;
            record.state = State::IsNew;
        }

        mark_modified(record_id, modified_ids);
        return;
    }

    // Non-.output locations
    let config = match LOCATION_CONFIG.get(location.as_str()) {
        Some(c) => *c,
        None => return,
    };

    let (match_fields, allows_new) = config;
    let change_hash = change.hash.as_deref().unwrap_or("");
    let change_content_hash = change.content_hash.as_deref();

    // Try to match against existing records
    let mut matched_id: Option<Uuid> = None;
    let mut matched_field: Option<&str> = None;

    for field in match_fields {
        if change_hash.is_empty() {
            continue;
        }
        match *field {
            "source_hash" => {
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
                // Also match by content hash
                if let Some(ch) = change_content_hash {
                    if !ch.is_empty() {
                        if let Some(r) = find_by_source_content_hash(records, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_source_content_hash(existing_new, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_source_content_hash(created, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                    }
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
                // Also match by content hash
                if let Some(ch) = change_content_hash {
                    if !ch.is_empty() {
                        if let Some(r) = find_by_content_hash(records, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_content_hash(existing_new, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_content_hash(created, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                    }
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
        new_record.source_content_hash = change.content_hash.clone();
        new_record.source_paths.push(PathEntry {
            path: change.path.clone(),
            timestamp: now,
        });

        // Pre-set context and metadata fields from directory for sorted/ files
        if location == "sorted" {
            let (_, loc_path, _) = decompose_path(&change.path);
            if !loc_path.is_empty() {
                let parts: Vec<&str> = loc_path.split('/').collect();

                // Reject files with an invalid (unconfigured) context
                if let Some(folders_map) = context_folders {
                    if !folders_map.contains_key(parts[0]) {
                        // Build error destination: error/{subpath after context}/{filename}
                        let subpath = if parts.len() > 1 {
                            parts[1..].join("/")
                        } else {
                            String::new()
                        };
                        let (_, _, fname) = decompose_path(&change.path);
                        let error_dest = if subpath.is_empty() {
                            format!("error/{}", fname)
                        } else {
                            format!("error/{}/{}", subpath, fname)
                        };
                        warn!(
                            "Invalid context '{}' in sorted path {}, rejecting to {}",
                            parts[0], change.path, error_dest
                        );
                        rejected.push((change.path.clone(), error_dest));
                        return;
                    }
                }

                new_record.context = Some(parts[0].to_string());

                // Validate subfolder values against field candidates
                if let Some(fc_map) = folder_field_candidates {
                    if let Some(field_map) = fc_map.get(parts[0]) {
                        if let Some(folders_map) = context_folders {
                            if let Some(folders) = folders_map.get(parts[0]) {
                                for (i, field) in folders.iter().enumerate() {
                                    if field == "context" {
                                        continue;
                                    }
                                    if let Some(value) = parts.get(i) {
                                        if value.is_empty() {
                                            continue;
                                        }
                                        if let Some((candidates, allow_new)) =
                                            field_map.get(field)
                                        {
                                            let is_known =
                                                candidates.iter().any(|c| c == value);
                                            if !is_known && !allow_new {
                                                // Reject: error/{loc_path}/{filename}
                                                let (_, _, fname) =
                                                    decompose_path(&change.path);
                                                let error_dest =
                                                    format!("error/{}/{}", loc_path, fname);
                                                warn!(
                                                    "Invalid candidate '{}' for field '{}' in context '{}', rejecting {} to {}",
                                                    value, field, parts[0], change.path, error_dest
                                                );
                                                rejected
                                                    .push((change.path.clone(), error_dest));
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Extract additional subfolder fields into metadata
                if let Some(folders_map) = context_folders {
                    if let Some(folders) = folders_map.get(parts[0]) {
                        let mut meta = serde_json::Map::new();
                        for (i, field) in folders.iter().enumerate() {
                            if field == "context" {
                                continue;
                            }
                            if let Some(value) = parts.get(i) {
                                if !value.is_empty() {
                                    meta.insert(
                                        field.clone(),
                                        serde_json::Value::String(value.to_string()),
                                    );
                                }
                            }
                        }
                        if !meta.is_empty() {
                            new_record.metadata =
                                Some(serde_json::Value::Object(meta));
                        }
                    }
                }
            }
        }

        // Set date_added to today
        new_record.date_added = Some(now.date_naive());

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
            // If a new source just arrived (Phase 1 set source_reference),
            // reset for reprocessing instead of staying IsMissing.
            if record.source_reference.is_some() && record.state == State::IsMissing {
                record.missing_current_paths.clear();
                // Remove old source entries in missing/ — they're superseded
                let mut kept = Vec::new();
                for pe in record.source_paths.drain(..) {
                    let (loc, _, _) = decompose_path(&pe.path);
                    if loc == "missing" {
                        record.deleted_paths.push(pe.path.clone());
                        record.missing_source_paths.push(pe);
                    } else {
                        kept.push(pe);
                    }
                }
                record.source_paths = kept;
                record.state = State::IsNew;
                record.context = None;
                record.metadata = None;
                record.assigned_filename = None;
                record.hash = None;
                record.content_hash = None;
                record.target_path = None;
                record.current_reference = None;
                return Some(record);
            }

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

    // Ensure source is in archive/ for any complete record.
    // After a reclassify race (sorted deleted → source moved to missing/ →
    // reclassify reprocesses → IsComplete with source still in missing/),
    // the source_reference mechanism moves it back.
    if record.state == State::IsComplete && record.source_reference.is_none() {
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
            let (_, loc_path, _) = decompose_path(&current.path);
            let dir_context = if !loc_path.is_empty() {
                let parts: Vec<&str> = loc_path.split('/').collect();
                if !parts[0].is_empty() {
                    Some(parts[0].to_string())
                } else {
                    None
                }
            } else {
                None
            };

            // Context change → reclassify-style reset with locked fields
            let context_changed = dir_context.is_some()
                && record.context.as_deref() != dir_context.as_deref();

            if context_changed {
                let new_ctx = dir_context.as_deref().unwrap();
                let parts: Vec<&str> = loc_path.split('/').collect();

                // Reset for reprocessing (reclassify-style)
                record.context = Some(new_ctx.to_string());
                record.assigned_filename = None;
                record.hash = None;
                record.content_hash = None;
                record.target_path = None;
                record.source_reference = None;
                record.current_reference = None;

                // Pre-set metadata from subfolder fields (locked)
                record.metadata = None;
                if let Some(folders_map) = context_folders {
                    if let Some(folders) = folders_map.get(new_ctx) {
                        let mut meta = serde_json::Map::new();
                        for (i, field) in folders.iter().enumerate() {
                            if field == "context" {
                                continue;
                            }
                            if let Some(value) = parts.get(i) {
                                if !value.is_empty() {
                                    meta.insert(
                                        field.clone(),
                                        serde_json::Value::String(value.to_string()),
                                    );
                                }
                            }
                        }
                        if !meta.is_empty() {
                            record.metadata =
                                Some(serde_json::Value::Object(meta));
                        }
                    }
                }

                // Void the current sorted file — reprocess from archive source.
                // Keep a record in missing_source_paths so the .output handler
                // knows this record came from sorted/ and routes back there.
                for pe in record.current_paths.drain(..) {
                    record.deleted_paths.push(pe.path.clone());
                    record.missing_source_paths.push(pe);
                }
                record.missing_current_paths.clear();

                // Go straight to NeedsProcessing (archive source stays)
                record.output_filename = Some(Uuid::new_v4().to_string());
                record.state = State::NeedsProcessing;
            } else {
                // No context change — adopt user renames as before
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

                if let Some(ref ctx) = dir_context {
                    if let Some(folders_map) = context_folders {
                        let parts: Vec<&str> = loc_path.split('/').collect();
                        if let Some(folders) = folders_map.get(ctx.as_str()) {
                            // Check if the file has all folder levels filled.
                            // If a level is missing (file moved "up"), put it
                            // back where it belongs based on existing metadata.
                            let expected_depth = folders.len();
                            let actual_depth = parts.len();

                            if actual_depth < expected_depth {
                                // File is at a shallower level — move it back
                                // to the correct location derived from metadata.
                                let target = compute_target_path(record, context_folders);
                                if let Some(t) = target {
                                    if t != current.path {
                                        record.target_path = Some(t);
                                        record.current_reference =
                                            Some(current.path.clone());
                                    }
                                }
                            } else {
                                // File has all folder levels — update metadata
                                // from the folder position (sideways move) and
                                // recompute the filename if metadata changed.
                                let metadata = record.metadata.get_or_insert_with(|| {
                                    serde_json::Value::Object(serde_json::Map::new())
                                });
                                let mut metadata_changed = false;
                                if let Some(obj) = metadata.as_object_mut() {
                                    for (i, field) in folders.iter().enumerate() {
                                        if field == "context" {
                                            continue;
                                        }
                                        if let Some(value) = parts.get(i) {
                                            if !value.is_empty() {
                                                let new_val = serde_json::Value::String(
                                                    value.to_string(),
                                                );
                                                if obj.get(field) != Some(&new_val) {
                                                    obj.insert(field.clone(), new_val);
                                                    metadata_changed = true;
                                                }
                                            }
                                        }
                                    }
                                }

                                if metadata_changed {
                                    if let Some(recompute) = recompute_filename {
                                        if let Some(new_name) = recompute(record) {
                                            record.assigned_filename = Some(new_name);
                                        }
                                    }
                                    let target =
                                        compute_target_path(record, context_folders);
                                    if let Some(t) = target {
                                        if t != current.path {
                                            record.target_path = Some(t);
                                            record.current_reference =
                                                Some(current.path.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Metadata completeness check
                if let (Some(cfn), Some(ref ctx)) =
                    (context_field_names, &record.context)
                {
                    if let Some(field_names) = cfn.get(ctx.as_str()) {
                        let metadata = record.metadata.get_or_insert_with(|| {
                            serde_json::Value::Object(serde_json::Map::new())
                        });
                        if let Some(obj) = metadata.as_object_mut() {
                            for fn_name in field_names {
                                if !obj.contains_key(fn_name) {
                                    obj.insert(
                                        fn_name.clone(),
                                        serde_json::Value::Null,
                                    );
                                }
                            }
                        }
                    }
                }

                record.state = State::IsComplete;
            }
        }

        _ => {}
    }

    Some(record)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ChangeItem, EventType, PathEntry, Record, State};
    use chrono::{DateTime, Duration, Utc};
    use serde_json::json;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn _ts(offset_hours: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + Duration::hours(offset_hours)
    }

    /// Create a Record with sensible defaults.  Caller can override fields
    /// after construction.
    fn _make_record() -> Record {
        Record::new("test.pdf".to_string(), "abc123".to_string())
    }

    fn _make_change(
        event_type: EventType,
        path: &str,
        hash: Option<&str>,
        size: Option<u64>,
    ) -> ChangeItem {
        ChangeItem {
            event_type,
            path: path.to_string(),
            hash: hash.map(|s| s.to_string()),
            content_hash: None,
            size,
        }
    }

    fn _noop_sidecar(_path: &str) -> serde_json::Value {
        json!({})
    }

    // -----------------------------------------------------------------------
    // TestPreprocessOutput (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_output_matches_sidecar_ingested() {
        let mut records = vec![{
            let mut r = _make_record();
            r.output_filename = Some("uuid-123".to_string());
            r.state = State::NeedsProcessing;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-123",
            Some("content_hash"),
            Some(1024),
        );
        let sidecar_data = json!({
            "context": "work",
            "metadata": {"date": "2025-01-01", "title": "Invoice"},
            "assigned_filename": "2025-01-Invoice.pdf",
        });

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                sidecar_data.clone()
            }, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&records[0].id));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(
            records[0].metadata,
            Some(json!({"date": "2025-01-01", "title": "Invoice"}))
        );
        assert_eq!(
            records[0].assigned_filename.as_deref(),
            Some("2025-01-Invoice.pdf")
        );
        assert_eq!(records[0].hash.as_deref(), Some("content_hash"));
        assert!(records[0].output_filename.is_none());
        assert_eq!(records[0].current_paths.len(), 1);
        assert_eq!(records[0].current_paths[0].path, ".output/uuid-123");
        assert!(created.is_empty());
    }

    #[test]
    fn test_output_zero_byte_sets_error() {
        let mut records = vec![{
            let mut r = _make_record();
            r.output_filename = Some("uuid-456".to_string());
            r.state = State::NeedsProcessing;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-456",
            Some("empty_hash"),
            Some(0),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert_eq!(records[0].state, State::HasError);
        assert!(records[0].output_filename.is_none());
        assert!(created.is_empty());
    }

    #[test]
    fn test_output_no_match_ignored() {
        let mut records = vec![{
            let mut r = _make_record();
            r.output_filename = Some("other-uuid".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/unknown-uuid",
            Some("some_hash"),
            Some(100),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    // -----------------------------------------------------------------------
    // TestPreprocessIncoming (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_incoming_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "file_hash_abc".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/doc.pdf",
            Some("file_hash_abc"),
            Some(500),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&records[0].id));
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.pdf"));
        assert!(created.is_empty());
    }

    #[test]
    fn test_incoming_unknown_creates_new() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "existing_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/new.pdf",
            Some("new_hash"),
            Some(1024),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert!(modified_ids.is_empty());
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].original_filename, "new.pdf");
        assert_eq!(created[0].source_hash, "new_hash");
        assert_eq!(created[0].source_paths[0].path, "incoming/new.pdf");
    }

    #[test]
    fn test_new_record_has_date_added_set() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/new_with_date.pdf",
            Some("date_hash"),
            Some(1024),
        );

        let (_modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(created.len(), 1);
        assert!(
            created[0].date_added.is_some(),
            "New records should have date_added set"
        );
        // date_added should be today's date
        let today = chrono::Utc::now().date_naive();
        assert_eq!(created[0].date_added.unwrap(), today);
    }

    #[test]
    fn test_two_identical_sources_same_cycle() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change_a = _make_change(
            EventType::Addition,
            "incoming/a.pdf",
            Some("same_hash"),
            Some(500),
        );
        let change_b = _make_change(
            EventType::Addition,
            "incoming/b.pdf",
            Some("same_hash"),
            Some(500),
        );

        let (_modified_ids, created, _rejected) = preprocess(
            &[change_a, change_b],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert_eq!(
            created.len(),
            1,
            "Expected 1 record for identical sources, got {}",
            created.len()
        );
        let paths: Vec<&str> = created[0].source_paths.iter().map(|pe| pe.path.as_str()).collect();
        assert!(paths.contains(&"incoming/a.pdf"));
        assert!(paths.contains(&"incoming/b.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessSorted (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_matches_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("sorted_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/doc.pdf",
            Some("sorted_hash"),
            Some(800),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
    }

    #[test]
    fn test_sorted_matches_source_hash_not_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "src_hash".to_string();
            r.hash = Some("different_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/original.pdf",
            Some("src_hash"),
            Some(600),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/original.pdf"));
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/original.pdf"));
    }

    #[test]
    fn test_sorted_unknown_creates_new() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/personal/unknown.pdf",
            Some("unknown_hash"),
            Some(300),
        );

        let (_modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].original_filename, "unknown.pdf");
        assert_eq!(created[0].context.as_deref(), Some("personal"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessOtherLocations (7 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_archive_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "arch_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/doc.pdf",
            Some("arch_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "archive/doc.pdf"));
    }

    #[test]
    fn test_missing_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "miss_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "missing/doc.pdf",
            Some("miss_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "missing/doc.pdf"));
    }

    #[test]
    fn test_missing_both_match_uses_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "both_hash".to_string();
            r.hash = Some("both_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "missing/doc.pdf",
            Some("both_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "missing/doc.pdf"));
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "missing/doc.pdf"));
    }

    #[test]
    fn test_missing_unknown_not_created() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "missing/stray.pdf",
            Some("stray_hash"),
            Some(500),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    #[test]
    fn test_archive_unknown_not_created() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/stray.pdf",
            Some("stray_hash"),
            Some(500),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    #[test]
    fn test_processed_matches_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("proc_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "processed/doc.pdf",
            Some("proc_hash"),
            Some(700),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "processed/doc.pdf"));
    }

    #[test]
    fn test_trash_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "trash_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "trash/doc.pdf",
            Some("trash_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "trash/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessRemovals (2 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_removal_from_source_paths() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_paths.push(PathEntry {
                path: "incoming/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(EventType::Removal, "incoming/doc.pdf", None, None);

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(!records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.pdf"));
        assert!(records[0]
            .missing_source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.pdf"));
    }

    #[test]
    fn test_removal_from_current_paths() {
        let mut records = vec![{
            let mut r = _make_record();
            r.current_paths.push(PathEntry {
                path: "sorted/work/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(EventType::Removal, "sorted/work/doc.pdf", None, None);

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
        assert!(records[0]
            .missing_current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessLocationAware (2 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_archive_both_match_uses_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "both_hash".to_string();
            r.hash = Some("both_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/doc.pdf",
            Some("both_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "archive/doc.pdf"));
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "archive/doc.pdf"));
    }

    #[test]
    fn test_sorted_both_match_prefers_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "both_hash".to_string();
            r.hash = Some("both_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/doc.pdf",
            Some("both_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
        assert!(!records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessIdempotency (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_duplicate_source_path_not_appended() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "abc123".to_string();
            r.state = State::IsComplete;
            r.source_paths.push(PathEntry {
                path: "archive/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/doc.pdf",
            Some("abc123"),
            Some(100),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(
            modified_ids.len(),
            0,
            "Record should not be marked modified"
        );
        assert_eq!(
            records[0].source_paths.len(),
            1,
            "No duplicate path should be added"
        );
    }

    #[test]
    fn test_duplicate_current_path_not_appended() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("def456".to_string());
            r.state = State::IsComplete;
            r.current_paths.push(PathEntry {
                path: "sorted/work/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/doc.pdf",
            Some("def456"),
            Some(100),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(
            modified_ids.len(),
            0,
            "Record should not be marked modified"
        );
        assert_eq!(
            records[0].current_paths.len(),
            1,
            "No duplicate path should be added"
        );
    }

    #[test]
    fn test_new_path_still_appended() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "abc123".to_string();
            r.state = State::IsComplete;
            r.source_paths.push(PathEntry {
                path: "archive/old.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/new-copy.pdf",
            Some("abc123"),
            Some(100),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/new-copy.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestReconcileSourcePaths (9 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_source_in_trash() {
        let mut record = _make_record();
        record.source_paths.push(PathEntry {
            path: "trash/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsDeletion);
    }

    #[test]
    fn test_is_new_sets_processing() {
        let mut record = _make_record();
        record.state = State::IsNew;
        record.source_paths.push(PathEntry {
            path: "incoming/invoice.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
        assert_eq!(r.source_reference.as_deref(), Some("incoming/invoice.pdf"));
        let archive_entries: Vec<_> = r
            .source_paths
            .iter()
            .filter(|pe| pe.path.starts_with("archive/"))
            .collect();
        assert_eq!(archive_entries.len(), 1);
        assert_eq!(archive_entries[0].path, "archive/invoice.pdf");
    }

    #[test]
    fn test_source_not_archive_not_lost() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.source_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "incoming/copy.pdf".to_string(),
            timestamp: _ts(1),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.source_reference.as_deref(), Some("incoming/copy.pdf"));
        assert!(r.duplicate_sources.contains(&"incoming/doc.pdf".to_string()));
        let archive_entries: Vec<_> = r
            .source_paths
            .iter()
            .filter(|pe| pe.path.starts_with("archive/"))
            .collect();
        assert!(!archive_entries.is_empty());
    }

    #[test]
    fn test_is_complete_new_source_is_duplicate() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.duplicate_sources.contains(&"incoming/doc.pdf".to_string()));
        assert!(r.source_paths.iter().any(|pe| pe.path == "archive/doc.pdf"));
        assert!(!r
            .source_paths
            .iter()
            .any(|pe| pe.path.starts_with("incoming/")));
        assert!(r
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
        assert!(r.output_filename.is_none());
    }

    #[test]
    fn test_is_complete_new_source_in_sorted_is_duplicate() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.current_paths.push(PathEntry {
            path: "processed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r
            .duplicate_sources
            .contains(&"sorted/work/doc.pdf".to_string()));
        assert!(!r.source_paths.iter().any(|pe| {
            let (loc, _, _) = Record::decompose_path(&pe.path);
            loc == "sorted"
        }));
    }

    #[test]
    fn test_is_complete_source_in_archive_no_duplicate() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.duplicate_sources.is_empty());
    }

    #[test]
    fn test_is_missing_recovers_with_new_source() {
        let mut record = _make_record();
        record.state = State::IsMissing;
        record.source_paths.push(PathEntry {
            path: "missing/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
    }

    #[test]
    fn test_is_missing_sets_source_reference() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
        assert_eq!(r.source_reference.as_deref(), Some("archive/doc.pdf"));
    }

    #[test]
    fn test_is_missing_no_source_reference_without_archive() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "missing/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
        assert!(r.source_reference.is_none());
    }

    // -----------------------------------------------------------------------
    // TestReconcileHasError (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_error_source_in_error_no_current_deletes() {
        let mut record = _make_record();
        record.state = State::HasError;
        record.source_paths.push(PathEntry {
            path: "error/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_error_source_in_archive() {
        let mut record = _make_record();
        record.state = State::HasError;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.duplicate_sources.push("incoming/extra.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.source_reference.as_deref(), Some("archive/doc.pdf"));
        assert!(r.duplicate_sources.is_empty());
    }

    #[test]
    fn test_error_with_current_paths() {
        let mut record = _make_record();
        record.state = State::HasError;
        record.source_paths.push(PathEntry {
            path: "error/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: ".output/uuid".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert!(r.current_paths.is_empty());
        assert!(r.deleted_paths.contains(&".output/uuid".to_string()));
    }

    // -----------------------------------------------------------------------
    // TestReconcileCurrentPaths (15 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_in_trash_needs_deletion() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "trash/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.hash = Some("some_hash".to_string());
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsDeletion);
    }

    #[test]
    fn test_is_new_needs_processing() {
        // No source_paths → source section skipped → reach current section
        let mut record = _make_record();
        record.state = State::IsNew;

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
    }

    #[test]
    fn test_is_deleted_removed() {
        let mut record = _make_record();
        record.state = State::IsDeleted;
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_needs_processing_no_current() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.output_filename = Some("some-uuid".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
    }

    #[test]
    fn test_no_current_missing_current_is_missing() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
    }

    #[test]
    fn test_is_missing_current_reappeared() {
        let mut record = _make_record();
        record.state = State::IsMissing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_is_missing_still_empty() {
        let mut record = _make_record();
        record.state = State::IsMissing;
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
    }

    #[test]
    fn test_invalid_location_deleted() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert!(r.current_paths.is_empty());
        assert!(r.deleted_paths.contains(&"incoming/doc.pdf".to_string()));
    }

    #[test]
    fn test_multiple_keep_most_recent() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/old.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/new.pdf".to_string(),
            timestamp: _ts(2),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/mid.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("new.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.current_paths.len(), 1);
        assert_eq!(r.current_paths[0].path, "sorted/work/new.pdf");
        assert!(r.deleted_paths.contains(&"sorted/work/old.pdf".to_string()));
        assert!(r.deleted_paths.contains(&"sorted/work/mid.pdf".to_string()));
    }

    #[test]
    fn test_single_output_target_processed() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.source_paths.push(PathEntry {
            path: "archive/test.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: ".output/uuid-out".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(
            r.target_path.as_deref(),
            Some("processed/2025-01-Invoice.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some(".output/uuid-out"));
    }

    #[test]
    fn test_single_output_from_sorted_target_sorted() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.source_paths.push(PathEntry {
            path: "archive/test.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_source_paths.push(PathEntry {
            path: "sorted/work/test.pdf".to_string(),
            timestamp: _ts(-1),
        });
        record.current_paths.push(PathEntry {
            path: ".output/uuid-out".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/2025-01-Invoice.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some(".output/uuid-out"));
    }

    #[test]
    fn test_single_processed_is_complete() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "processed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_single_reviewed_target_sorted() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reviewed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/2025-01-Doc.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some("reviewed/doc.pdf"));
    }

    #[test]
    fn test_single_sorted_matches_complete() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/2025-01-Invoice.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_single_sorted_doesnt_match() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/wrong_name.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("wrong_name.pdf"));
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    // -----------------------------------------------------------------------
    // TestComputeTargetPath (5 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_target_path_with_context_and_filename() {
        let mut record = _make_record();
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = compute_target_path(&record, None);

        assert_eq!(result.as_deref(), Some("sorted/work/2025-01-Invoice.pdf"));
    }

    #[test]
    fn test_compute_target_path_no_context_returns_none() {
        let mut record = _make_record();
        record.context = None;
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = compute_target_path(&record, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_compute_target_path_no_filename_returns_none() {
        let mut record = _make_record();
        record.context = Some("work".to_string());
        record.assigned_filename = None;

        let result = compute_target_path(&record, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_compute_target_path_with_context_folders() {
        let mut record = _make_record();
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("arbeit-rechnung-2025.pdf".to_string());
        record.metadata = Some(json!({
            "context": "arbeit",
            "sender": "Schulze GmbH",
            "type": "Rechnung",
        }));
        let folders: HashMap<String, Vec<String>> =
            [("arbeit".to_string(), vec!["context".to_string(), "sender".to_string()])]
                .into_iter()
                .collect();

        let result = compute_target_path(&record, Some(&folders));

        assert_eq!(
            result.as_deref(),
            Some("sorted/arbeit/Schulze GmbH/arbeit-rechnung-2025.pdf")
        );
    }

    #[test]
    fn test_compute_target_path_with_context_folders_missing_field() {
        let mut record = _make_record();
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("arbeit-rechnung-2025.pdf".to_string());
        record.metadata = Some(json!({"context": "arbeit"}));
        let folders: HashMap<String, Vec<String>> =
            [("arbeit".to_string(), vec!["context".to_string(), "sender".to_string()])]
                .into_iter()
                .collect();

        let result = compute_target_path(&record, Some(&folders));

        // "sender" is missing, so only "context" folder part is used
        assert_eq!(
            result.as_deref(),
            Some("sorted/arbeit/arbeit-rechnung-2025.pdf")
        );
    }

    // -----------------------------------------------------------------------
    // TestIsCollisionVariant (9 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_collision_exact_match() {
        assert!(is_collision_variant(
            "sorted/work/invoice.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_suffix_matches() {
        assert!(is_collision_variant(
            "sorted/work/invoice_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_different_suffix_still_matches() {
        assert!(is_collision_variant(
            "sorted/work/invoice_abcd1234.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_different_filename_no_match() {
        assert!(!is_collision_variant(
            "sorted/work/receipt_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_different_directory_no_match() {
        assert!(!is_collision_variant(
            "sorted/personal/invoice_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_non_hex_suffix_no_match() {
        assert!(!is_collision_variant(
            "sorted/work/invoice_zzzzzzzz.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_wrong_length_suffix_no_match() {
        assert!(!is_collision_variant(
            "sorted/work/invoice_abcd12.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_deep_path_with_collision() {
        assert!(is_collision_variant(
            "sorted/belege/nain_trading/79901/belege-2025-naturescene_231x173-972_e3ca2c9b.pdf",
            "sorted/belege/nain_trading/79901/belege-2025-naturescene_231x173-972.pdf",
        ));
    }

    #[test]
    fn test_collision_txt_extension() {
        assert!(is_collision_variant(
            "sorted/work/transcript_aabbccdd.txt",
            "sorted/work/transcript.txt",
        ));
    }

    // -----------------------------------------------------------------------
    // TestReconcileSortedCollisionVariant (6 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_collision_variant_is_complete() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/2025-01-Invoice_e3ca2c9b.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
        assert!(r.current_reference.is_none());
    }

    #[test]
    fn test_collision_variant_with_context_folders() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/arbeit/Schulze GmbH/rechnung_abcd1234.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("rechnung.pdf".to_string());
        record.metadata = Some(json!({"context": "arbeit", "sender": "Schulze GmbH"}));
        let folders: HashMap<String, Vec<String>> =
            [("arbeit".to_string(), vec!["context".to_string(), "sender".to_string()])]
                .into_iter()
                .collect();

        let result = reconcile(&mut record, None, Some(&folders), None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_user_rename_adopts_new_filename() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/my-custom-name.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("my-custom-name.pdf"));
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_user_rename_context_change() {
        // Context change in sorted/ triggers reprocessing
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/personal/my-custom-name.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("my-custom-name.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.context.as_deref(), Some("personal"));
        assert!(r.assigned_filename.is_none(), "assigned_filename should be cleared for reprocessing");
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some(), "output_filename should be set for processing");
        // The sorted file is voided, archive source stays for reprocessing
        assert!(r.current_paths.is_empty());
        assert!(r.deleted_paths.contains(&"sorted/personal/my-custom-name.pdf".to_string()));
    }

    #[test]
    fn test_user_rename_and_context_change() {
        // Context change + rename in sorted/ triggers reprocessing
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/personal/renamed.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.context.as_deref(), Some("personal"));
        assert!(r.assigned_filename.is_none());
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
    }

    #[test]
    fn test_context_change_with_subfolder_metadata() {
        // Move from sorted/arbeit/schmidt/file.pdf to sorted/privat/gesundheit/file.pdf
        // → reprocess with locked context + metadata from subfolders
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.context = Some("arbeit".to_string());
        record.metadata = Some(serde_json::json!({"sender": "schmidt", "type": "Rechnung"}));
        record.assigned_filename = Some("arbeit-rechnung-schmidt.pdf".to_string());
        record.hash = Some("old_hash".to_string());
        record.current_paths.push(PathEntry {
            path: "sorted/privat/gesundheit/file.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "archive/file.pdf".to_string(),
            timestamp: _ts(-10),
        });

        let context_folders: HashMap<String, Vec<String>> = [
            (
                "arbeit".to_string(),
                vec!["context".to_string(), "sender".to_string()],
            ),
            (
                "privat".to_string(),
                vec!["context".to_string(), "content".to_string()],
            ),
        ]
        .into_iter()
        .collect();

        let result = reconcile(&mut record, None, Some(&context_folders), None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert_eq!(r.context.as_deref(), Some("privat"));
        assert!(r.assigned_filename.is_none());
        assert!(r.hash.is_none());
        assert!(r.output_filename.is_some());
        // Metadata should be fresh from new subfolder, not old
        let meta = r.metadata.as_ref().expect("metadata should be set");
        assert_eq!(meta["content"], "gesundheit");
        assert!(meta.get("sender").is_none(), "old field should not be present");
        // Current sorted path voided, archive source stays
        assert!(r.current_paths.is_empty());
        assert!(r.deleted_paths.contains(&"sorted/privat/gesundheit/file.pdf".to_string()));
        assert_eq!(r.source_paths.len(), 1);
        assert_eq!(r.source_paths[0].path, "archive/file.pdf");
    }

    #[test]
    fn test_same_context_no_reprocess() {
        // Move within same context → no reprocessing, just adopt changes
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("old-name.pdf".to_string());
        record.current_paths.push(PathEntry {
            path: "sorted/arbeit/renamed.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert_eq!(r.context.as_deref(), Some("arbeit"));
        assert_eq!(r.assigned_filename.as_deref(), Some("renamed.pdf"));
    }

    #[test]
    fn test_exact_match_still_works() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/2025-01-Invoice.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    // -----------------------------------------------------------------------
    // TestPreprocessOutputDuplicateHash (7 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_output_hash_matches_other_source_hash() {
        let mut r1 = _make_record();
        r1.source_hash = "XHASH".to_string();
        r1.state = State::IsComplete;

        let mut r2 = _make_record();
        r2.source_hash = "other".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });

        let _r1_id = r1.id;
        let r2_id = r2.id;
        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("XHASH"),
            Some(1024),
        );

        let (modified_ids, _created, _rejected) = preprocess(&[change], &mut records, &mut new_records, |_p| {
            json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
        }, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&r2_id));
        // r2 is the second record
        assert_eq!(records[1].state, State::HasError);
        assert!(records[1].output_filename.is_none());
        assert!(records[1]
            .deleted_paths
            .contains(&".output/uuid-r2".to_string()));
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
        assert!(records[1].source_paths.is_empty());
        assert!(records[1].current_paths.is_empty());
        // r1 unchanged
        assert_eq!(records[0].state, State::IsComplete);
    }

    #[test]
    fn test_output_hash_matches_other_hash() {
        let mut r1 = _make_record();
        r1.source_hash = "src1".to_string();
        r1.hash = Some("XHASH".to_string());
        r1.state = State::IsComplete;

        let mut r2 = _make_record();
        r2.source_hash = "src2".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });

        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("XHASH"),
            Some(1024),
        );

        let (_modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            }, None, None);

        assert_eq!(records[1].state, State::HasError);
        assert!(records[1]
            .deleted_paths
            .contains(&".output/uuid-r2".to_string()));
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
    }

    #[test]
    fn test_output_hash_matches_own_source_hash_only() {
        let mut r = _make_record();
        r.source_hash = "XHASH".to_string();
        r.output_filename = Some("uuid-r".to_string());
        r.state = State::NeedsProcessing;

        let mut records = vec![r];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r",
            Some("XHASH"),
            Some(1024),
        );

        let (_modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {"date": "2025"}, "assigned_filename": "out.pdf"})
            }, None, None);

        assert_eq!(records[0].hash.as_deref(), Some("XHASH"));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(records[0].current_paths.len(), 1);
        assert!(records[0].deleted_paths.is_empty());
    }

    #[test]
    fn test_output_hash_no_match() {
        let mut r = _make_record();
        r.source_hash = "src".to_string();
        r.output_filename = Some("uuid-r".to_string());
        r.state = State::NeedsProcessing;

        let mut records = vec![r];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r",
            Some("NEWHASH"),
            Some(1024),
        );

        let (_modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            }, None, None);

        assert_eq!(records[0].hash.as_deref(), Some("NEWHASH"));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(records[0].current_paths.len(), 1);
        assert!(records[0].deleted_paths.is_empty());
    }

    #[test]
    fn test_output_duplicate_keeps_non_archive_sources() {
        let mut r1 = _make_record();
        r1.source_hash = "XHASH".to_string();
        r1.state = State::IsComplete;

        let mut r2 = _make_record();
        r2.source_hash = "src2".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });
        r2.source_paths.push(PathEntry {
            path: "missing/r2.pdf".to_string(),
            timestamp: _ts(1),
        });

        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("XHASH"),
            Some(1024),
        );

        let (_modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            }, None, None);

        assert_eq!(records[1].state, State::HasError);
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
        // missing/r2.pdf stays in source_paths
        assert_eq!(records[1].source_paths.len(), 1);
        assert_eq!(records[1].source_paths[0].path, "missing/r2.pdf");
    }

    #[test]
    fn test_two_identical_outputs_same_cycle() {
        let mut r1 = _make_record();
        r1.source_hash = "src1".to_string();
        r1.output_filename = Some("uuid-r1".to_string());
        r1.state = State::NeedsProcessing;
        r1.source_paths.push(PathEntry {
            path: "archive/r1.pdf".to_string(),
            timestamp: _ts(0),
        });

        let mut r2 = _make_record();
        r2.source_hash = "src2".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });

        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let c1 = _make_change(
            EventType::Addition,
            ".output/uuid-r1",
            Some("IDENTICAL"),
            Some(1024),
        );
        let c2 = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("IDENTICAL"),
            Some(1024),
        );

        let (_modified_ids, _created, _rejected) =
            preprocess(&[c1, c2], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            }, None, None);

        // r1: ingested normally (first in the batch)
        assert_eq!(records[0].hash.as_deref(), Some("IDENTICAL"));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(records[0].current_paths.len(), 1);
        assert!(records[0].deleted_paths.is_empty());

        // r2: detected as duplicate (r1.hash was already set)
        assert_eq!(records[1].state, State::HasError);
        assert!(records[1].output_filename.is_none());
        assert!(records[1]
            .deleted_paths
            .contains(&".output/uuid-r2".to_string()));
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
        assert!(records[1].source_paths.is_empty());
    }

    // -----------------------------------------------------------------------
    // TestPreprocessIncomingMatchesHash (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_incoming_matches_record_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "SOURCE".to_string();
            r.hash = Some("PROCESSED".to_string());
            r.state = State::IsComplete;
            r.current_paths.push(PathEntry {
                path: "sorted/work/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/copy.pdf",
            Some("PROCESSED"),
            Some(500),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&records[0].id));
        // incoming matches source_hash first, but here source_hash="SOURCE" != "PROCESSED"
        // so it matches hash="PROCESSED" -> current_paths
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "incoming/copy.pdf"));
        assert!(created.is_empty());
    }

    #[test]
    fn test_incoming_matches_source_hash_preferred() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "HASH1".to_string();
            r.hash = Some("HASH1".to_string());
            r.state = State::IsComplete;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/doc.txt",
            Some("HASH1"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        // Matched by source_hash (first in list for incoming) → source_paths
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.txt"));
    }

    #[test]
    fn test_incoming_hash_match_cleaned_by_reconcile() {
        let mut record = _make_record();
        record.source_hash = "SOURCE".to_string();
        record.hash = Some("PROCESSED".to_string());
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "incoming/copy.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        // incoming/ is not a valid current location → moved to deleted_paths
        assert!(r.deleted_paths.contains(&"incoming/copy.pdf".to_string()));
        assert!(!r
            .current_paths
            .iter()
            .any(|pe| pe.path == "incoming/copy.pdf"));
        assert_eq!(r.state, State::IsComplete);
    }

    // -----------------------------------------------------------------------
    // TestPreprocessReset (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_reset_matches_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("reset_hash".to_string());
            r.state = State::IsComplete;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reset/doc.pdf",
            Some("reset_hash"),
            Some(700),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "reset/doc.pdf"));
    }

    #[test]
    fn test_reset_unknown_not_created() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reset/stray.pdf",
            Some("unknown_hash"),
            Some(500),
        );

        let (modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    #[test]
    fn test_reset_does_not_match_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "src_hash".to_string();
            r.hash = None;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reset/doc.pdf",
            Some("src_hash"),
            Some(500),
        );

        let (modified_ids, _created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, None, None);

        // source_hash match is not configured for reset/
        assert!(modified_ids.is_empty());
    }

    // -----------------------------------------------------------------------
    // TestReconcileReset (8 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_reset_moves_to_sorted() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.metadata = Some(json!({
            "context": "work",
            "date": "2025-01-15",
            "type": "Invoice",
        }));
        record.assigned_filename = Some("old-name.pdf".to_string());
        record.original_filename = "original.pdf".to_string();

        let recompute = |_r: &Record| -> Option<String> {
            Some("invoice-2025-01-15.pdf".to_string())
        };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert_eq!(
            r.assigned_filename.as_deref(),
            Some("invoice-2025-01-15.pdf")
        );
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/invoice-2025-01-15.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some("reset/doc.pdf"));
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_reset_uses_context_folders() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.metadata = Some(json!({
            "context": "work",
            "date": "2025",
            "sender": "Acme",
        }));
        record.assigned_filename = Some("old.pdf".to_string());

        let recompute = |_r: &Record| -> Option<String> {
            Some("invoice-2025.pdf".to_string())
        };
        let folders: HashMap<String, Vec<String>> = [(
            "work".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let result = reconcile(&mut record, None, Some(&folders), Some(&recompute));

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("invoice-2025.pdf"));
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/Acme/invoice-2025.pdf")
        );
    }

    #[test]
    fn test_reset_without_recompute_callback() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("existing-name.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("existing-name.pdf"));
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/existing-name.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some("reset/doc.pdf"));
    }

    #[test]
    fn test_reset_recompute_returns_none() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("keep-this.pdf".to_string());

        let recompute = |_r: &Record| -> Option<String> { None };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("keep-this.pdf"));
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/keep-this.pdf")
        );
    }

    #[test]
    fn test_reset_no_context() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = None;
        record.assigned_filename = Some("doc.pdf".to_string());

        let recompute = |_r: &Record| -> Option<String> { Some("new.pdf".to_string()) };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("new.pdf"));
        assert!(r.target_path.is_none());
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_reset_no_assigned_filename() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = None;

        let recompute = |_r: &Record| -> Option<String> { None };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_reset_deduplicates_current_paths() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "processed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        // Dedup keeps most recent (reset/), deletes older (processed/)
        assert_eq!(r.current_paths.len(), 1);
        assert_eq!(r.current_paths[0].path, "reset/doc.pdf");
        assert!(r.deleted_paths.contains(&"processed/doc.pdf".to_string()));
    }

    #[test]
    fn test_reset_not_cleaned_as_invalid_location() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        // Should NOT be in deleted_paths
        assert!(!r.deleted_paths.contains(&"reset/doc.pdf".to_string()));
        assert_eq!(r.current_reference.as_deref(), Some("reset/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestReconcileContextFieldNames (1 test)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Sorted → archive → trash lifecycle (regression)
    // -----------------------------------------------------------------------

    /// Full lifecycle: file added to sorted/ → processed → trash copy added.
    /// Verifies that archive source_paths are preserved when trash triggers
    /// NeedsDeletion, so step4 can void the archive copy.
    #[test]
    fn test_sorted_trash_lifecycle_preserves_archive_source_paths() {
        // --- Phase 1: File appears in sorted/ ---
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let sorted_change = ChangeItem {
            event_type: EventType::Addition,
            path: "sorted/ga/file.m4a".to_string(),
            hash: Some("src_hash_123".to_string()),
            content_hash: Some("content_abc".to_string()),
            size: Some(10000),
        };

        let (_modified, created, _rejected) =
            preprocess(&[sorted_change], &mut records, &mut new_records, _noop_sidecar, None, None);
        assert_eq!(created.len(), 1, "Should create one new record");
        records.push(created.into_iter().next().unwrap());

        // --- Phase 2: Reconcile IsNew → NeedsProcessing ---
        let result = reconcile(&mut records[0], None, None, None);
        assert!(result.is_some());
        assert_eq!(records[0].state, State::NeedsProcessing);
        assert_eq!(
            records[0].source_reference.as_deref(),
            Some("sorted/ga/file.m4a"),
            "source_reference should point to sorted/ path"
        );
        assert!(
            records[0].source_paths.iter().any(|pe| pe.path == "archive/file.m4a"),
            "archive/ entry should be added to source_paths"
        );

        // --- Phase 3: Simulate step4 moving sorted → archive ---
        // (In real code, FilesystemReconciler does this. Here we simulate.)
        records[0].source_reference = None; // cleared by clear_temporary_fields next cycle
        // Replace the sorted/ entry with archive/ (as step4 would do)
        if let Some(idx) = records[0].source_paths.iter().position(|pe| pe.path == "sorted/ga/file.m4a") {
            let old = records[0].source_paths[idx].clone();
            records[0].missing_source_paths.push(old);
            records[0].source_paths[idx] = PathEntry {
                path: "archive/file.m4a".to_string(),
                timestamp: _ts(1),
            };
        }

        // --- Phase 4: Processing complete — output appears in .output/ ---
        let output_change = ChangeItem {
            event_type: EventType::Addition,
            path: ".output/uuid-sorted-test".to_string(),
            hash: Some("output_hash".to_string()),
            content_hash: Some("output_content".to_string()),
            size: Some(5000),
        };
        records[0].output_filename = Some("uuid-sorted-test".to_string());
        let sidecar = json!({
            "context": "ga",
            "metadata": {"date": "2026-01-10"},
            "assigned_filename": "ga-2026-01-10-file.txt",
        });
        let (_modified, _created, _rejected) =
            preprocess(&[output_change], &mut records, &mut new_records, |_| sidecar.clone(), None, None);

        assert!(records[0].output_filename.is_none(), "output_filename consumed");
        assert_eq!(records[0].current_paths.len(), 1);
        assert_eq!(records[0].current_paths[0].path, ".output/uuid-sorted-test");

        // --- Phase 5: Reconcile moves .output → sorted target ---
        let result = reconcile(&mut records[0], None, None, None);
        assert!(result.is_some());
        assert!(records[0].target_path.is_some(), "target_path should be set");
        assert!(records[0].current_reference.is_some(), "current_reference should be set");

        // Simulate step4 moving .output → sorted/
        records[0].current_paths[0] = PathEntry {
            path: "sorted/ga/schmidt/ga-2026-01-10-file.txt".to_string(),
            timestamp: _ts(2),
        };
        records[0].current_reference = None;
        records[0].target_path = None;

        // --- Phase 6: Next reconcile picks up sorted/ current → IsComplete ---
        let result = reconcile(&mut records[0], None, None, None);
        assert!(result.is_some());
        assert_eq!(records[0].state, State::IsComplete);

        // Verify archive source_paths are intact at this point
        let archive_count = records[0]
            .source_paths
            .iter()
            .filter(|pe| pe.path.starts_with("archive/"))
            .count();
        assert!(archive_count > 0, "archive source_paths should exist before trash");

        // --- Phase 7: Copy of source .m4a appears in trash/ ---
        let trash_change = ChangeItem {
            event_type: EventType::Addition,
            path: "trash/file.m4a".to_string(),
            hash: Some("src_hash_123".to_string()), // same source_hash
            content_hash: Some("content_abc".to_string()),
            size: Some(10000),
        };
        let (_modified, _created, _rejected) =
            preprocess(&[trash_change], &mut records, &mut new_records, _noop_sidecar, None, None);

        assert!(
            records[0].source_paths.iter().any(|pe| pe.path == "trash/file.m4a"),
            "trash/ should be added to source_paths"
        );

        // --- Phase 8: Reconcile detects trash → NeedsDeletion ---
        let result = reconcile(&mut records[0], None, None, None);
        assert!(result.is_some());
        assert_eq!(records[0].state, State::NeedsDeletion);

        // THE KEY ASSERTION: archive source_paths must survive the
        // trash → NeedsDeletion transition so step4 can void them.
        let archive_paths: Vec<&str> = records[0]
            .source_paths
            .iter()
            .filter(|pe| pe.path.starts_with("archive/"))
            .map(|pe| pe.path.as_str())
            .collect();
        assert!(
            !archive_paths.is_empty(),
            "BUG: archive source_paths lost during trash → NeedsDeletion transition. \
             Step4 won't be able to void the archive copy. Got source_paths: {:?}",
            records[0].source_paths.iter().map(|pe| &pe.path).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_sorted_with_context_field_names_missing_fields_added() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.metadata = Some(json!({"context": "work", "date": "2025-01-01"}));
        record.assigned_filename = Some("doc.pdf".to_string());

        let cfn: HashMap<String, Vec<String>> = [(
            "work".to_string(),
            vec![
                "context".to_string(),
                "date".to_string(),
                "category".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let result = reconcile(&mut record, Some(&cfn), None, None);

        let r = result.unwrap();
        let meta = r.metadata.as_ref().unwrap();
        assert!(meta.get("category").is_some());
        assert!(meta.get("category").unwrap().is_null());
        assert_eq!(meta.get("context").unwrap().as_str(), Some("work"));
        assert_eq!(meta.get("date").unwrap().as_str(), Some("2025-01-01"));
    }

    /// Audio file placed in sorted/{context}/{field}/ with conditional filename
    /// pattern: AI returns a different context and field, but the record keeps
    /// the folder-derived context and stays in its directory.
    #[test]
    fn test_sorted_audio_context_locked_ai_disagrees() {
        // folders config: privat → ["context", "type"]
        // File at sorted/privat/Arztbrief/ → context="privat", type="Arztbrief" (both locked)
        let context_folders: HashMap<String, Vec<String>> = [(
            "privat".to_string(),
            vec!["context".to_string(), "type".to_string()],
        )]
        .into_iter()
        .collect();

        // 1. Detect the audio file in sorted/privat/Arztbrief/
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/privat/Arztbrief/audio-memo.m4a",
            Some("audio_hash_1"),
            Some(50000),
        );

        let (_modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, Some(&context_folders), None);

        assert_eq!(created.len(), 1);
        let record = &created[0];
        // Context is locked from the directory path
        assert_eq!(record.context.as_deref(), Some("privat"));
        assert_eq!(record.original_filename, "audio-memo.m4a");
        assert_eq!(record.state, State::IsNew);
        // Subfolder field "type" is extracted into metadata at creation time
        let meta = record.metadata.as_ref().expect("metadata should be set from subfolder");
        assert_eq!(
            meta.get("type").and_then(|v| v.as_str()),
            Some("Arztbrief"),
            "type must be extracted from subfolder at creation time"
        );

        // 2. Reconcile: IsNew → NeedsProcessing (assigns output_filename)
        let mut all_records = created;
        let result = reconcile(&mut all_records[0], None, None, None);
        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
        let output_uuid = r.output_filename.clone().unwrap();

        // 3. Simulate processing completion: .output file arrives with
        //    sidecar metadata where AI disagrees on both context and type
        let sidecar_fn = |path: &str| -> serde_json::Value {
            if path.contains(&output_uuid) {
                json!({
                    "context": "arbeit",
                    "metadata": {
                        "context": "arbeit",
                        "type": "Rechnung",
                        "sender": "Schulze GmbH",
                        "date": "2025-08-15"
                    },
                    "assigned_filename": "arbeit-rechnung-2025-08-15-schulze.txt"
                })
            } else {
                json!({})
            }
        };

        let output_change = _make_change(
            EventType::Addition,
            &format!(".output/{}", output_uuid),
            Some("output_hash_1"),
            Some(10000),
        );
        let (modified_ids, _, _rejected) = preprocess(
            &[output_change],
            &mut all_records,
            &mut Vec::new(),
            sidecar_fn,
            Some(&context_folders),
            None,
        );

        assert!(!modified_ids.is_empty());
        let r = &all_records[0];

        // Context stays locked to "privat" (from sorted/ directory),
        // NOT overridden by sidecar's "arbeit"
        assert_eq!(
            r.context.as_deref(),
            Some("privat"),
            "Context must remain locked to folder, not AI's classification"
        );

        // Subfolder-locked field "type" must keep folder value "Arztbrief",
        // NOT the AI's "Rechnung". Non-locked fields from AI are adopted.
        let meta = r.metadata.as_ref().unwrap();
        assert_eq!(
            meta.get("type").unwrap().as_str(),
            Some("Arztbrief"),
            "type must remain locked to subfolder value, not AI's classification"
        );
        assert_eq!(
            meta.get("sender").unwrap().as_str(),
            Some("Schulze GmbH"),
            "sender (not locked) should be adopted from AI"
        );
        assert_eq!(
            meta.get("date").unwrap().as_str(),
            Some("2025-08-15"),
            "date (not locked) should be adopted from AI"
        );

        // assigned_filename comes from AI (service used its pattern)
        assert!(r.assigned_filename.is_some());
        assert!(r.output_filename.is_none()); // cleared after ingestion

        // 4. Reconcile: .output → sorted/ target path using locked context
        let result = reconcile(
            &mut all_records[0],
            None,
            Some(&context_folders),
            None,
        );
        let r = result.unwrap();

        // Context still locked to folder-derived value
        assert_eq!(
            r.context.as_deref(),
            Some("privat"),
            "Context must stay locked after reconcile"
        );

        // assigned_filename preserved from sidecar (service resolves it)
        assert_eq!(
            r.assigned_filename.as_deref(),
            Some("arbeit-rechnung-2025-08-15-schulze.txt"),
            "assigned_filename from sidecar is used as-is in .output path"
        );

        // Target path uses locked context, NOT the AI-classified context
        let target = r.target_path.as_deref().unwrap();
        assert!(
            target.starts_with("sorted/privat/"),
            "Target path should be under sorted/privat/ (locked context), got: {}",
            target,
        );
        assert!(
            !target.starts_with("sorted/arbeit/"),
            "Target path must NOT use AI-classified context 'arbeit', got: {}",
            target,
        );

        // Target path uses locked type subfolder "Arztbrief", not AI's "Rechnung"
        assert!(
            target.contains("/Arztbrief/"),
            "Target path should contain /Arztbrief/ (locked type subfolder), got: {}",
            target,
        );
    }

    #[test]
    fn test_sorted_audio_subfolder_locked_full_lifecycle() {
        // Full lifecycle test: audio file in sorted/arbeit/Fischer AG/
        // with folders: ["context", "sender"].
        //
        // Simulates multiple watcher cycles:
        //   Cycle 1: detect file, create record, reconcile (IsNew → NeedsProcessing),
        //            move source to archive
        //   Cycle 2: detect source removal, detect archive addition,
        //            launch processing (verify compute_locked_fields)
        //   Cycle 3: sidecar arrives (AI disagrees on sender), ingest, reconcile
        //
        // The sender field must stay locked to "Fischer AG" throughout.

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        // === Cycle 1: file detected in sorted/arbeit/Fischer AG/ ===
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/Fischer AG/subfolder-locked-audio.m4a",
            Some("audio_subfolder_hash"),
            Some(126000),
        );

        let (_modified_ids, created, _rejected) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar, Some(&context_folders), None);

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].context.as_deref(), Some("arbeit"));
        let meta = created[0].metadata.as_ref().expect("metadata must be set from subfolder");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("Fischer AG"),
            "sender must be extracted from subfolder at creation"
        );

        // Reconcile: IsNew → NeedsProcessing
        let mut all_records = created;
        let result = reconcile(&mut all_records[0], None, Some(&context_folders), None);
        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
        let output_uuid = r.output_filename.clone().unwrap();

        // Metadata must survive reconcile
        let meta = r.metadata.as_ref().expect("metadata must survive reconcile");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("Fischer AG"),
            "sender must survive reconcile"
        );

        // === Cycle 2: source moved to archive (step4 ran) ===
        // Detect removal of sorted/ path
        let removal = _make_change(
            EventType::Removal,
            "sorted/arbeit/Fischer AG/subfolder-locked-audio.m4a",
            None,
            None,
        );
        let (modified_ids, _, _rejected) = preprocess(
            &[removal],
            &mut all_records,
            &mut Vec::new(),
            _noop_sidecar,
            Some(&context_folders),
            None,
        );
        assert!(!modified_ids.is_empty(), "removal should modify record");

        // sorted/ path moved to missing_source_paths
        assert!(
            all_records[0].missing_source_paths.iter().any(|pe| pe.path.starts_with("sorted/")),
            "sorted/ path should be in missing_source_paths"
        );

        // Metadata must survive source removal
        let meta = all_records[0].metadata.as_ref().expect("metadata must survive source removal");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("Fischer AG"),
            "sender must survive source removal"
        );

        // Verify compute_locked_fields works from missing_source_paths
        // (this is the state the record is in when step3 runs)
        let locked = compute_locked_fields(&all_records[0], Some(&context_folders));
        let locked = locked.expect("compute_locked_fields must find sorted/ in missing_source_paths");
        assert_eq!(
            locked.get("sender").unwrap().get("value").unwrap().as_str(),
            Some("Fischer AG"),
            "locked_fields must contain sender from subfolder"
        );

        // === Cycle 3: processing complete, sidecar arrives ===
        // AI disagrees: sender="Schulze GmbH", context="arbeit"
        let sidecar_fn = |path: &str| -> serde_json::Value {
            if path.contains(&output_uuid) {
                json!({
                    "context": "arbeit",
                    "metadata": {
                        "context": "arbeit",
                        "type": "Rechnung",
                        "sender": "Schulze GmbH",
                        "date": "2025-11-05"
                    },
                    "assigned_filename": "arbeit-2025-11-05-fischer_ag-rechnung.txt"
                })
            } else {
                json!({})
            }
        };

        let output_change = _make_change(
            EventType::Addition,
            &format!(".output/{}", output_uuid),
            Some("output_hash_audio"),
            Some(5000),
        );
        let (modified_ids, _, _rejected) = preprocess(
            &[output_change],
            &mut all_records,
            &mut Vec::new(),
            sidecar_fn,
            Some(&context_folders),
            None,
        );
        assert!(!modified_ids.is_empty());

        let r = &all_records[0];
        // Context stays "arbeit"
        assert_eq!(r.context.as_deref(), Some("arbeit"));

        // Sender must stay locked to "Fischer AG", NOT AI's "Schulze GmbH"
        let meta = r.metadata.as_ref().unwrap();
        assert_eq!(
            meta.get("sender").unwrap().as_str(),
            Some("Fischer AG"),
            "sender must remain locked to subfolder value, not AI's 'Schulze GmbH'"
        );
        // Non-locked fields from AI are adopted
        assert_eq!(meta.get("type").unwrap().as_str(), Some("Rechnung"));
        assert_eq!(meta.get("date").unwrap().as_str(), Some("2025-11-05"));

        // === Final reconcile: compute target_path ===
        let result = reconcile(
            &mut all_records[0],
            None,
            Some(&context_folders),
            None,
        );
        let r = result.unwrap();

        let target = r.target_path.as_deref().unwrap();
        assert!(
            target.starts_with("sorted/arbeit/Fischer AG/"),
            "Target path must use locked sender subfolder 'Fischer AG', got: {}",
            target,
        );
        assert!(
            !target.contains("Schulze"),
            "Target path must NOT use AI's sender 'Schulze GmbH', got: {}",
            target,
        );
    }

    // -----------------------------------------------------------------------
    // Subfolder field extraction and locking tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_subfolder_field_locked_in_metadata() {
        // sorted/arbeit/schmidt/file.pdf with folders: ["context", "sender"]
        // → metadata should contain {"sender": "schmidt"}
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/schmidt/file.pdf",
            Some("subfolder_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, _rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            None,
        );

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].context.as_deref(), Some("arbeit"));
        let meta = created[0].metadata.as_ref().expect("metadata should be set");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("schmidt"),
            "sender field should be extracted from subfolder"
        );
    }

    #[test]
    fn test_sorted_no_subfolder_no_metadata() {
        // sorted/arbeit/file.pdf (no subfolder) → metadata should be None
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/file.pdf",
            Some("no_subfolder_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, _rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            None,
        );

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].context.as_deref(), Some("arbeit"));
        // No subfolder → no metadata (context is first folder, no second)
        assert!(
            created[0].metadata.is_none(),
            "metadata should be None when no subfolder fields"
        );
    }

    #[test]
    fn test_sorted_deep_folder_structure() {
        // sorted/arbeit/schmidt/Rechnung/file.pdf
        // folders: ["context", "sender", "type"]
        // → metadata should contain {"sender": "schmidt", "type": "Rechnung"}
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/schmidt/Rechnung/file.pdf",
            Some("deep_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec![
                "context".to_string(),
                "sender".to_string(),
                "type".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, _rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            None,
        );

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].context.as_deref(), Some("arbeit"));
        let meta = created[0].metadata.as_ref().expect("metadata should be set");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("schmidt"),
        );
        assert_eq!(
            meta.get("type").and_then(|v| v.as_str()),
            Some("Rechnung"),
        );
    }

    #[test]
    fn test_sorted_partial_folder_structure() {
        // sorted/arbeit/schmidt/file.pdf with folders: ["context", "sender", "type"]
        // Only 2 levels present → metadata has sender but not type
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/schmidt/file.pdf",
            Some("partial_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec![
                "context".to_string(),
                "sender".to_string(),
                "type".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, _rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            None,
        );

        assert_eq!(created.len(), 1);
        let meta = created[0].metadata.as_ref().expect("metadata should be set");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("schmidt"),
        );
        // type not present in path → not in metadata
        assert!(
            meta.get("type").is_none(),
            "type should not be in metadata when not present in path"
        );
    }

    #[test]
    fn test_sorted_no_context_folders_config() {
        // sorted/arbeit/schmidt/file.pdf with NO context_folders → just context set
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/schmidt/file.pdf",
            Some("no_config_hash"),
            Some(1024),
        );

        let (_modified_ids, created, _rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].context.as_deref(), Some("arbeit"));
        // No context_folders config → no metadata from subfolder
        assert!(created[0].metadata.is_none());
    }

    #[test]
    fn test_sorted_context_not_in_folders_config() {
        // sorted/arbeit/schmidt/file.pdf but folders config only has "privat"
        // → file should be rejected (invalid context)
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/schmidt/file.pdf",
            Some("no_ctx_match_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "privat".to_string(),
            vec!["context".to_string(), "type".to_string()],
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            None,
        );

        assert_eq!(created.len(), 0);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].0, "sorted/arbeit/schmidt/file.pdf");
        assert_eq!(rejected[0].1, "error/schmidt/file.pdf");
    }

    #[test]
    fn test_sorted_invalid_candidate_rejected() {
        // sorted/arbeit/foo/file.pdf — "foo" is not a valid sender and
        // allow_new_candidates is false → reject
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/foo/file.pdf",
            Some("inv_cand_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let folder_field_candidates: FolderFieldCandidates = [(
            "arbeit".to_string(),
            [(
                "sender".to_string(),
                (
                    vec![
                        "Schulze GmbH".to_string(),
                        "Fischer AG".to_string(),
                    ],
                    false, // allow_new_candidates = false
                ),
            )]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            Some(&folder_field_candidates),
        );

        assert_eq!(created.len(), 0);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].0, "sorted/arbeit/foo/file.pdf");
        assert_eq!(rejected[0].1, "error/arbeit/foo/file.pdf");
    }

    #[test]
    fn test_sorted_new_candidate_allowed() {
        // sorted/arbeit/new_sender/file.pdf — "new_sender" is not a known sender
        // but allow_new_candidates is true → allow
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/new_sender/file.pdf",
            Some("new_cand_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let folder_field_candidates: FolderFieldCandidates = [(
            "arbeit".to_string(),
            [(
                "sender".to_string(),
                (
                    vec!["Schulze GmbH".to_string()],
                    true, // allow_new_candidates = true
                ),
            )]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            Some(&folder_field_candidates),
        );

        assert_eq!(created.len(), 1, "file should be accepted");
        assert!(rejected.is_empty(), "should not be rejected");
        assert_eq!(created[0].context.as_deref(), Some("arbeit"));
        let meta = created[0].metadata.as_ref().expect("metadata should be set");
        assert_eq!(meta["sender"], "new_sender");
    }

    #[test]
    fn test_sorted_known_candidate_accepted() {
        // sorted/arbeit/Schulze GmbH/file.pdf — known sender → allow
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/arbeit/Schulze GmbH/file.pdf",
            Some("known_cand_hash"),
            Some(1024),
        );

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let folder_field_candidates: FolderFieldCandidates = [(
            "arbeit".to_string(),
            [(
                "sender".to_string(),
                (
                    vec!["Schulze GmbH".to_string()],
                    false,
                ),
            )]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect();

        let (_modified_ids, created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            Some(&context_folders),
            Some(&folder_field_candidates),
        );

        assert_eq!(created.len(), 1, "file should be accepted");
        assert!(rejected.is_empty());
    }

    #[test]
    fn test_compute_locked_fields_from_source_path() {
        // Record with sorted/ source path → locked_fields extracted
        let mut rec = _make_record();
        rec.context = Some("arbeit".to_string());
        rec.source_paths.push(PathEntry {
            path: "sorted/arbeit/schmidt/file.pdf".to_string(),
            timestamp: _ts(0),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let locked = compute_locked_fields(&rec, Some(&context_folders));
        let locked = locked.expect("should produce locked_fields");
        let sender = locked.get("sender").expect("should have sender");
        assert_eq!(sender.get("value").unwrap().as_str(), Some("schmidt"));
    }

    #[test]
    fn test_compute_locked_fields_deep_path() {
        // Record with deep sorted/ source path
        let mut rec = _make_record();
        rec.context = Some("arbeit".to_string());
        rec.source_paths.push(PathEntry {
            path: "sorted/arbeit/schmidt/Rechnung/file.pdf".to_string(),
            timestamp: _ts(0),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec![
                "context".to_string(),
                "sender".to_string(),
                "type".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let locked = compute_locked_fields(&rec, Some(&context_folders));
        let locked = locked.expect("should produce locked_fields");
        assert_eq!(
            locked.get("sender").unwrap().get("value").unwrap().as_str(),
            Some("schmidt"),
        );
        assert_eq!(
            locked.get("type").unwrap().get("value").unwrap().as_str(),
            Some("Rechnung"),
        );
    }

    #[test]
    fn test_compute_locked_fields_from_missing_source_paths() {
        // Record where sorted/ path is in missing_source_paths
        let mut rec = _make_record();
        rec.context = Some("arbeit".to_string());
        rec.missing_source_paths.push(PathEntry {
            path: "sorted/arbeit/schmidt/file.pdf".to_string(),
            timestamp: _ts(0),
        });
        rec.source_paths.push(PathEntry {
            path: "archive/file.pdf".to_string(),
            timestamp: _ts(1),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let locked = compute_locked_fields(&rec, Some(&context_folders));
        let locked = locked.expect("should find locked_fields from missing_source_paths");
        assert_eq!(
            locked.get("sender").unwrap().get("value").unwrap().as_str(),
            Some("schmidt"),
        );
    }

    #[test]
    fn test_compute_locked_fields_no_sorted_path() {
        // Record with only incoming/ source → no locked_fields
        let mut rec = _make_record();
        rec.context = Some("arbeit".to_string());
        rec.source_paths.push(PathEntry {
            path: "incoming/file.pdf".to_string(),
            timestamp: _ts(0),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let locked = compute_locked_fields(&rec, Some(&context_folders));
        assert!(locked.is_none(), "no sorted/ path → no locked_fields");
    }

    #[test]
    fn test_compute_locked_fields_only_context_folder() {
        // folders: ["context"] only → no locked fields (context isn't locked_fields)
        let mut rec = _make_record();
        rec.context = Some("arbeit".to_string());
        rec.source_paths.push(PathEntry {
            path: "sorted/arbeit/file.pdf".to_string(),
            timestamp: _ts(0),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string()],
        )]
        .into_iter()
        .collect();

        let locked = compute_locked_fields(&rec, Some(&context_folders));
        assert!(locked.is_none(), "only context folder → no locked_fields");
    }

    #[test]
    fn test_merge_metadata_preserves_preset_values() {
        let existing = json!({"sender": "schmidt"});
        let incoming = json!({"sender": "Mueller", "type": "Rechnung", "date": "2025-01-15"});

        let merged = merge_metadata(Some(&existing), Some(incoming));
        let merged = merged.unwrap();

        assert_eq!(merged.get("sender").unwrap().as_str(), Some("schmidt"),
            "pre-set sender should be preserved");
        assert_eq!(merged.get("type").unwrap().as_str(), Some("Rechnung"),
            "type from service should be added");
        assert_eq!(merged.get("date").unwrap().as_str(), Some("2025-01-15"),
            "date from service should be added");
    }

    #[test]
    fn test_merge_metadata_no_existing() {
        let incoming = json!({"type": "Rechnung"});
        let merged = merge_metadata(None, Some(incoming));
        assert_eq!(merged.unwrap().get("type").unwrap().as_str(), Some("Rechnung"));
    }

    #[test]
    fn test_merge_metadata_no_incoming() {
        let existing = json!({"sender": "schmidt"});
        let merged = merge_metadata(Some(&existing), None);
        assert_eq!(merged.unwrap().get("sender").unwrap().as_str(), Some("schmidt"));
    }

    #[test]
    fn test_merge_metadata_null_existing_overwritten() {
        // Null values in existing should be overwritten by incoming
        let existing = json!({"sender": null, "type": "Notiz"});
        let incoming = json!({"sender": "Mueller", "type": "Rechnung"});

        let merged = merge_metadata(Some(&existing), Some(incoming));
        let merged = merged.unwrap();

        assert_eq!(merged.get("sender").unwrap().as_str(), Some("Mueller"),
            "null existing value should be overwritten");
        assert_eq!(merged.get("type").unwrap().as_str(), Some("Notiz"),
            "non-null existing value should be preserved");
    }

    #[test]
    fn test_reconcile_sorted_updates_metadata_from_subfolder() {
        // Record with current_path in sorted/arbeit/schmidt/file.pdf
        // → reconcile should set sender in metadata
        let mut rec = _make_record();
        rec.state = State::IsComplete;
        rec.hash = Some("h".to_string());
        rec.context = Some("arbeit".to_string());
        rec.assigned_filename = Some("file.pdf".to_string());
        rec.current_paths.push(PathEntry {
            path: "sorted/arbeit/schmidt/file.pdf".to_string(),
            timestamp: _ts(0),
        });
        rec.source_paths.push(PathEntry {
            path: "archive/file.pdf".to_string(),
            timestamp: _ts(-1),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let result = reconcile(&mut rec, None, Some(&context_folders), None);
        let r = result.unwrap();

        let meta = r.metadata.as_ref().expect("metadata should be set");
        assert_eq!(
            meta.get("sender").and_then(|v| v.as_str()),
            Some("schmidt"),
            "reconcile should set sender from subfolder"
        );
    }

    #[test]
    fn test_reconcile_sorted_deep_updates_metadata() {
        // sorted/arbeit/schmidt/Rechnung/file.pdf with folders: ["context", "sender", "type"]
        let mut rec = _make_record();
        rec.state = State::IsComplete;
        rec.hash = Some("h".to_string());
        rec.context = Some("arbeit".to_string());
        rec.assigned_filename = Some("file.pdf".to_string());
        rec.metadata = Some(json!({"sender": "old", "type": "old"}));
        rec.current_paths.push(PathEntry {
            path: "sorted/arbeit/schmidt/Rechnung/file.pdf".to_string(),
            timestamp: _ts(0),
        });
        rec.source_paths.push(PathEntry {
            path: "archive/file.pdf".to_string(),
            timestamp: _ts(-1),
        });

        let context_folders: HashMap<String, Vec<String>> = [(
            "arbeit".to_string(),
            vec![
                "context".to_string(),
                "sender".to_string(),
                "type".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let result = reconcile(&mut rec, None, Some(&context_folders), None);
        let r = result.unwrap();

        let meta = r.metadata.as_ref().unwrap();
        assert_eq!(meta.get("sender").unwrap().as_str(), Some("schmidt"));
        assert_eq!(meta.get("type").unwrap().as_str(), Some("Rechnung"));
    }

    // -----------------------------------------------------------------------
    // Reclassify tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_reclassify_source_hash_match_live_source() {
        // File in reclassify/ matches a record by source_hash.
        // Record has a live archive source → reclassify file is just a trigger.
        let mut record = _make_record();
        record.source_hash = "src_hash_1".to_string();
        record.hash = Some("out_hash_1".to_string());
        record.state = State::IsComplete;
        record.context = Some("arbeit".to_string());
        record.metadata = Some(json!({"type": "Rechnung"}));
        record.assigned_filename = Some("arbeit-rechnung.pdf".to_string());
        record.source_paths.push(PathEntry {
            path: "archive/file.pdf".to_string(),
            timestamp: _ts(-10),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/arbeit/arbeit-rechnung.pdf".to_string(),
            timestamp: _ts(-5),
        });
        let record_id = record.id;

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/file.pdf",
            Some("src_hash_1"),
            Some(1024),
        );

        let (modified_ids, created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert!(rejected.is_empty());
        assert!(created.is_empty());
        assert!(modified_ids.contains(&record_id));

        let r = &records[0];
        // Case 1: live source in archive → NeedsProcessing directly
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.context.is_none());
        assert!(r.metadata.is_none());
        assert!(r.assigned_filename.is_none());
        assert!(r.hash.is_none());
        assert!(r.output_filename.is_some()); // UUID assigned directly

        // Live archive source is kept
        assert_eq!(r.source_paths.len(), 1);
        assert_eq!(r.source_paths[0].path, "archive/file.pdf");

        // Reclassify trigger + old current_paths are in deleted_paths
        assert!(r.deleted_paths.contains(&"reclassify/file.pdf".to_string()));
        assert!(r.deleted_paths.contains(&"sorted/arbeit/arbeit-rechnung.pdf".to_string()));
        assert!(r.current_paths.is_empty());
    }

    #[test]
    fn test_reclassify_source_hash_match_no_live_source() {
        // File in reclassify/ matches by source_hash, but no live source exists.
        // The reclassify file becomes the new source.
        let mut record = _make_record();
        record.source_hash = "src_hash_2".to_string();
        record.hash = Some("out_hash_2".to_string());
        record.state = State::IsMissing;
        record.context = Some("arbeit".to_string());
        // No source_paths (all missing)
        record.missing_source_paths.push(PathEntry {
            path: "archive/file.pdf".to_string(),
            timestamp: _ts(-20),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/arbeit/old.pdf".to_string(),
            timestamp: _ts(-5),
        });
        let record_id = record.id;

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/file.pdf",
            Some("src_hash_2"),
            Some(1024),
        );

        let (modified_ids, created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert!(rejected.is_empty());
        assert!(created.is_empty());
        assert!(modified_ids.contains(&record_id));

        let r = &records[0];
        assert_eq!(r.state, State::IsNew);

        // Reclassify file becomes the new source
        assert_eq!(r.source_paths.len(), 1);
        assert_eq!(r.source_paths[0].path, "reclassify/file.pdf");

        // Old current_paths cleaned up
        assert!(r.deleted_paths.contains(&"sorted/arbeit/old.pdf".to_string()));
        assert!(r.current_paths.is_empty());
    }

    #[test]
    fn test_reclassify_hash_match_no_live_source_rejected() {
        // File matches by hash only, no live source → can't recover, reject to error
        let mut record = _make_record();
        record.source_hash = "different_src_hash".to_string();
        record.hash = Some("out_hash_3".to_string());
        record.state = State::IsComplete;
        record.context = Some("arbeit".to_string());
        // No live source_paths
        let original_state = record.state;

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/processed.pdf",
            Some("out_hash_3"), // matches record.hash, NOT source_hash
            Some(1024),
        );

        let (_modified_ids, _created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].0, "reclassify/processed.pdf");
        assert_eq!(rejected[0].1, "error/processed.pdf");
        // Record untouched
        assert_eq!(records[0].state, original_state);
    }

    #[test]
    fn test_reclassify_no_match_rejected() {
        // File in reclassify/ matches no record → reject to error
        let mut record = _make_record();
        record.source_hash = "some_hash".to_string();
        record.hash = Some("other_hash".to_string());

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/unknown.pdf",
            Some("no_match_hash"),
            Some(1024),
        );

        let (_modified_ids, _created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].1, "error/unknown.pdf");
    }

    #[test]
    fn test_reclassify_skips_deleted_records() {
        // Record in NeedsDeletion should not be matched
        let mut record = _make_record();
        record.source_hash = "del_hash".to_string();
        record.state = State::NeedsDeletion;

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/file.pdf",
            Some("del_hash"),
            Some(1024),
        );

        let (_modified_ids, _created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert_eq!(rejected.len(), 1, "should not match deleted record");
    }

    #[test]
    fn test_reclassify_hash_match_with_live_source() {
        // File matches by hash (not source_hash), but live source exists → OK
        let mut record = _make_record();
        record.source_hash = "original_src".to_string();
        record.hash = Some("processed_hash".to_string());
        record.state = State::IsComplete;
        record.context = Some("privat".to_string());
        record.assigned_filename = Some("privat-doc.pdf".to_string());
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(-10),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/privat/privat-doc.pdf".to_string(),
            timestamp: _ts(-5),
        });
        let record_id = record.id;

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/output.pdf",
            Some("processed_hash"), // matches record.hash
            Some(2048),
        );

        let (modified_ids, _created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert!(rejected.is_empty());
        assert!(modified_ids.contains(&record_id));

        let r = &records[0];
        // Case 1: live source in archive → NeedsProcessing directly
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
        // Live archive source kept
        assert_eq!(r.source_paths.len(), 1);
        assert_eq!(r.source_paths[0].path, "archive/doc.pdf");
        // Reclassify file is trigger → in deleted_paths
        assert!(r.deleted_paths.contains(&"reclassify/output.pdf".to_string()));
    }

    #[test]
    fn test_reclassify_source_in_missing_moved_back_to_archive() {
        // Race condition: sorted file was deleted in a previous cycle,
        // causing the source to be moved from archive/ to missing/.
        // Now a reclassify file appears.  The source in missing/ should
        // be moved back to archive/ (via source_reference + IsNew).
        let mut record = _make_record();
        record.source_hash = "src_hash_race".to_string();
        record.hash = Some("out_hash_race".to_string());
        record.state = State::IsMissing;
        record.context = Some("arbeit".to_string());
        record.metadata = Some(json!({"type": "Rechnung"}));
        record.assigned_filename = Some("arbeit-rechnung.pdf".to_string());
        // Source was moved to missing/ by a previous cycle
        record.source_paths.push(PathEntry {
            path: "missing/file.pdf".to_string(),
            timestamp: _ts(-3),
        });
        // Original sorted path is in missing_current_paths
        record.missing_current_paths.push(PathEntry {
            path: "sorted/arbeit/arbeit-rechnung.pdf".to_string(),
            timestamp: _ts(-5),
        });
        let record_id = record.id;

        let mut records = vec![record];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reclassify/file.pdf",
            Some("src_hash_race"),
            Some(1024),
        );

        let (modified_ids, _created, rejected) = preprocess(
            &[change],
            &mut records,
            &mut new_records,
            _noop_sidecar,
            None,
            None,
        );

        assert!(rejected.is_empty());
        assert!(modified_ids.contains(&record_id));

        let r = &records[0];
        // Source is in missing/ → should use IsNew so reconcile moves it back to archive/
        assert_eq!(r.state, State::IsNew);
        assert!(r.output_filename.is_none());
        // source_reference should point to the missing/ path for step4 to move
        assert_eq!(r.source_reference.as_deref(), Some("missing/file.pdf"));
        // Source path still listed (will be updated by step4 after move)
        assert_eq!(r.source_paths.len(), 1);
        assert_eq!(r.source_paths[0].path, "missing/file.pdf");
        // Record was fully reset
        assert!(r.context.is_none());
        assert!(r.metadata.is_none());
        assert!(r.assigned_filename.is_none());
        assert!(r.hash.is_none());
        // Reclassify trigger in deleted_paths
        assert!(r.deleted_paths.contains(&"reclassify/file.pdf".to_string()));
    }
}
