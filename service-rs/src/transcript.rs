use serde_json::{json, Value};
use tracing::info;

// =============================================================================
// Quote normalization
// =============================================================================

fn normalize_quotes(text: &str) -> String {
    let mut result = text.to_string();
    let replacements = [
        ('\u{201C}', '"'), // left double
        ('\u{201D}', '"'), // right double
        ('\u{201E}', '"'), // double low-9
        ('\u{201F}', '"'), // double high-reversed-9
        ('\u{00AB}', '"'), // left guillemet
        ('\u{00BB}', '"'), // right guillemet
        ('\u{2018}', '\''), // left single
        ('\u{2019}', '\''), // right single
        ('\u{201A}', '\''), // single low-9
        ('\u{201B}', '\''), // single high-reversed-9
        ('\u{2039}', '\''), // left single guillemet
        ('\u{203A}', '\''), // right single guillemet
    ];
    for (from, to) in replacements {
        result = result.replace(from, &to.to_string());
    }
    result
}

// =============================================================================
// JSON processing
// =============================================================================

/// Strip words from transcript JSON, keeping only text/start/end/speaker.
pub fn strip_words_from_json(transcript_json: &Value) -> Value {
    let segments = transcript_json
        .get("segments")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let stripped_segments: Vec<Value> = segments
        .iter()
        .map(|seg| {
            let text = seg
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let text = normalize_quotes(text);
            json!({
                "text": text,
                "start": seg.get("start").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "end": seg.get("end").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "speaker": seg.get("speaker"),
            })
        })
        .collect();

    json!({
        "language": transcript_json.get("language").and_then(|v| v.as_str()).unwrap_or(""),
        "segments": stripped_segments,
    })
}

/// Create plain text from transcript JSON.
pub fn create_text_content(transcript_json: &Value) -> String {
    let segments = transcript_json
        .get("segments")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Auto-detect multi-speaker
    let mut speakers = std::collections::HashSet::new();
    for seg in &segments {
        if let Some(speaker) = seg.get("speaker") {
            if !speaker.is_null() {
                speakers.insert(speaker.to_string());
            }
        }
    }
    let multi_speaker = speakers.len() > 1;

    if !multi_speaker {
        return segments
            .iter()
            .filter_map(|seg| seg.get("text").and_then(|v| v.as_str()))
            .collect::<Vec<_>>()
            .join(" ");
    }

    // Multi-speaker format
    segments
        .iter()
        .map(|seg| {
            let text = seg.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let speaker = seg
                .get("speaker")
                .and_then(|v| v.as_str())
                .map(|s| format!("Speaker {}", s))
                .unwrap_or_else(|| "Speaker".to_string());
            let start = seg.get("start").and_then(|v| v.as_f64()).unwrap_or(0.0);

            if start > 0.0 {
                let timestamp = format_time(start);
                format!("[{}] [{}]: {}", timestamp, speaker, text)
            } else {
                format!("[{}]: {}", speaker, text)
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn format_time(seconds: f64) -> String {
    let total = seconds as u64;
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let secs = total % 60;
    if hours > 0 {
        format!("{:02}:{:02}:{:02}", hours, minutes, secs)
    } else {
        format!("{:02}:{:02}", minutes, secs)
    }
}

// =============================================================================
// Correction
// =============================================================================

const CORRECTION_PROMPT: &str = r#"The following JSON array contains text segments from an audio transcription.
Check for errors resulting from wrong transcription.
Common errors include:
- Misheard words or phrases
- Incorrect word boundaries
- Missing or incorrect punctuation
- Grammatical errors introduced by the transcription
- Names or technical terms that may have been transcribed incorrectly

Only fix clear transcription errors - do not rephrase or restructure the content.

Meta information (such as date, subject, participants, or topic) may be dictated at the beginning or the end of the recording. It must always appear at the beginning of the output array. If it was dictated at the end, move those segments to the beginning.
When you reorder segments, return a JSON object instead of a plain array:
{"texts": ["corrected segment 1", ...], "order": [4, 5, 0, 1, 2, 3]}
- "texts": the corrected text strings in the new order
- "order": the original 0-based indices rearranged to reflect the new order
When no reordering is needed, return the plain JSON array as before.

The response must have exactly the same number of elements as the input.
Each element must be the corrected version of the corresponding input text.

CRITICAL: Return ONLY valid JSON. Do not truncate. Do not add commentary.
- All special characters in strings must be properly escaped (especially quotes and backslashes).
- The output MUST be complete — if the input has N segments, the output must have exactly N strings.
- If the output would be very long, keep corrections minimal to stay within limits."#;

const CONTEXT_TEMPLATE: &str = r#"
Additional context for this transcription:
{context}

Use this context to help identify and correct names, technical terms, and domain-specific vocabulary."#;

/// Max output tokens the model can produce in one response.
const MAX_OUTPUT_TOKENS: usize = 48_000;

/// Rough chars-per-token estimate for token budgeting.
const CHARS_PER_TOKEN: usize = 4;

const CHUNK_CONTINUATION_PROMPT: &str = r#"This is a continuation of a longer transcript correction task.

Summary of the preceding portion of the transcript:
{summary}

Apply the same correction rules as before. Maintain consistency with names, terms, and style from the preceding portion."#;

const SUMMARIZE_PROMPT: &str = r#"Summarize the following corrected transcript chunk in at most 500 words. Focus on:
- Names of people, places, and organizations mentioned (with correct spellings)
- Key topics and technical terms discussed
- The general flow / narrative so far
- Any recurring terms that were corrected

Return ONLY the summary text, no JSON.

Corrected text:
"#;

/// Estimate token count from a string (~4 chars per token).
fn estimate_tokens(s: &str) -> usize {
    s.len() / CHARS_PER_TOKEN
}

/// Split text_array indices into even chunks that each fit within the output token limit.
fn compute_chunks(text_array: &[String]) -> Vec<std::ops::Range<usize>> {
    let total_tokens: usize = text_array.iter().map(|s| estimate_tokens(s)).sum();

    if total_tokens <= MAX_OUTPUT_TOKENS {
        return vec![0..text_array.len()];
    }

    let num_chunks = (total_tokens + MAX_OUTPUT_TOKENS - 1) / MAX_OUTPUT_TOKENS;
    let chunk_size = (text_array.len() + num_chunks - 1) / num_chunks;

    (0..text_array.len())
        .step_by(chunk_size)
        .map(|start| start..text_array.len().min(start + chunk_size))
        .collect()
}

/// Send a single chunk of text segments to the API for correction.
async fn correct_chunk(
    client: &reqwest::Client,
    base_url: &str,
    key: &str,
    model: &str,
    extended_thinking: bool,
    thinking_budget: u32,
    base_prompt: &str,
    previous_summary: Option<&str>,
    text_array: &[String],
    user_dir: Option<&std::path::Path>,
    filename: Option<&str>,
) -> Result<(Vec<String>, Option<Vec<u64>>), String> {
    let text_json = serde_json::to_string_pretty(text_array)
        .map_err(|e| format!("JSON error: {}", e))?;

    let mut prompt = base_prompt.to_string();
    if let Some(summary) = previous_summary {
        prompt.push_str("\n\n");
        prompt.push_str(&CHUNK_CONTINUATION_PROMPT.replace("{summary}", summary));
    }

    let user_content = format!(
        "{}\n\nText segments to correct:\n```json\n{}\n```",
        prompt, text_json
    );

    let estimated_output_tokens = (text_json.len() / 3) as u64;
    let output_budget = estimated_output_tokens.max(16000).min(128000);
    let max_tokens = if extended_thinking {
        output_budget + thinking_budget as u64
    } else {
        output_budget
    };

    let mut params = json!({
        "model": model,
        "max_tokens": max_tokens,
        "stream": true,
        "messages": [{"role": "user", "content": user_content}],
    });

    if extended_thinking {
        params["thinking"] = json!({
            "type": "enabled",
            "budget_tokens": thinking_budget,
        });
    }

    let response = client
        .post(format!("{}/v1/messages", base_url))
        .header("x-api-key", key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&params)
        .send()
        .await
        .map_err(|e| format!("API request failed: {}", e))?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("API error: {}", body));
    }

    let body = response.text().await.map_err(|e| format!("Read error: {}", e))?;
    let response_text = parse_sse_text(&body);

    if let Some(ud) = user_dir {
        let (input_t, output_t) = parse_sse_usage(&body);
        if input_t > 0 || output_t > 0 {
            crate::costs::get_cost_tracker().record_anthropic(
                model, input_t, output_t, ud, false,
            );
            crate::costs::get_cost_tracker().log_api_call(
                "correct_transcript",
                model,
                filename,
                &serde_json::json!({"segments": text_array.len()}),
                input_t,
                output_t,
                ud,
            );
        }
    }

    parse_correction_result(&response_text, text_array.len())
}

/// Ask the model to summarize a corrected chunk for context continuity.
async fn summarize_chunk(
    client: &reqwest::Client,
    base_url: &str,
    key: &str,
    model: &str,
    corrected_texts: &[String],
) -> Result<String, String> {
    let joined = corrected_texts.join("\n");
    let user_content = format!("{}{}", SUMMARIZE_PROMPT, joined);

    let params = json!({
        "model": model,
        "max_tokens": 2048,
        "stream": true,
        "messages": [{"role": "user", "content": user_content}],
    });

    let response = client
        .post(format!("{}/v1/messages", base_url))
        .header("x-api-key", key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&params)
        .send()
        .await
        .map_err(|e| format!("Summarize request failed: {}", e))?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Summarize API error: {}", body));
    }

    let body = response.text().await.map_err(|e| format!("Read error: {}", e))?;
    Ok(parse_sse_text(&body))
}

/// Correct transcript JSON using Anthropic API.
///
/// Large transcripts are automatically split into even chunks that fit
/// within the model's output token limit.  Each chunk after the first
/// receives a summary of the preceding chunk for context continuity.
pub async fn correct_transcript_json(
    transcript_json: &Value,
    api_key: Option<&str>,
    model: &str,
    extended_thinking: bool,
    thinking_budget: u32,
    context: &str,
    _use_batch: bool,
    user_dir: Option<&std::path::Path>,
    filename: Option<&str>,
) -> Result<Value, String> {
    let key = api_key
        .map(|s| s.to_string())
        .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
        .ok_or_else(|| "Anthropic API key not set".to_string())?;

    let base_url = std::env::var("ANTHROPIC_BASE_URL")
        .unwrap_or_else(|_| "https://api.anthropic.com".to_string());

    let segments = transcript_json
        .get("segments")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let text_array: Vec<String> = segments
        .iter()
        .map(|seg| {
            normalize_quotes(seg.get("text").and_then(|v| v.as_str()).unwrap_or(""))
        })
        .collect();

    let mut base_prompt = CORRECTION_PROMPT.to_string();
    if !context.trim().is_empty() {
        base_prompt.push_str(&CONTEXT_TEMPLATE.replace("{context}", context.trim()));
    }

    let chunks = compute_chunks(&text_array);

    info!(
        "Correcting {} segments in {} chunk(s) (~{} chars total)",
        text_array.len(),
        chunks.len(),
        text_array.iter().map(|s| s.len()).sum::<usize>(),
    );

    let client = reqwest::Client::new();
    let mut all_corrected: Vec<String> = vec![String::new(); text_array.len()];
    let mut all_order: Option<Vec<u64>> = None;
    let mut previous_summary: Option<String> = None;

    for (chunk_idx, range) in chunks.iter().enumerate() {
        let chunk_texts = &text_array[range.clone()];

        info!(
            "Chunk {}/{}: segments {}..{} ({} segments, ~{} chars)",
            chunk_idx + 1,
            chunks.len(),
            range.start,
            range.end,
            chunk_texts.len(),
            chunk_texts.iter().map(|s| s.len()).sum::<usize>(),
        );

        let (corrected, order) = correct_chunk(
            &client,
            &base_url,
            &key,
            model,
            extended_thinking,
            thinking_budget,
            &base_prompt,
            previous_summary.as_deref(),
            chunk_texts,
            user_dir,
            filename,
        )
        .await?;

        // Only the first chunk may reorder (meta info at end → beginning)
        if chunk_idx == 0 {
            all_order = order;
        }

        // Place corrected texts at original indices
        for (i, text) in corrected.into_iter().enumerate() {
            let global_idx = range.start + i;
            if global_idx < all_corrected.len() {
                all_corrected[global_idx] = text;
            }
        }

        // Summarize this chunk for the next one (skip last chunk)
        if chunk_idx + 1 < chunks.len() {
            let chunk_corrected: Vec<String> = (range.start..range.end)
                .filter_map(|i| all_corrected.get(i).cloned())
                .collect();
            match summarize_chunk(&client, &base_url, &key, model, &chunk_corrected).await {
                Ok(summary) => {
                    info!("Generated summary for chunk {} ({} chars)", chunk_idx + 1, summary.len());
                    previous_summary = Some(summary);
                }
                Err(e) => {
                    info!("Summary generation failed (continuing without): {}", e);
                    previous_summary = None;
                }
            }
        }
    }

    // Reorder if needed (first chunk only)
    let reordered_segments = if let Some(order) = &all_order {
        info!("Reordering {} segments", order.len());
        // The order applies to the first chunk's indices only; append remaining as-is
        let first_chunk_end = chunks[0].end;
        let mut reordered: Vec<Value> = order
            .iter()
            .filter_map(|&idx| segments.get(idx as usize).cloned())
            .collect();
        reordered.extend(segments[first_chunk_end..].iter().cloned());
        reordered
    } else {
        segments
    };

    // Merge corrected text
    let corrected_segments: Vec<Value> = reordered_segments
        .iter()
        .enumerate()
        .map(|(i, seg)| {
            let text = all_corrected
                .get(i)
                .filter(|s| !s.is_empty())
                .cloned()
                .unwrap_or_else(|| seg.get("text").and_then(|v| v.as_str()).unwrap_or("").to_string());
            json!({
                "text": text,
                "start": seg.get("start").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "end": seg.get("end").and_then(|v| v.as_f64()).unwrap_or(0.0),
                "speaker": seg.get("speaker"),
            })
        })
        .collect();

    Ok(json!({
        "language": transcript_json.get("language").and_then(|v| v.as_str()).unwrap_or(""),
        "segments": corrected_segments,
    }))
}

fn parse_sse_text(body: &str) -> String {
    let mut text_accum = String::new();
    for line in body.lines() {
        if !line.starts_with("data: ") {
            continue;
        }
        let data = &line[6..];
        if let Ok(event) = serde_json::from_str::<Value>(data) {
            let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if event_type == "content_block_delta" {
                if let Some(delta) = event.get("delta") {
                    if delta.get("type").and_then(|v| v.as_str()) == Some("text_delta") {
                        if let Some(t) = delta.get("text").and_then(|v| v.as_str()) {
                            text_accum.push_str(t);
                        }
                    }
                }
            }
        }
    }
    text_accum
}

fn parse_sse_usage(body: &str) -> (u64, u64) {
    let mut input = 0u64;
    let mut output = 0u64;
    for line in body.lines() {
        if !line.starts_with("data: ") {
            continue;
        }
        let data = &line[6..];
        if let Ok(event) = serde_json::from_str::<Value>(data) {
            let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if event_type == "message_start" {
                if let Some(usage) = event.get("message").and_then(|m| m.get("usage")) {
                    input = usage.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                }
            } else if event_type == "message_delta" {
                if let Some(usage) = event.get("usage") {
                    output = usage.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                }
            }
        }
    }
    (input, output)
}

fn parse_correction_result(
    response_text: &str,
    _expected_count: usize,
) -> Result<(Vec<String>, Option<Vec<u64>>), String> {
    let mut text = response_text.to_string();

    // Strip markdown
    if text.starts_with("```json") {
        text = text[7..].to_string();
    }
    if text.starts_with("```") {
        text = text[3..].to_string();
    }
    if text.ends_with("```") {
        text = text[..text.len() - 3].to_string();
    }
    text = text.trim().to_string();
    text = normalize_quotes(&text);

    // Try object format first
    if text.trim_start().starts_with('{') {
        if let Ok(obj) = serde_json::from_str::<Value>(&text) {
            if let (Some(texts), Some(order)) = (
                obj.get("texts").and_then(|v| v.as_array()),
                obj.get("order").and_then(|v| v.as_array()),
            ) {
                let texts: Vec<String> = texts
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                let order: Vec<u64> = order
                    .iter()
                    .filter_map(|v| v.as_u64())
                    .collect();
                if texts.len() == order.len() {
                    return Ok((texts, Some(order)));
                }
                return Ok((texts, None));
            }
        }
    }

    // Find JSON array
    if let Some(start) = text.find('[') {
        let mut depth = 0i32;
        let mut end = None;
        let mut in_string = false;
        let mut escape_next = false;
        for (i, c) in text[start..].char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }
            if c == '\\' {
                escape_next = true;
                continue;
            }
            if c == '"' && !escape_next {
                in_string = !in_string;
                continue;
            }
            if in_string {
                continue;
            }
            if c == '[' {
                depth += 1;
            } else if c == ']' {
                depth -= 1;
                if depth == 0 {
                    end = Some(start + i + 1);
                    break;
                }
            }
        }
        if let Some(end_pos) = end {
            text = text[start..end_pos].to_string();
        }
    }

    match serde_json::from_str::<Vec<String>>(&text) {
        Ok(arr) => Ok((arr, None)),
        Err(e) => Err(format!("Failed to parse corrected JSON: {}", e)),
    }
}
