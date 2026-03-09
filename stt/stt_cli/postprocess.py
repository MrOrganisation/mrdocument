"""Post-processing module for transcript correction using Anthropic AI."""

import base64
import json
import os
import sys
from typing import Optional

import anthropic

from .transcript import TranscriptResult, TranscriptSegment, TranscriptWord


CORRECTION_PROMPT_ARRAY = """The following JSON array contains text segments from an audio transcription.
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
Return valid JSON with all special characters in strings properly escaped."""

CONTEXT_PROMPT_TEMPLATE = """
Additional context for this transcription:
{context}

Use this context to help identify and correct names, technical terms, and domain-specific vocabulary."""

CORRECTION_PROMPT_TEXT = """The following is a transcript from an audio recording with speaker labels.
Check for errors resulting from wrong transcription.
Common errors include:
- Misheard words or phrases
- Missing or incorrect punctuation
- Grammatical errors introduced by the transcription
- Names or technical terms that may have been transcribed incorrectly

Correct the transcript while preserving:
- All speaker labels exactly as they appear (e.g., [Speaker 1], [Speaker 2])
- The overall structure and paragraph breaks

Only fix clear transcription errors - do not rephrase or restructure the content.

Return ONLY the corrected transcript text, no explanations."""

# Approximate tokens per character (conservative estimate for multilingual)
CHARS_PER_TOKEN = 3
# Leave room for prompt and response
MAX_CHUNK_TOKENS = 150000
MAX_CHUNK_CHARS = MAX_CHUNK_TOKENS * CHARS_PER_TOKEN


def _log(msg: str) -> None:
    """Print status message."""
    print(msg, file=sys.stderr, flush=True)


def _estimate_tokens(text: str) -> int:
    """Estimate token count for text."""
    return len(text) // CHARS_PER_TOKEN


def _extract_response_text(message) -> str:
    """Extract response text from message, handling thinking blocks."""
    response_text = ""
    for block in message.content:
        if block.type == "text":
            response_text = block.text.strip()
            break
    return response_text


def _fix_unescaped_quotes(json_str: str) -> str:
    """
    Fix unescaped quotes inside JSON string values.
    
    Handles cases like:
    - "He said "hello" to me"  -> "He said \"hello\" to me"
    - "He said \"hello" to me" -> "He said \"hello\" to me" (partially escaped)
    - "He said "hello\" to me" -> "He said \"hello\" to me" (partially escaped)
    """
    # First, try to parse as-is
    try:
        json.loads(json_str)
        return json_str  # Already valid
    except json.JSONDecodeError:
        pass
    
    # Strategy: Find string values and fix unescaped quotes within them
    # A string value ends with " followed by optional whitespace and then , or } or ] or :
    
    result = []
    i = 0
    n = len(json_str)
    
    while i < n:
        c = json_str[i]
        
        if c == '"':
            result.append(c)
            i += 1
            
            # Read string content until we find a valid closing quote
            while i < n:
                c2 = json_str[i]
                
                if c2 == '\\':
                    # Check what's being escaped
                    if i + 1 < n:
                        next_char = json_str[i + 1]
                        if next_char == '"':
                            # Escaped quote - check if this looks like end of string
                            # (i.e., the quote after backslash might actually be the closing quote
                            # and the backslash is a mistake)
                            rest = json_str[i+2:i+52]
                            rest_stripped = rest.lstrip(' \t\n\r')
                            
                            if rest_stripped and rest_stripped[0] in ',}]:':
                                # Pattern like: ...\", - the \" is actually end of string
                                # but with erroneous backslash. Keep as escaped quote.
                                result.append(c2)
                                result.append(next_char)
                                i += 2
                            else:
                                # Valid escaped quote inside string
                                result.append(c2)
                                result.append(next_char)
                                i += 2
                        else:
                            # Other escape sequence
                            result.append(c2)
                            result.append(next_char)
                            i += 2
                    else:
                        result.append(c2)
                        i += 1
                    continue
                
                if c2 == '"':
                    # Look ahead to check if this is end of string
                    # Valid end: ", or "} or "] or ": or end of input
                    rest = json_str[i+1:i+50]
                    rest_stripped = rest.lstrip(' \t\n\r')
                    
                    is_end = False
                    if not rest_stripped:
                        # End of input
                        is_end = True
                    elif rest_stripped[0] in ',}]:':
                        is_end = True
                    
                    if is_end:
                        result.append(c2)
                        i += 1
                        break
                    else:
                        # Unescaped quote inside string - escape it
                        result.append('\\')
                        result.append(c2)
                        i += 1
                        continue
                
                result.append(c2)
                i += 1
        else:
            result.append(c)
            i += 1
    
    fixed = ''.join(result)
    
    # Verify fix worked
    try:
        json.loads(fixed)
        return fixed
    except json.JSONDecodeError:
        # If still broken, try a more aggressive approach: 
        # find all \" and " inside strings and normalize them
        return _fix_quotes_aggressive(json_str)


def _fix_quotes_aggressive(json_str: str) -> str:
    """
    More aggressive quote fixing - normalizes all quotes in string values.
    Used as fallback when simple fix doesn't work.
    """
    import re
    
    # Find all string-like patterns: "key": "value"
    # and fix quotes in values
    
    def fix_string_value(match):
        """Fix quotes within a matched string value."""
        full = match.group(0)
        prefix = match.group(1)  # : or [ or , with whitespace
        content = match.group(2)  # string content without outer quotes
        
        # Remove any existing escapes on quotes, then re-escape all
        # This normalizes partially escaped quotes
        content = content.replace('\\"', '"')  # unescape
        content = content.replace('"', '\\"')   # re-escape all
        
        return f'{prefix}"{content}"'
    
    # Match string values (after : or in arrays)
    # This pattern finds ": " followed by a quoted string
    # The string ends at " followed by , or } or ] or newline
    
    # Process line by line for "text" fields which are most problematic
    lines = json_str.split('\n')
    fixed_lines = []
    
    for line in lines:
        # Look for "text": "..." pattern
        text_match = re.match(r'^(\s*"text"\s*:\s*)"(.*)(",?\s*)$', line)
        if text_match:
            prefix = text_match.group(1)
            content = text_match.group(2)
            suffix = text_match.group(3)
            
            # Normalize quotes in content
            # First unescape, then re-escape
            content = content.replace('\\"', '\x00')  # temp placeholder
            content = content.replace('"', '\\"')      # escape unescaped
            content = content.replace('\x00', '\\"')   # restore escaped
            
            line = f'{prefix}"{content}{suffix}'
        
        fixed_lines.append(line)
    
    fixed = '\n'.join(fixed_lines)
    
    try:
        json.loads(fixed)
        return fixed
    except json.JSONDecodeError:
        return json_str  # Give up, return original


def _extract_response_json(message, expect_array: bool = False) -> str:
    """Extract JSON response text from message, handling thinking blocks."""
    response_text = _extract_response_text(message)
    
    # Remove markdown code block if present
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.startswith("```"):
        response_text = response_text[3:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]
    response_text = response_text.strip()
    
    # Normalize unicode quotes that might have been introduced
    response_text = _normalize_quotes(response_text)
    
    # Determine what to look for: array or object
    if expect_array:
        open_char, close_char = '[', ']'
    else:
        open_char, close_char = '{', '}'
    
    # Try to find complete JSON structure if response was truncated
    start = response_text.find(open_char)
    if start != -1:
        # Find matching closing bracket by counting
        depth = 0
        end = -1
        in_string = False
        escape_next = False
        for i, char in enumerate(response_text[start:], start):
            if escape_next:
                escape_next = False
                continue
            if char == '\\':
                escape_next = True
                continue
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == open_char:
                depth += 1
            elif char == close_char:
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        
        if end != -1:
            response_text = response_text[start:end]
    
    # Fix unescaped quotes inside strings
    response_text = _fix_unescaped_quotes(response_text)
    
    return response_text


def correct_json_light(
    transcript_json: dict,
    api_key: Optional[str] = None,
    model: str = "claude-sonnet-4-20250514",
    extended_thinking: bool = True,
    thinking_budget: int = 50000,
    use_batch: bool = True,
    context: str = "",
) -> dict:
    """
    Send transcript text to Anthropic for correction, preserving all metadata.

    Only the text content is sent to Anthropic as a simple array of strings.
    This prevents the model from modifying speaker labels, timestamps, or other metadata.
    The corrected text is then merged back with the original metadata.

    Args:
        transcript_json: The transcript JSON (should not include words array).
        api_key: Anthropic API key. If not provided, uses ANTHROPIC_API_KEY env var.
        model: The Anthropic model to use.
        extended_thinking: Enable extended thinking mode.
        thinking_budget: Token budget for extended thinking.
        use_batch: Use batch API for long-running requests.
        context: Additional context for correction (names, terms, topic info).

    Returns:
        Corrected transcript JSON with original metadata preserved.

    Raises:
        ValueError: If API key is not set.
        RuntimeError: If API call fails.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError(
            "Anthropic API key not set. Set ANTHROPIC_API_KEY environment variable "
            "or pass api_key parameter."
        )

    client = anthropic.Anthropic(api_key=key)
    
    # Extract only text from segments
    segments = transcript_json.get("segments", [])
    text_array = [_normalize_quotes(seg.get("text", "")) for seg in segments]
    
    text_json = json.dumps(text_array, indent=2, ensure_ascii=False)
    estimated_tokens = _estimate_tokens(text_json)
    _log(f"[Anthropic] Sending {len(text_array)} text segments (~{estimated_tokens} tokens, {len(text_json)} chars)")

    # Build the prompt with optional context
    prompt = CORRECTION_PROMPT_ARRAY
    if context and context.strip():
        prompt += CONTEXT_PROMPT_TEMPLATE.format(context=context.strip())
        _log(f"[Anthropic] Including additional context ({len(context)} chars)")

    # Build request parameters - send only the text array
    request_params = {
        "model": model,
        "max_tokens": 64000,  # Large enough for full response
        "messages": [
            {
                "role": "user",
                "content": f"{prompt}\n\nText segments to correct:\n```json\n{text_json}\n```",
            }
        ],
    }

    # Add extended thinking if enabled
    if extended_thinking:
        request_params["thinking"] = {
            "type": "enabled",
            "budget_tokens": thinking_budget,
        }
        _log(f"[Anthropic] Extended thinking enabled (budget: {thinking_budget} tokens)")

    if use_batch:
        corrected_texts, order = _correct_array_with_batch(client, request_params, len(text_array))
    else:
        corrected_texts, order = _correct_array_direct(client, request_params, len(text_array))

    # Reorder segments if the LLM moved meta info to the front
    if order is not None:
        _log(f"[Anthropic] Reordering {len(order)} segments per LLM instruction")
        reordered_segments = [segments[idx] for idx in order if idx < len(segments)]
    else:
        reordered_segments = segments

    # Merge corrected text with (possibly reordered) metadata
    corrected_segments = []
    for i, seg in enumerate(reordered_segments):
        corrected_seg = {
            "text": corrected_texts[i] if i < len(corrected_texts) else seg.get("text", ""),
            "start": seg.get("start", 0.0),
            "end": seg.get("end", 0.0),
            "speaker": seg.get("speaker"),
        }
        corrected_segments.append(corrected_seg)

    return {
        "language": transcript_json.get("language", ""),
        "segments": corrected_segments,
    }


def _correct_array_direct(client: anthropic.Anthropic, request_params: dict, expected_count: int) -> tuple[list[str], Optional[list[int]]]:
    """Send direct (non-batch) streaming request to Anthropic, expecting an array response.

    Returns:
        Tuple of (corrected_texts, order) where order is None if no reordering.
    """
    _log("[Anthropic] Using streaming API...")

    response_text = ""
    thinking_text = ""
    current_block_type = None
    input_tokens = 0
    output_tokens = 0
    last_thinking_log = 0
    last_response_log = 0

    try:
        with client.messages.stream(**request_params) as stream:
            for event in stream:
                if hasattr(event, 'type'):
                    if event.type == 'content_block_start':
                        # Track what type of block we're in
                        if hasattr(event, 'content_block'):
                            current_block_type = getattr(event.content_block, 'type', None)
                            if current_block_type == 'thinking':
                                _log("[Anthropic] Thinking...")
                            elif current_block_type == 'text':
                                if thinking_text:
                                    _log(f"[Anthropic] Thinking complete ({len(thinking_text)} chars)")
                                _log("[Anthropic] Generating response...")

                    elif event.type == 'content_block_delta':
                        if current_block_type == 'thinking':
                            if hasattr(event.delta, 'thinking'):
                                thinking_text += event.delta.thinking
                                # Log thinking progress every 2000 chars
                                if len(thinking_text) - last_thinking_log >= 2000:
                                    _log(f"[Anthropic] Thinking... {len(thinking_text)} chars")
                                    last_thinking_log = len(thinking_text)
                        elif current_block_type == 'text':
                            if hasattr(event.delta, 'text'):
                                response_text += event.delta.text
                                # Log response progress every 1000 chars
                                if len(response_text) - last_response_log >= 1000:
                                    _log(f"[Anthropic] Response... {len(response_text)} chars")
                                    last_response_log = len(response_text)

                    elif event.type == 'message_delta':
                        if hasattr(event, 'usage') and event.usage:
                            output_tokens = getattr(event.usage, 'output_tokens', 0)

                    elif event.type == 'message_start':
                        if hasattr(event, 'message') and hasattr(event.message, 'usage'):
                            input_tokens = getattr(event.message.usage, 'input_tokens', 0)

            # Get final message for complete usage stats
            final_message = stream.get_final_message()
            if hasattr(final_message, 'usage'):
                input_tokens = final_message.usage.input_tokens
                output_tokens = final_message.usage.output_tokens

    except anthropic.APIError as e:
        raise RuntimeError(f"Anthropic API error: {e}")

    stats = f"Input: {input_tokens} tokens, Output: {output_tokens} tokens"
    if thinking_text:
        stats += f", Thinking: {len(thinking_text)} chars"
    _log(f"[Anthropic] Stream complete. {stats}")

    return _parse_correction_result(response_text.strip(), expected_count)


def _correct_array_with_batch(client: anthropic.Anthropic, request_params: dict, expected_count: int) -> tuple[list[str], Optional[list[int]]]:
    """Send request via Anthropic Batch API for long-running operations, expecting array response.

    Returns:
        Tuple of (corrected_texts, order) where order is None if no reordering.
    """
    import time

    _log("[Anthropic] Using batch API for long-running request...")

    # Create batch request
    batch_request = {
        "custom_id": "transcript-correction",
        "params": request_params,
    }

    try:
        # Create the batch
        batch = client.messages.batches.create(
            requests=[batch_request]
        )
        batch_id = batch.id
        _log(f"[Anthropic] Batch created: {batch_id}")

        # Poll for completion
        poll_interval = 5  # Start with 5 seconds
        max_poll_interval = 30  # Max 30 seconds between polls

        while True:
            batch_status = client.messages.batches.retrieve(batch_id)
            status = batch_status.processing_status

            if status == "ended":
                _log(f"[Anthropic] Batch completed")
                break
            elif status == "canceling" or status == "canceled":
                raise RuntimeError(f"Batch was canceled")
            elif status == "expired":
                raise RuntimeError(f"Batch expired before completion")
            else:
                # in_progress or other status
                counts = batch_status.request_counts
                _log(f"[Anthropic] Batch status: {status} (processing: {counts.processing}, succeeded: {counts.succeeded}, errored: {counts.errored})")
                time.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, max_poll_interval)

        # Retrieve results
        results = list(client.messages.batches.results(batch_id))

        if not results:
            raise RuntimeError("Batch completed but no results returned")

        result = results[0]

        if result.result.type == "error":
            error = result.result.error
            error_msg = getattr(error, 'message', None) or getattr(getattr(error, 'error', None), 'message', str(error))
            error_type = getattr(error, 'type', 'unknown')
            raise RuntimeError(f"Batch request failed: {error_type} - {error_msg}")

        if result.result.type == "errored":
            error = result.result.error
            # Handle nested error structure: ErrorResponse.error.message
            if hasattr(error, 'error') and hasattr(error.error, 'message'):
                error_msg = error.error.message
                error_type = getattr(error.error, 'type', 'unknown')
            else:
                error_msg = getattr(error, 'message', str(error))
                error_type = getattr(error, 'type', 'unknown')
            raise RuntimeError(f"Batch request errored: {error_type} - {error_msg}")

        if result.result.type != "succeeded":
            # Try to get more details
            error_details = ""
            if hasattr(result.result, "error") and result.result.error:
                error_details = f": {result.result.error}"
            raise RuntimeError(f"Unexpected result type: {result.result.type}{error_details}")

        message = result.result.message

        # Log usage info
        if hasattr(message, "usage"):
            _log(f"[Anthropic] Input tokens: {message.usage.input_tokens}, Output tokens: {message.usage.output_tokens}")

        response_text = _extract_response_text(message)
        return _parse_correction_result(response_text, expected_count)

    except anthropic.APIError as e:
        raise RuntimeError(f"Anthropic API error: {e}")


def _correct_single(
    client: anthropic.Anthropic,
    transcript_json: dict,
    model: str,
    extended_thinking: bool = True,
    thinking_budget: int = 50000,
) -> dict:
    """
    Correct transcript in a single API call.

    Args:
        client: Anthropic client.
        transcript_json: Full transcript JSON.
        model: Model to use.
        extended_thinking: Enable extended thinking mode.
        thinking_budget: Token budget for thinking (if enabled).

    Returns:
        Corrected transcript JSON.
    """
    json_str = json.dumps(transcript_json, indent=2, ensure_ascii=False)
    json_bytes = json_str.encode("utf-8")
    json_b64 = base64.standard_b64encode(json_bytes).decode("ascii")

    # Build request parameters
    request_params = {
        "model": model,
        "max_tokens": 128000,  # Large output for full transcript
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "text/plain",
                            "data": json_b64,
                        },
                        "cache_control": {"type": "ephemeral"},
                    },
                    {
                        "type": "text",
                        "text": CORRECTION_PROMPT,
                    },
                ],
            }
        ],
    }

    # Add extended thinking if enabled
    if extended_thinking:
        request_params["thinking"] = {
            "type": "enabled",
            "budget_tokens": thinking_budget,
        }
        _log(f"[Anthropic] Extended thinking enabled (budget: {thinking_budget} tokens)")

    message = client.messages.create(**request_params)

    # Log usage info
    if hasattr(message, "usage"):
        _log(f"[Anthropic] Input tokens: {message.usage.input_tokens}, Output tokens: {message.usage.output_tokens}")

    response_text = _extract_response_json(message)
    return json.loads(response_text)


def _correct_chunk(
    client: anthropic.Anthropic,
    segments: list[dict],
    language: str,
    model: str,
) -> list[dict]:
    """
    Correct a chunk of segments.

    Args:
        client: Anthropic client.
        segments: List of segment dictionaries.
        language: Language code.
        model: Model to use.

    Returns:
        Corrected segments.
    """
    chunk_json = {"language": language, "segments": segments}
    json_str = json.dumps(chunk_json, indent=2, ensure_ascii=False)

    # Send as base64-encoded document attachment
    json_bytes = json_str.encode("utf-8")
    json_b64 = base64.standard_b64encode(json_bytes).decode("ascii")

    message = client.messages.create(
        model=model,
        max_tokens=16384,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "text/plain",
                            "data": json_b64,
                        },
                        "cache_control": {"type": "ephemeral"},
                    },
                    {
                        "type": "text",
                        "text": CORRECTION_PROMPT_JSON,
                    },
                ],
            }
        ],
    )

    response_text = _extract_response_json(message)
    corrected = json.loads(response_text)
    return corrected.get("segments", segments)


def correct_transcript(
    transcript_json: dict,
    api_key: Optional[str] = None,
    model: str = "claude-sonnet-4-20250514",
    extended_thinking: bool = True,
    thinking_budget: int = 50000,
    auto_chunk: bool = True,
    on_progress: Optional[callable] = None,
) -> dict:
    """
    Send transcript JSON to Anthropic for correction.

    Args:
        transcript_json: The transcript data as a dictionary.
        api_key: Anthropic API key. If not provided, uses ANTHROPIC_API_KEY env var.
        model: The Anthropic model to use.
        extended_thinking: Enable extended thinking mode (model chooses strategy).
        thinking_budget: Token budget for extended thinking.
        auto_chunk: Automatically chunk large transcripts (disable to let model handle it).
        on_progress: Optional callback(current, total) for progress updates.

    Returns:
        Corrected transcript JSON as dictionary.

    Raises:
        ValueError: If API key is not set.
        RuntimeError: If API call fails or response is invalid.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError(
            "Anthropic API key not set. Set ANTHROPIC_API_KEY environment variable "
            "or pass api_key parameter."
        )

    client = anthropic.Anthropic(api_key=key)
    segments = transcript_json.get("segments", [])
    language = transcript_json.get("language", "")

    if not segments:
        return transcript_json

    full_json = json.dumps(transcript_json, ensure_ascii=False)
    estimated_tokens = _estimate_tokens(full_json)
    _log(f"[Anthropic] Transcript size: ~{estimated_tokens} tokens")

    # If extended thinking or chunking disabled, try single request
    if extended_thinking or not auto_chunk:
        if on_progress:
            on_progress(0, 1)
        try:
            _log("[Anthropic] Sending full transcript in single request...")
            corrected = _correct_single(
                client,
                transcript_json,
                model,
                extended_thinking=extended_thinking,
                thinking_budget=thinking_budget,
            )
            if on_progress:
                on_progress(1, 1)
            return corrected
        except anthropic.APIError as e:
            if auto_chunk and "too long" in str(e).lower():
                _log(f"[Anthropic] Request too large, falling back to chunking...")
            else:
                raise RuntimeError(f"Anthropic API error: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse corrected JSON: {e}")

    # Check if we need to chunk
    if estimated_tokens < MAX_CHUNK_TOKENS:
        # Small enough to process in one go
        if on_progress:
            on_progress(0, 1)
        try:
            corrected_segments = _correct_chunk(client, segments, language, model)
            if on_progress:
                on_progress(1, 1)
            return {"language": language, "segments": corrected_segments}
        except anthropic.APIError as e:
            raise RuntimeError(f"Anthropic API error: {e}")
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse corrected JSON: {e}")

    # Need to chunk - split segments into batches
    _log(f"[Anthropic] Transcript too large, splitting into chunks...")

    chunks: list[list[dict]] = []
    current_chunk: list[dict] = []
    current_size = 0

    for segment in segments:
        segment_json = json.dumps(segment, ensure_ascii=False)
        segment_size = len(segment_json)

        if current_size + segment_size > MAX_CHUNK_CHARS and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0

        current_chunk.append(segment)
        current_size += segment_size

    if current_chunk:
        chunks.append(current_chunk)

    _log(f"[Anthropic] Processing {len(chunks)} chunks...")

    # Process each chunk
    corrected_segments: list[dict] = []
    for i, chunk in enumerate(chunks):
        if on_progress:
            on_progress(i, len(chunks))
        _log(f"[Anthropic] Processing chunk {i + 1}/{len(chunks)} ({len(chunk)} segments)...")

        try:
            corrected = _correct_chunk(client, chunk, language, model)
            corrected_segments.extend(corrected)
        except anthropic.APIError as e:
            _log(f"[Anthropic] Chunk {i + 1} failed: {e}, keeping original")
            corrected_segments.extend(chunk)
        except json.JSONDecodeError as e:
            _log(f"[Anthropic] Chunk {i + 1} parse error: {e}, keeping original")
            corrected_segments.extend(chunk)

    if on_progress:
        on_progress(len(chunks), len(chunks))

    return {"language": language, "segments": corrected_segments}


def json_to_transcript(data: dict) -> TranscriptResult:
    """
    Convert JSON dictionary to TranscriptResult.

    Args:
        data: JSON dictionary with language and segments.

    Returns:
        TranscriptResult object.
    """
    segments = []
    for seg_data in data.get("segments", []):
        words = []
        for word_data in seg_data.get("words", []):
            words.append(TranscriptWord(
                word=word_data.get("word", ""),
                start_time=word_data.get("start", 0.0),
                end_time=word_data.get("end", 0.0),
                speaker_tag=word_data.get("speaker"),
            ))

        segments.append(TranscriptSegment(
            text=seg_data.get("text", ""),
            confidence=seg_data.get("confidence", 1.0),
            words=words,
            speaker_tag=seg_data.get("speaker"),
            start_time=seg_data.get("start", 0.0),
            end_time=seg_data.get("end", 0.0),
        ))

    return TranscriptResult(
        segments=segments,
        language_code=data.get("language", ""),
    )


def _normalize_quotes(text: str) -> str:
    """
    Normalize unicode quotation marks to ASCII equivalents.
    
    This prevents confusion when the text is embedded in JSON and
    sent to LLMs which might misinterpret unicode quotes as JSON delimiters.
    """
    # Common unicode quote characters to normalize
    quote_map = {
        '"': '"',  # left double quotation mark
        '"': '"',  # right double quotation mark
        '„': '"',  # double low-9 quotation mark (German opening)
        '‟': '"',  # double high-reversed-9 quotation mark
        '«': '"',  # left-pointing double angle quotation mark
        '»': '"',  # right-pointing double angle quotation mark
        ''': "'",  # left single quotation mark
        ''': "'",  # right single quotation mark
        '‚': "'",  # single low-9 quotation mark
        '‛': "'",  # single high-reversed-9 quotation mark
        '‹': "'",  # single left-pointing angle quotation mark
        '›': "'",  # single right-pointing angle quotation mark
    }
    for unicode_quote, ascii_quote in quote_map.items():
        text = text.replace(unicode_quote, ascii_quote)
    return text


def _parse_correction_result(
    response_text: str, expected_count: int
) -> tuple[list[str], Optional[list[int]]]:
    """Parse correction response, handling both plain array and reordered object formats.

    Returns:
        Tuple of (corrected_texts, order) where order is None if no reordering.
    """
    # Strip markdown code blocks
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.startswith("```"):
        response_text = response_text[3:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]
    response_text = response_text.strip()
    response_text = _normalize_quotes(response_text)

    # Try parsing as JSON object first (reordered format)
    if response_text.lstrip().startswith("{"):
        try:
            obj = json.loads(response_text)
            if isinstance(obj, dict) and "texts" in obj and "order" in obj:
                texts = obj["texts"]
                order = obj["order"]
                if not isinstance(texts, list) or not isinstance(order, list):
                    raise RuntimeError("'texts' and 'order' must be arrays")
                if len(texts) != expected_count:
                    _log(f"[Anthropic] Warning: Expected {expected_count} items, got {len(texts)}")
                if len(order) != len(texts):
                    _log(f"[Anthropic] Warning: Order length {len(order)} != texts length {len(texts)}, ignoring order")
                    return texts, None
                return texts, order
        except json.JSONDecodeError:
            pass  # Fall through to array parsing

    # Find JSON array
    start = response_text.find("[")
    if start != -1:
        depth = 0
        end = -1
        in_string = False
        escape_next = False
        for i, char in enumerate(response_text[start:], start):
            if escape_next:
                escape_next = False
                continue
            if char == "\\":
                escape_next = True
                continue
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        if end != -1:
            response_text = response_text[start:end]
        else:
            # Response likely truncated — try to recover by closing at last complete element
            truncated = response_text[start:]
            last_sep = truncated.rfind('",')
            if last_sep == -1:
                last_sep = truncated.rfind('" ,')
            if last_sep != -1:
                response_text = truncated[: last_sep + 1] + "]"
                _log(f"[Anthropic] Warning: Response appears truncated ({len(truncated)} chars), recovered by closing array after last complete element")

    response_text = _fix_unescaped_quotes(response_text)

    try:
        result_array = json.loads(response_text)
        if not isinstance(result_array, list):
            raise RuntimeError(f"Expected JSON array, got {type(result_array).__name__}")
        if len(result_array) != expected_count:
            _log(f"[Anthropic] Warning: Expected {expected_count} items, got {len(result_array)}")
        return result_array, None
    except json.JSONDecodeError as e:
        debug_file = "/tmp/anthropic_response_debug.txt"
        with open(debug_file, "w") as f:
            f.write(response_text)
        _log(f"[Anthropic] Raw response saved to {debug_file} for debugging")
        raise RuntimeError(f"Failed to parse corrected JSON: {e}\nResponse saved to {debug_file}")


def strip_words_from_json(transcript_json: dict, normalize_quotes: bool = True) -> dict:
    """
    Create a copy of transcript JSON without the words arrays.

    Args:
        transcript_json: Full transcript JSON with words.
        normalize_quotes: Normalize unicode quotes to ASCII.

    Returns:
        JSON without words arrays (smaller for API calls).
    """
    segments = []
    for seg in transcript_json.get("segments", []):
        text = seg.get("text", "")
        if normalize_quotes:
            text = _normalize_quotes(text)
        segments.append({
            "text": text,
            "start": seg.get("start", 0.0),
            "end": seg.get("end", 0.0),
            "speaker": seg.get("speaker"),
        })
    
    return {
        "language": transcript_json.get("language", ""),
        "segments": segments,
    }


def transcript_to_json(transcript: TranscriptResult, include_words: bool = False) -> dict:
    """
    Convert TranscriptResult to JSON dictionary.

    Args:
        transcript: The transcript result.
        include_words: Include word-level details.

    Returns:
        JSON-serializable dictionary.
    """
    segments_data = []
    for seg in transcript.segments:
        seg_dict = {
            "text": seg.text,
            "start": seg.start_time,
            "end": seg.end_time,
            "speaker": seg.speaker_tag,
        }
        if include_words and seg.words:
            seg_dict["words"] = [
                {
                    "word": w.word,
                    "start": w.start_time,
                    "end": w.end_time,
                    "speaker": w.speaker_tag,
                }
                for w in seg.words
            ]
        segments_data.append(seg_dict)

    return {
        "language": transcript.language_code,
        "segments": segments_data,
    }
