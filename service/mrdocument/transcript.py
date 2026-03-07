"""Transcript processing module for MrDocument.

Handles transcript correction, PDF generation, and text output for STT results.
"""

import io
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import anthropic

from mrdocument.costs import get_cost_tracker
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

logger = logging.getLogger(__name__)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class TranscriptWord:
    """A single word in the transcript."""
    word: str
    start_time: float
    end_time: float
    speaker_tag: Optional[int] = None


@dataclass
class TranscriptSegment:
    """A segment of the transcript."""
    text: str
    confidence: float
    words: list[TranscriptWord]
    speaker_tag: Optional[int] = None
    start_time: float = 0.0
    end_time: float = 0.0


@dataclass
class TranscriptResult:
    """Complete transcription result."""
    segments: list[TranscriptSegment]
    language_code: str
    error: Optional[str] = None

    def to_text(self, include_speakers: bool = False, include_timestamps: bool = False) -> str:
        """Convert the transcript to plain text."""
        if not include_speakers:
            return " ".join(seg.text for seg in self.segments)

        def format_time(seconds: float) -> str:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            if hours > 0:
                return f"{hours:02d}:{minutes:02d}:{secs:02d}"
            return f"{minutes:02d}:{secs:02d}"

        lines = []
        for segment in self.segments:
            speaker = f"Speaker {segment.speaker_tag}" if segment.speaker_tag else "Speaker"
            if include_timestamps and segment.start_time > 0:
                timestamp = format_time(segment.start_time)
                lines.append(f"[{timestamp}] [{speaker}]: {segment.text}")
            else:
                lines.append(f"[{speaker}]: {segment.text}")

        return "\n\n".join(lines)


# =============================================================================
# JSON Processing
# =============================================================================


def _normalize_quotes(text: str) -> str:
    """Normalize unicode quotation marks to ASCII equivalents."""
    quote_map = {
        '"': '"', '"': '"', '„': '"', '‟': '"', '«': '"', '»': '"',
        ''': "'", ''': "'", '‚': "'", '‛': "'", '‹': "'", '›': "'",
    }
    for unicode_quote, ascii_quote in quote_map.items():
        text = text.replace(unicode_quote, ascii_quote)
    return text


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


def json_to_transcript(data: dict) -> TranscriptResult:
    """Convert JSON dictionary to TranscriptResult."""
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


# =============================================================================
# Correction Prompts
# =============================================================================


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


# =============================================================================
# Correction Logic
# =============================================================================


# Approximate tokens per character (conservative estimate for multilingual)
CHARS_PER_TOKEN = 3


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
                    logger.warning("Expected %d items, got %d", expected_count, len(texts))
                if len(order) != len(texts):
                    logger.warning("Order length %d != texts length %d, ignoring order", len(order), len(texts))
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
                logger.warning(
                    "Response appears truncated (%d chars), recovered by closing array after last complete element",
                    len(truncated),
                )

    response_text = _fix_unescaped_quotes(response_text)

    try:
        result_array = json.loads(response_text)
        if not isinstance(result_array, list):
            raise RuntimeError(f"Expected JSON array, got {type(result_array).__name__}")
        if len(result_array) != expected_count:
            logger.warning("Expected %d items, got %d", expected_count, len(result_array))
        return result_array, None
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse corrected JSON: {e}")


def _estimate_tokens(text: str) -> int:
    """Estimate token count for text."""
    return len(text) // CHARS_PER_TOKEN


def _fix_unescaped_quotes(json_str: str) -> str:
    """Fix unescaped quotes inside JSON string values."""
    try:
        json.loads(json_str)
        return json_str
    except json.JSONDecodeError:
        pass
    
    result = []
    i = 0
    n = len(json_str)
    
    while i < n:
        c = json_str[i]
        
        if c == '"':
            result.append(c)
            i += 1
            
            while i < n:
                c2 = json_str[i]
                
                if c2 == '\\':
                    if i + 1 < n:
                        next_char = json_str[i + 1]
                        result.append(c2)
                        result.append(next_char)
                        i += 2
                    else:
                        result.append(c2)
                        i += 1
                    continue
                
                if c2 == '"':
                    rest = json_str[i+1:i+50]
                    rest_stripped = rest.lstrip(' \t\n\r')
                    
                    is_end = False
                    if not rest_stripped:
                        is_end = True
                    elif rest_stripped[0] in ',}]:':
                        is_end = True
                    
                    if is_end:
                        result.append(c2)
                        i += 1
                        break
                    else:
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
    
    try:
        json.loads(fixed)
        return fixed
    except json.JSONDecodeError:
        return json_str


def _extract_response_text(message) -> str:
    """Extract response text from message, handling thinking blocks."""
    response_text = ""
    for block in message.content:
        if block.type == "text":
            response_text = block.text.strip()
            break
    return response_text


def correct_transcript_json(
    transcript_json: dict,
    api_key: Optional[str] = None,
    model: str = "claude-sonnet-4-20250514",
    extended_thinking: bool = True,
    thinking_budget: int = 50000,
    context: str = "",
    use_batch: bool = True,
    user_dir: Optional[Path] = None,
) -> dict:
    """
    Send transcript text to Anthropic for correction, preserving all metadata.

    Args:
        transcript_json: The transcript JSON (should not include words array).
        api_key: Anthropic API key. If not provided, uses ANTHROPIC_API_KEY env var.
        model: The Anthropic model to use.
        extended_thinking: Enable extended thinking mode.
        thinking_budget: Token budget for extended thinking.
        context: Additional context for correction (names, terms, topic info).
        use_batch: Use batch API (True) or streaming (False).
        user_dir: User directory for cost tracking (optional).

    Returns:
        Corrected transcript JSON with original metadata preserved.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError("Anthropic API key not set")

    client = anthropic.Anthropic(api_key=key)
    
    segments = transcript_json.get("segments", [])
    text_array = [_normalize_quotes(seg.get("text", "")) for seg in segments]
    
    text_json = json.dumps(text_array, indent=2, ensure_ascii=False)
    estimated_tokens = _estimate_tokens(text_json)
    logger.info(
        "Sending %d text segments for correction (~%d tokens, %d chars)",
        len(text_array), estimated_tokens, len(text_json)
    )

    # Build the prompt with optional context
    prompt = CORRECTION_PROMPT_ARRAY
    if context and context.strip():
        prompt += CONTEXT_PROMPT_TEMPLATE.format(context=context.strip())
        logger.info("Including additional context (%d chars)", len(context))

    # Build request parameters
    request_params = {
        "model": model,
        "max_tokens": 64000,
        "messages": [
            {
                "role": "user",
                "content": f"{prompt}\n\nText segments to correct:\n```json\n{text_json}\n```",
            }
        ],
    }

    if extended_thinking:
        request_params["thinking"] = {
            "type": "enabled",
            "budget_tokens": thinking_budget,
        }
        logger.info("Extended thinking enabled (budget: %d tokens)", thinking_budget)

    # Use batch API or streaming based on config
    if use_batch:
        corrected_texts, order = _correct_with_batch(client, request_params, len(text_array), model, user_dir)
    else:
        corrected_texts, order = _correct_with_streaming(client, request_params, len(text_array), model, user_dir)

    # Reorder segments if the LLM moved meta info to the front
    if order is not None:
        logger.info("Reordering %d segments per LLM instruction", len(order))
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


def _correct_with_streaming(
    client: anthropic.Anthropic,
    request_params: dict,
    expected_count: int,
    model: str = "",
    user_dir: Optional[Path] = None,
) -> tuple[list[str], Optional[list[int]]]:
    """Send request via streaming API for synchronous processing.

    Returns:
        Tuple of (corrected_texts, order) where order is None if no reordering.
    """
    logger.info("Using streaming API for transcript correction...")

    try:
        with client.messages.stream(**request_params) as stream:
            message = stream.get_final_message()

        if hasattr(message, "usage"):
            logger.info(
                "Correction complete. Input: %d tokens, Output: %d tokens",
                message.usage.input_tokens, message.usage.output_tokens
            )
            # Track costs
            if user_dir:
                get_cost_tracker().record_anthropic(
                    model=model,
                    input_tokens=message.usage.input_tokens,
                    output_tokens=message.usage.output_tokens,
                    user_dir=user_dir,
                )

        response_text = _extract_response_text(message)
        return _parse_correction_result(response_text, expected_count)

    except anthropic.APIError as e:
        raise RuntimeError(f"Anthropic API error during streaming correction: {e}")


def _correct_with_batch(
    client: anthropic.Anthropic,
    request_params: dict,
    expected_count: int,
    model: str = "",
    user_dir: Optional[Path] = None,
) -> tuple[list[str], Optional[list[int]]]:
    """Send request via Anthropic Batch API for long-running operations.

    Returns:
        Tuple of (corrected_texts, order) where order is None if no reordering.
    """
    logger.info("Using batch API for transcript correction...")

    batch_request = {
        "custom_id": "transcript-correction",
        "params": request_params,
    }

    try:
        batch = client.messages.batches.create(requests=[batch_request])
        batch_id = batch.id
        logger.info("Batch created: %s", batch_id)

        poll_interval = 5
        max_poll_interval = 30

        while True:
            batch_status = client.messages.batches.retrieve(batch_id)
            status = batch_status.processing_status

            if status == "ended":
                logger.info("Batch completed")
                break
            elif status in ("canceling", "canceled"):
                raise RuntimeError("Batch was canceled")
            elif status == "expired":
                raise RuntimeError("Batch expired before completion")
            else:
                counts = batch_status.request_counts
                logger.info(
                    "Batch status: %s (processing: %d, succeeded: %d, errored: %d)",
                    status, counts.processing, counts.succeeded, counts.errored
                )
                time.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, max_poll_interval)

        results = list(client.messages.batches.results(batch_id))

        if not results:
            raise RuntimeError("Batch completed but no results returned")

        result = results[0]

        if result.result.type == "error":
            error = result.result.error
            error_msg = getattr(error, 'message', str(error))
            raise RuntimeError(f"Batch request failed: {error_msg}")

        if result.result.type == "errored":
            error = result.result.error
            if hasattr(error, 'error') and hasattr(error.error, 'message'):
                error_msg = error.error.message
            else:
                error_msg = getattr(error, 'message', str(error))
            raise RuntimeError(f"Batch request errored: {error_msg}")

        if result.result.type != "succeeded":
            raise RuntimeError(f"Unexpected result type: {result.result.type}")

        message = result.result.message

        if hasattr(message, "usage"):
            logger.info(
                "Correction complete. Input: %d tokens, Output: %d tokens",
                message.usage.input_tokens, message.usage.output_tokens
            )
            # Track costs
            if user_dir:
                get_cost_tracker().record_anthropic(
                    model=model,
                    input_tokens=message.usage.input_tokens,
                    output_tokens=message.usage.output_tokens,
                    user_dir=user_dir,
                )

        response_text = _extract_response_text(message)
        return _parse_correction_result(response_text, expected_count)

    except anthropic.APIError as e:
        raise RuntimeError(f"Anthropic API error: {e}")


# =============================================================================
# Output Generation
# =============================================================================


def _count_speakers_in_json(transcript_json: dict) -> int:
    """Count unique speakers in transcript JSON."""
    speakers = set()
    for seg in transcript_json.get("segments", []):
        speaker = seg.get("speaker")
        if speaker is not None:
            speakers.add(speaker)
    return len(speakers)


def format_time(seconds: float) -> str:
    """Format seconds as HH:MM:SS or MM:SS."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def create_text_content(
    transcript_json: dict,
    include_speakers: Optional[bool] = None,
    include_timestamps: Optional[bool] = None,
) -> str:
    """
    Create plain text from transcript JSON.

    Args:
        transcript_json: Transcript as JSON dictionary.
        include_speakers: Include speaker labels. If None, auto-detect.
        include_timestamps: Include timestamps. If None, auto-detect.

    Returns:
        Transcript as plain text string.
    """
    if include_speakers is None or include_timestamps is None:
        multi_speaker = _count_speakers_in_json(transcript_json) > 1
        if include_speakers is None:
            include_speakers = multi_speaker
        if include_timestamps is None:
            include_timestamps = multi_speaker
    
    transcript = json_to_transcript(transcript_json)
    return transcript.to_text(
        include_speakers=include_speakers,
        include_timestamps=include_timestamps,
    )


def create_pdf_bytes(
    transcript_json: dict,
    title: Optional[str] = None,
    include_speakers: Optional[bool] = None,
    include_timestamps: Optional[bool] = None,
) -> bytes:
    """
    Create PDF from transcript JSON and return as bytes.

    Args:
        transcript_json: Transcript as JSON dictionary.
        title: Optional document title.
        include_speakers: Include speaker labels. If None, auto-detect.
        include_timestamps: Include timestamps. If None, auto-detect.

    Returns:
        PDF content as bytes.
    """
    if include_speakers is None or include_timestamps is None:
        multi_speaker = _count_speakers_in_json(transcript_json) > 1
        if include_speakers is None:
            include_speakers = multi_speaker
        if include_timestamps is None:
            include_timestamps = multi_speaker
    
    transcript = json_to_transcript(transcript_json)
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=20 * mm,
        leftMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )

    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        "TranscriptTitle",
        parent=styles["Heading1"],
        fontSize=16,
        spaceAfter=12,
    )

    meta_style = ParagraphStyle(
        "TranscriptMeta",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.grey,
        spaceAfter=20,
    )

    speaker_style = ParagraphStyle(
        "SpeakerLabel",
        parent=styles["Normal"],
        fontSize=10,
        textColor=colors.HexColor("#0066cc"),
        fontName="Helvetica-Bold",
        spaceBefore=12,
        spaceAfter=2,
    )

    text_style = ParagraphStyle(
        "TranscriptText",
        parent=styles["Normal"],
        fontSize=11,
        leading=16,
        spaceAfter=8,
    )

    story = []

    if title:
        story.append(Paragraph(title, title_style))
    
    meta_text = f"Language: {transcript.language_code}"
    if transcript.segments:
        total_duration = max((seg.end_time for seg in transcript.segments if seg.end_time > 0), default=0)
        if total_duration > 0:
            meta_text += f" | Duration: {format_time(total_duration)}"
        
        if include_speakers:
            speakers = set(seg.speaker_tag for seg in transcript.segments if seg.speaker_tag is not None)
            if speakers:
                meta_text += f" | Speakers: {len(speakers)}"
    
    story.append(Paragraph(meta_text, meta_style))

    current_speaker = None
    for segment in transcript.segments:
        if include_speakers:
            speaker_label = segment.speaker_tag
            if speaker_label != current_speaker:
                current_speaker = speaker_label
                speaker_name = f"Speaker {speaker_label}" if speaker_label is not None else "Speaker"
                
                if include_timestamps and segment.start_time > 0:
                    timestamp = format_time(segment.start_time)
                    speaker_text = f"{speaker_name} ({timestamp})"
                else:
                    speaker_text = speaker_name
                
                story.append(Paragraph(speaker_text, speaker_style))

        text = segment.text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        story.append(Paragraph(text, text_style))

    doc.build(story)
    return buffer.getvalue()
