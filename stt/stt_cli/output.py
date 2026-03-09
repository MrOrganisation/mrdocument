"""Output formatters for transcript results."""

import io
import json
from pathlib import Path
from typing import Optional, Union

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

from .transcript import TranscriptResult, TranscriptSegment, TranscriptWord


def format_time(seconds: float) -> str:
    """Format seconds as HH:MM:SS or MM:SS."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _count_speakers_in_json(transcript_json: dict) -> int:
    """Count unique speakers in transcript JSON."""
    speakers = set()
    for seg in transcript_json.get("segments", []):
        speaker = seg.get("speaker")
        if speaker is not None:
            speakers.add(speaker)
    return len(speakers)


def write_json(
    transcript: TranscriptResult,
    output_path: Path,
    include_words: bool = False,
) -> None:
    """
    Write transcript to JSON file.

    Args:
        transcript: The transcript result.
        output_path: Path to write JSON file.
        include_words: Include word-level details.
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

    output_data = {
        "language": transcript.language_code,
        "segments": segments_data,
    }

    output_path.write_text(json.dumps(output_data, indent=2, ensure_ascii=False))


def write_pdf(
    transcript: TranscriptResult,
    output_path: Path,
    title: Optional[str] = None,
    include_speakers: bool = True,
    include_timestamps: bool = True,
) -> None:
    """
    Write transcript to PDF file.

    Args:
        transcript: The transcript result.
        output_path: Path to write PDF file.
        title: Optional document title.
        include_speakers: Include speaker labels.
        include_timestamps: Include timestamps in output.
    """
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=20 * mm,
        leftMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )

    # Define styles
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

    # Build document content
    story = []

    # Title
    if title:
        story.append(Paragraph(title, title_style))
    
    # Metadata
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

    # Transcript segments
    current_speaker = None
    for segment in transcript.segments:
        if include_speakers:
            # Speaker label (only when speaker changes)
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

        # Segment text
        # Escape special XML characters for reportlab
        text = segment.text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        story.append(Paragraph(text, text_style))

    doc.build(story)


def write_text(
    transcript: TranscriptResult,
    output_path: Path,
    include_speakers: bool = True,
    include_timestamps: bool = True,
) -> None:
    """
    Write transcript to plain text file.

    Args:
        transcript: The transcript result.
        output_path: Path to write text file.
        include_speakers: Include speaker labels.
        include_timestamps: Include timestamps.
    """
    text = transcript.to_text(
        include_speakers=include_speakers,
        include_timestamps=include_timestamps,
    )
    output_path.write_text(text)


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


def write_pdf_from_json(
    transcript_json: dict,
    output_path: Path,
    title: Optional[str] = None,
    include_speakers: Optional[bool] = None,
    include_timestamps: Optional[bool] = None,
) -> None:
    """
    Write transcript JSON to PDF file.

    Args:
        transcript_json: Transcript as JSON dictionary.
        output_path: Path to write PDF file.
        title: Optional document title.
        include_speakers: Include speaker labels. If None, auto-detect based on speaker count.
        include_timestamps: Include timestamps in output. If None, auto-detect based on speaker count.
    """
    # Auto-detect: only include speakers/timestamps if multiple speakers
    if include_speakers is None or include_timestamps is None:
        multi_speaker = _count_speakers_in_json(transcript_json) > 1
        if include_speakers is None:
            include_speakers = multi_speaker
        if include_timestamps is None:
            include_timestamps = multi_speaker
    
    transcript = json_to_transcript(transcript_json)
    write_pdf(transcript, output_path, title=title, include_speakers=include_speakers, include_timestamps=include_timestamps)


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
        include_speakers: Include speaker labels. If None, auto-detect based on speaker count.
        include_timestamps: Include timestamps in output. If None, auto-detect based on speaker count.

    Returns:
        PDF content as bytes.
    """
    # Auto-detect: only include speakers/timestamps if multiple speakers
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

    # Define styles
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

    # Build document content
    story = []

    # Title
    if title:
        story.append(Paragraph(title, title_style))
    
    # Metadata
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

    # Transcript segments
    current_speaker = None
    for segment in transcript.segments:
        if include_speakers:
            # Speaker label (only when speaker changes)
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

        # Segment text
        # Escape special XML characters for reportlab
        text = segment.text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        story.append(Paragraph(text, text_style))

    doc.build(story)
    return buffer.getvalue()


def write_text_from_json(
    transcript_json: dict,
    output_path: Path,
    include_speakers: Optional[bool] = None,
    include_timestamps: Optional[bool] = None,
) -> None:
    """
    Write transcript JSON to plain text file.

    Args:
        transcript_json: Transcript as JSON dictionary.
        output_path: Path to write text file.
        include_speakers: Include speaker labels. If None, auto-detect based on speaker count.
        include_timestamps: Include timestamps in output. If None, auto-detect based on speaker count.
    """
    # Auto-detect: only include speakers/timestamps if multiple speakers
    if include_speakers is None or include_timestamps is None:
        multi_speaker = _count_speakers_in_json(transcript_json) > 1
        if include_speakers is None:
            include_speakers = multi_speaker
        if include_timestamps is None:
            include_timestamps = multi_speaker
    
    transcript = json_to_transcript(transcript_json)
    write_text(transcript, output_path, include_speakers=include_speakers, include_timestamps=include_timestamps)


def create_text_content(
    transcript_json: dict,
    include_speakers: Optional[bool] = None,
    include_timestamps: Optional[bool] = None,
) -> str:
    """
    Create plain text from transcript JSON.

    Args:
        transcript_json: Transcript as JSON dictionary.
        include_speakers: Include speaker labels. If None, auto-detect based on speaker count.
        include_timestamps: Include timestamps in output. If None, auto-detect based on speaker count.

    Returns:
        Transcript as plain text string.
    """
    # Auto-detect: only include speakers/timestamps if multiple speakers
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
