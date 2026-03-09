"""Shared transcript data structures."""

from dataclasses import dataclass


@dataclass
class TranscriptWord:
    """A single word in the transcript."""

    word: str
    start_time: float
    end_time: float
    speaker_tag: int | None = None


@dataclass
class TranscriptSegment:
    """A segment of the transcript."""

    text: str
    confidence: float
    words: list[TranscriptWord]
    speaker_tag: int | None = None
    start_time: float = 0.0
    end_time: float = 0.0


@dataclass
class TranscriptResult:
    """Complete transcription result."""

    segments: list[TranscriptSegment]
    language_code: str
    error: str | None = None

    def get_all_words(self) -> list[TranscriptWord]:
        """Get all words from all segments in order."""
        all_words = []
        for segment in self.segments:
            all_words.extend(segment.words)
        return all_words

    def aggregate_by_speaker(self) -> "TranscriptResult":
        """
        Aggregate words into segments by speaker changes.

        Creates new segments where each segment contains all consecutive
        words by the same speaker, with proper start/end timestamps.
        """
        all_words = self.get_all_words()
        if not all_words:
            return self

        new_segments = []
        current_speaker = all_words[0].speaker_tag
        current_words = []

        for word in all_words:
            if word.speaker_tag != current_speaker:
                # Speaker changed - create segment for previous speaker
                if current_words:
                    new_segments.append(TranscriptSegment(
                        text=" ".join(w.word for w in current_words),
                        confidence=0.0,
                        words=current_words,
                        speaker_tag=current_speaker,
                        start_time=current_words[0].start_time,
                        end_time=current_words[-1].end_time,
                    ))
                current_speaker = word.speaker_tag
                current_words = [word]
            else:
                current_words.append(word)

        # Don't forget the last segment
        if current_words:
            new_segments.append(TranscriptSegment(
                text=" ".join(w.word for w in current_words),
                confidence=0.0,
                words=current_words,
                speaker_tag=current_speaker,
                start_time=current_words[0].start_time,
                end_time=current_words[-1].end_time,
            ))

        return TranscriptResult(
            segments=new_segments,
            language_code=self.language_code,
            error=self.error,
        )

    def to_text(self, include_speakers: bool = False, include_timestamps: bool = False) -> str:
        """
        Convert the transcript to plain text.

        Args:
            include_speakers: Include speaker labels.
            include_timestamps: Include timestamps (requires include_speakers).
        """
        if not include_speakers:
            return " ".join(seg.text for seg in self.segments)

        def format_time(seconds: float) -> str:
            """Format seconds as HH:MM:SS or MM:SS."""
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
