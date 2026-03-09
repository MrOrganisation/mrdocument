# STT CLI

Command-line tool for speech-to-text transcription using ElevenLabs with Anthropic AI correction.

## Features

- High-quality transcription using ElevenLabs Scribe models
- AI-powered transcript correction using Anthropic Claude
- Speaker diarization (identify who said what)
- Word-level timestamps
- Audio event detection (laughs, applause, music, etc.)
- Automatic audio conversion (supports mp4, m4a, mkv, avi, mov)
- Multiple output formats (JSON, PDF, text)

## Prerequisites

- Python 3.10+
- FFmpeg (for audio conversion)
- ElevenLabs API key
- Anthropic API key (for correction)

## Installation

```bash
# Install FFmpeg (if not already installed)
# macOS
brew install ffmpeg

# Ubuntu/Debian
sudo apt install ffmpeg

# Install the CLI
pip install -e .
```

## Configuration

### API Keys

Set your API keys:

```bash
export ELEVENLABS_API_KEY="your-elevenlabs-key"
export ANTHROPIC_API_KEY="your-anthropic-key"
```

Get your API keys from:
- ElevenLabs: https://elevenlabs.io/app/settings/api-keys
- Anthropic: https://console.anthropic.com/

### View Configuration

```bash
stt config show
```

### Set Configuration

```bash
# Set ElevenLabs model
stt config set --model scribe_v1_experimental

# Set Anthropic model for correction
stt config set --anthropic-model claude-opus-4-20250514

# Set default language
stt config set --language en

# Enable diarization by default
stt config set --diarization

# Enable word timestamps by default
stt config set --timestamps

# Set expected speaker count
stt config set --speakers 3
```

## Usage

### Basic Transcription

```bash
stt transcribe audio.mp3
```

This will:
1. Transcribe with ElevenLabs
2. Correct with Anthropic Claude
3. Output files:
   - `audio_raw.json` - Original transcription
   - `audio.json` - Corrected transcription
   - `audio.pdf` - Formatted PDF
   - `audio.txt` - Plain text

### With Options

```bash
# Specify language
stt transcribe audio.mp3 --language de

# Enable speaker diarization
stt transcribe audio.mp3 --diarization

# Enable word timestamps
stt transcribe audio.mp3 --timestamps

# Combine options
stt transcribe meeting.m4a -d -t -s 4

# Custom output paths
stt transcribe audio.mp3 --json out/result.json --pdf out/result.pdf

# Skip raw JSON output
stt transcribe audio.mp3 --no-raw-json

# Skip Anthropic correction (no raw JSON saved)
stt transcribe audio.mp3 --no-correct

# Only specific outputs
stt transcribe audio.mp3 --no-pdf --no-text --no-raw-json  # Corrected JSON only
```

### Supported Audio Formats

**Direct support:** `.flac`, `.wav`, `.mp3`, `.ogg`, `.webm`

**Auto-converted:** `.mp4`, `.m4a`, `.mkv`, `.avi`, `.mov`

## Pipeline

```
Audio File
    │
    ▼
┌─────────────────┐
│   ElevenLabs    │  Transcription
│   Scribe API    │
└────────┬────────┘
         │ JSON
         ▼
┌─────────────────┐
│   Anthropic     │  Correction
│   Claude Opus   │
└────────┬────────┘
         │ Corrected JSON
         ▼
┌─────────────────┐
│  Output Files   │  JSON, PDF, Text
└─────────────────┘
```

## Models

### ElevenLabs

| Model | Description |
|-------|-------------|
| `scribe_v1` | Standard transcription model (default) |
| `scribe_v1_experimental` | Latest features and improvements |

### Anthropic

| Model | Description |
|-------|-------------|
| `claude-opus-4-20250514` | Most capable model (default) |
| `claude-sonnet-4-20250514` | Faster, more cost-effective |

## Output Formats

### JSON

Structured output with segments, timestamps, and speaker info:

```json
{
  "language": "en",
  "segments": [
    {
      "text": "Hello, how are you today?",
      "start": 0.0,
      "end": 2.5,
      "speaker": 1
    }
  ]
}
```

### PDF

Formatted document with:
- Title and metadata
- Speaker labels with timestamps
- Clean paragraph formatting

### Text

Plain text with optional speaker labels and timestamps:

```
[00:00] [Speaker 1]: Hello, how are you today?

[00:03] [Speaker 2]: I'm doing well, thank you.
```

## Language Codes

Use ISO 639-1 codes (2-letter):

- `en` - English
- `de` - German
- `fr` - French
- `es` - Spanish
- `it` - Italian
- `pt` - Portuguese
- `nl` - Dutch
- `pl` - Polish
- `ja` - Japanese
- `zh` - Chinese
- `ko` - Korean

Full list: https://elevenlabs.io/docs/api-reference/speech-to-text/convert
