"""Mock STT backend for integration tests.

Provides a mock speech-to-text service that returns canned transcripts
based on audio filename.

Endpoints:
    GET  /health      - Health check
    POST /transcribe  - Mock STT (returns canned transcript)

Usage:
    gunicorn --bind 0.0.0.0:8000 --workers 1 --timeout 60 mock_stt:app
"""

import re

from flask import Flask, jsonify, request

app = Flask("mock_stt")

# Canned transcripts keyed by audio filename stem.
# Each value is the full text that the mock STT "transcribes".
AUDIO_TRANSCRIPTS = {
    "besprechung-intro": (
        "Aufnahme vom fuenfzehnten Maerz zweitausendundfuenfundzwanzig. "
        "Teilnehmer sind Herr Schulze von der Schulze GmbH und Frau Weber "
        "aus der IT-Abteilung. Thema ist die Besprechung zum "
        "IT-Infrastrukturprojekt. "
        "Herr Schulze beginnt. Also, wir haben uns die Server-Infrastruktur "
        "angeschaut und festgestellt, dass die aktuelle Firewall-Konfiguration "
        "nicht mehr den Anforderungen entspricht."
    ),
    "arztgespraech-intro": (
        "Doktor Braun fragt. Herr Mustermann, was fuehrt Sie heute zu mir? "
        "Der Patient antwortet. Ich habe seit etwa zwei Wochen starke "
        "Rueckenschmerzen, besonders im unteren Bereich. "
        "Diese Aufnahme ist vom zehnten April zweitausendundfuenfundzwanzig. "
        "Gespraech zwischen Patient Max Mustermann und Doktor Braun in der "
        "Praxis fuer Allgemeinmedizin."
    ),
    "telefonat": (
        "Guten Tag, hier spricht Sabine Keller von Keller und Partner. "
        "Ich rufe an wegen unseres Angebots fuer die Bueroausstattung."
    ),
    "telefonat-pattern": (
        "Guten Tag, hier ist Herr Schulze von der Schulze GmbH. "
        "Ich moechte den Termin fuer die Lieferung am zwanzigsten September bestaetigen."
    ),
    "videocall": (
        "Guten Morgen zusammen, hier ist die woechentliche Projektbesprechung "
        "vom zwoelften Mai. Herr Fischer von der Fischer AG ist zugeschaltet. "
        "Wir haben die Schnittstelle zum Warenwirtschaftssystem fertiggestellt. "
        "Die ersten Testlaeufe waren erfolgreich."
    ),
    "sprachnachricht": (
        "Hallo Max, hier ist Claudia Mueller. Die Ergebnisse deiner "
        "Hautuntersuchung vom achtzehnten Juni liegen vor. Es handelt sich "
        "um ein harmloses Ekzem. Ich schicke dir den Befund per Post."
    ),
    "sorted-wrongctx-audio": (
        "Guten Tag, hier ist Martin Schulze von der Schulze GmbH. "
        "Ich moechte kurz die Ergebnisse unserer letzten Besprechung "
        "vom zwanzigsten Januar zusammenfassen. Die Serverausstattung "
        "wird wie besprochen im Februar geliefert."
    ),
}


def _make_transcript(text: str) -> dict:
    """Build a minimal STT transcript response with segments."""
    words = text.split()
    # Create one segment per ~20 words
    segments = []
    chunk_size = 20
    t = 0.0
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size])
        start = t
        duration = len(chunk) * 0.05  # ~50ms per char
        end = start + duration
        segments.append({
            "start": round(start, 2),
            "end": round(end, 2),
            "text": chunk,
            "speaker": "SPEAKER_00",
        })
        t = end + 0.5  # 500ms gap between segments
    return {"segments": segments}


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "mock-stt"})


@app.route("/transcribe", methods=["POST"])
def mock_transcribe():
    """Return a canned transcript based on the uploaded filename."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    uploaded = request.files["file"]
    filename = uploaded.filename or "unknown.mp3"
    stem = re.sub(r"\.[^.]+$", "", filename)

    text = AUDIO_TRANSCRIPTS.get(stem, f"Mock transcript for {filename}.")
    transcript = _make_transcript(text)

    return jsonify({"transcript": transcript})
