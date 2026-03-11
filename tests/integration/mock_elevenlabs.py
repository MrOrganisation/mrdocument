"""Mock ElevenLabs STT backend for integration tests.

Returns canned transcripts in ElevenLabs API response format
(word-level data with speaker diarization).

Endpoints:
    GET  /health              - Health check
    POST /v1/speech-to-text   - Mock transcription (ElevenLabs format)

Usage:
    gunicorn --bind 0.0.0.0:8080 --workers 1 --timeout 60 mock_elevenlabs:app
"""

import re

from flask import Flask, jsonify, request

app = Flask("mock_elevenlabs")

# Canned transcripts keyed by audio filename stem.
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


def _make_elevenlabs_response(text, language_code="deu"):
    """Build an ElevenLabs-format response with word-level data."""
    raw_words = text.split()
    words = []
    t = 0.0
    for w in raw_words:
        duration = len(w) * 0.05  # ~50ms per character
        words.append({
            "text": w,
            "start": round(t, 3),
            "end": round(t + duration, 3),
            "type": "word",
            "speaker_id": "speaker_0",
        })
        t += duration + 0.08  # small gap between words

    return {
        "language_code": language_code,
        "text": text,
        "words": words,
    }


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "mock-elevenlabs"})


@app.route("/v1/speech-to-text", methods=["POST"])
def mock_speech_to_text():
    """Return a canned transcript in ElevenLabs API response format."""
    if "file" not in request.files:
        return jsonify({"detail": {"message": "No file provided"}}), 400

    uploaded = request.files["file"]
    filename = uploaded.filename or "unknown.mp3"
    stem = re.sub(r"\.[^.]+$", "", filename)

    language_code = request.form.get("language_code", "deu")
    text = AUDIO_TRANSCRIPTS.get(stem, f"Mock transcript for {filename}.")

    return jsonify(_make_elevenlabs_response(text, language_code))
