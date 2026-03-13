#!/usr/bin/env python3
"""Generate test audio files (MP3) for MrDocument integration tests.

Uses gTTS for German text-to-speech and optionally pydub for segment
concatenation.  Falls back to single-pass gTTS if pydub is unavailable.

Usage:
    python generate_audio.py [--output-dir generated/]
"""

import argparse
import io
import tempfile
from pathlib import Path

from gtts import gTTS

# ---------------------------------------------------------------------------
# Audio templates
# ---------------------------------------------------------------------------

AUDIO_FILES = {
    "besprechung-intro": {
        "context": "arbeit",
        "date": "2025-03-15",
        "intro_position": "beginning",
        "intro_text": (
            "Aufnahme vom fuenfzehnten Maerz zweitausendundfuenfundzwanzig. "
            "Teilnehmer sind Herr Schulze von der Schulze GmbH und Frau Weber "
            "aus der IT-Abteilung. Thema ist die Besprechung zum "
            "IT-Infrastrukturprojekt."
        ),
        "body_text": (
            "Herr Schulze beginnt. Also, wir haben uns die Server-Infrastruktur "
            "angeschaut und festgestellt, dass die aktuelle Firewall-Konfiguration "
            "nicht mehr den Anforderungen entspricht. Frau Weber antwortet. "
            "Ja, das sehe ich auch so. Wir brauchen mindestens eine redundante "
            "Firewall-Loesung mit automatischem Failover. Ausserdem sollten wir "
            "die Netzwerksegmentierung im dritten Obergeschoss ueberarbeiten. "
            "Herr Schulze ergaenzt. Gut, dann nehme ich das in unser Angebot "
            "auf. Die Kosten fuer die Beratung und Installation hatten wir ja "
            "schon besprochen. Frau Weber sagt. Genau, bitte schicken Sie uns "
            "die Rechnung wie vereinbart mit dreissig Tagen Zahlungsziel."
        ),
    },
    "arztgespraech-intro": {
        "context": "privat",
        "date": "2025-04-10",
        "intro_position": "end",
        "intro_text": (
            "Diese Aufnahme ist vom zehnten April zweitausendundfuenfundzwanzig. "
            "Gespraech zwischen Patient Max Mustermann und Doktor Braun in der "
            "Praxis fuer Allgemeinmedizin."
        ),
        "body_text": (
            "Doktor Braun fragt. Herr Mustermann, was fuehrt Sie heute zu mir? "
            "Der Patient antwortet. Ich habe seit etwa zwei Wochen starke "
            "Rueckenschmerzen, besonders im unteren Bereich. Die Schmerzen "
            "strahlen manchmal ins linke Bein aus. Doktor Braun sagt. "
            "Verstehe, lassen Sie mich das untersuchen. Bitte legen Sie sich "
            "auf die Liege. Nach der Untersuchung. Also, ich vermute eine "
            "Bandscheibenprotrusion im Bereich L vier L fuenf. Ich empfehle "
            "Ihnen Physiotherapie zweimal woechentlich und bei Bedarf Ibuprofen "
            "sechshundert Milligramm. Wenn es nicht besser wird, machen wir "
            "ein MRT."
        ),
    },
    "telefonat": {
        "context": "arbeit",
        "date": "2025-09-20",
        "intro_position": None,
        "intro_text": None,
        "body_text": (
            "Guten Tag, hier spricht Sabine Keller von Keller und Partner. "
            "Ich rufe an wegen unseres Angebots fuer die Bueroausstattung. "
            "Wir haben die Preise nochmal kalkuliert und koennen Ihnen einen "
            "Rabatt von fuenf Prozent auf die Gesamtsumme anbieten, wenn Sie "
            "bis zum zwanzigsten Oktober bestellen. Die hoehenverstellbaren "
            "Schreibtische und die ergonomischen Buerostuehle sind aktuell "
            "auf Lager und koennten innerhalb von sechs Wochen geliefert "
            "werden. Die Besprechungstische haben eine Lieferzeit von acht "
            "Wochen. Montage ist im Preis inbegriffen. Bitte geben Sie uns "
            "Bescheid, ob das Angebot fuer Sie in Frage kommt. "
            "Auf Wiedersehen."
        ),
    },
    "videocall": {
        "context": "arbeit",
        "date": "2025-05-12",
        "intro_position": None,
        "intro_text": None,
        "format": "mov",
        "body_text": (
            "Guten Morgen zusammen, hier ist die woechentliche Projektbesprechung "
            "vom zwoelften Mai zweitausendundfuenfundzwanzig. Herr Fischer von "
            "der Fischer AG ist zugeschaltet. Herr Fischer, koennten Sie uns "
            "bitte den aktuellen Stand der ERP-Integration vorstellen? "
            "Ja, gerne. Wir haben in der letzten Woche die Schnittstelle "
            "zum Warenwirtschaftssystem fertiggestellt. Die ersten Testlaeufe "
            "waren erfolgreich. Naechste Woche beginnen wir mit der "
            "Anbindung der Finanzbuchhaltung. Der Zeitplan wird eingehalten."
        ),
    },
    "sprachnachricht": {
        "context": "privat",
        "date": "2025-06-18",
        "intro_position": None,
        "intro_text": None,
        "format": "mp4",
        "body_text": (
            "Hallo Max, hier ist Claudia Mueller. Ich wollte dir kurz "
            "Bescheid geben, dass die Ergebnisse deiner Hautuntersuchung "
            "vom achtzehnten Juni vorliegen. Es handelt sich um ein "
            "harmloses Ekzem, keine Psoriasis. Ich schicke dir den "
            "ausfuehrlichen Befund per Post. Bitte trage die verschriebene "
            "Salbe zweimal taeglich auf. Wenn du Fragen hast, ruf gerne "
            "in der Praxis an. Schoene Gruesse."
        ),
    },
    "condpattern-audio": {
        "context": "testctx",
        "date": "2025-02-20",
        "intro_position": None,
        "intro_text": None,
        "format": "m4a",
        "body_text": (
            "Guten Morgen, hier ist eine kurze Testaufnahme. "
            "Wir testen das bedingte Dateinamensmuster mit einem "
            "Kontextwechsel durch die KI-Klassifizierung."
        ),
    },
    "sorted-wrongctx-audio": {
        "context": "arbeit",
        "date": "2025-01-20",
        "intro_position": None,
        "intro_text": None,
        "format": "m4a",
        "body_text": (
            "Guten Tag, hier ist Martin Schulze von der Schulze GmbH. "
            "Ich moechte kurz die Ergebnisse unserer letzten Besprechung "
            "vom zwanzigsten Januar zusammenfassen. Die Serverausstattung "
            "wird wie besprochen im Februar geliefert. Die Installation "
            "der neuen Firewall beginnt in der dritten Kalenderwoche."
        ),
    },
    "privatnotiz": {
        "context": "privat",
        "date": "2025-08-03",
        "intro_position": None,
        "intro_text": None,
        "format": "m4a",
        "body_text": (
            "Hallo, hier ist eine kurze Notiz fuer mich selbst. "
            "Morgen um zehn Uhr habe ich einen Termin bei Doktor Braun "
            "in der Praxis am Marktplatz. Ich soll die Befunde vom "
            "dritten August mitbringen und vorher nichts essen."
        ),
    },
    "subfolder-locked-audio": {
        "context": "arbeit",
        "date": "2025-11-05",
        "intro_position": None,
        "intro_text": None,
        "format": "m4a",
        "body_text": (
            "Guten Tag, hier spricht Thomas Berger von der Schulze GmbH. "
            "Ich moechte die Besprechungsergebnisse vom fuenften November "
            "zweitausendundfuenfundzwanzig festhalten. Wir haben den "
            "Wartungsvertrag fuer die Serverinfrastruktur besprochen. "
            "Die jaehrliche Wartungspauschale betraegt sechstausend Euro. "
            "Bitte senden Sie uns die Rechnung an die bekannte Adresse."
        ),
    },
}


def _tts_to_bytes(text: str, lang: str = "de") -> bytes:
    """Render text to MP3 bytes via gTTS."""
    tts = gTTS(text=text, lang=lang)
    buf = io.BytesIO()
    tts.write_to_fp(buf)
    return buf.getvalue()


def _generate_with_pydub(
    intro_bytes: bytes | None,
    body_bytes: bytes,
    intro_position: str | None,
    output_path: Path,
) -> None:
    """Concatenate intro and body segments using pydub."""
    from pydub import AudioSegment

    body_seg = AudioSegment.from_mp3(io.BytesIO(body_bytes))
    silence = AudioSegment.silent(duration=2000)  # 2s pause

    if intro_bytes and intro_position:
        intro_seg = AudioSegment.from_mp3(io.BytesIO(intro_bytes))
        if intro_position == "beginning":
            combined = intro_seg + silence + body_seg
        else:  # end
            combined = body_seg + silence + intro_seg
    else:
        combined = body_seg

    combined.export(str(output_path), format="mp3")


def _generate_single_pass(
    intro_text: str | None,
    body_text: str,
    intro_position: str | None,
    output_path: Path,
) -> None:
    """Fallback: concatenate text and generate a single gTTS pass."""
    if intro_text and intro_position:
        pause = " ... ... ... "
        if intro_position == "beginning":
            full_text = intro_text + pause + body_text
        else:
            full_text = body_text + pause + intro_text
    else:
        full_text = body_text

    mp3_bytes = _tts_to_bytes(full_text)
    output_path.write_bytes(mp3_bytes)


def generate_all(output_dir: Path) -> list[Path]:
    """Generate all test audio files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check pydub availability
    try:
        from pydub import AudioSegment  # noqa: F401
        use_pydub = True
    except ImportError:
        print("Warning: pydub not available, using single-pass gTTS fallback")
        use_pydub = False

    generated = []
    for filename, spec in AUDIO_FILES.items():
        ext = spec.get("format", "mp3")
        output_path = output_dir / f"{filename}.{ext}"
        if output_path.exists():
            generated.append(output_path)
            continue

        intro_text = spec["intro_text"]
        body_text = spec["body_text"]
        intro_position = spec["intro_position"]

        if use_pydub:
            intro_bytes = _tts_to_bytes(intro_text) if intro_text else None
            body_bytes = _tts_to_bytes(body_text)
            _generate_with_pydub(intro_bytes, body_bytes, intro_position, output_path)
        else:
            _generate_single_pass(intro_text, body_text, intro_position, output_path)

        generated.append(output_path)

    return generated


def main():
    parser = argparse.ArgumentParser(description="Generate MrDocument test audio files")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "generated",
        help="Directory for generated files (default: generated/)",
    )
    args = parser.parse_args()

    expected = {
        f"{name}.{spec.get('format', 'mp3')}"
        for name, spec in AUDIO_FILES.items()
    }
    existing_before = {p.name for p in args.output_dir.glob("*") if p.name in expected}

    files = generate_all(args.output_dir)
    new_files = [f for f in files if f.name not in existing_before]

    if new_files:
        print(f"Generated {len(new_files)} new audio files in {args.output_dir}:")
        for f in sorted(new_files):
            print(f"  {f.name}")
    else:
        print(f"All {len(files)} audio files already exist in {args.output_dir}, nothing to do.")


if __name__ == "__main__":
    main()
