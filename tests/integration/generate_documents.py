#!/usr/bin/env python3
"""Generate test documents (PDF, TXT, RTF) for MrDocument integration tests.

Each document has clear metadata (sender, type, date, context) embedded in the
content so the AI classifier can reliably extract them.

Usage:
    python generate_documents.py [--output-dir generated/]
"""

import argparse
from pathlib import Path

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

# ---------------------------------------------------------------------------
# Document templates
# ---------------------------------------------------------------------------

DOCUMENTS = {
    "arbeit_rechnung_schulze": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-03-15",
        "content": [
            ("heading", "Schulze GmbH"),
            ("subheading", "Industriestrasse 42, 70173 Stuttgart"),
            ("body", ""),
            ("body", "Datum: 15. Maerz 2025"),
            ("body", "Rechnungsnummer: RE-2025-03-0471"),
            ("body", ""),
            ("heading", "RECHNUNG"),
            ("body", ""),
            ("body", "Sehr geehrte Damen und Herren,"),
            ("body", ""),
            ("body", "hiermit stellen wir Ihnen folgende Leistungen in Rechnung:"),
            ("body", ""),
            ("body", "Pos. 1: Beratungsleistung IT-Infrastruktur, 40 Stunden a 120,00 EUR = 4.800,00 EUR"),
            ("body", "Pos. 2: Netzwerkinstallation Buero 3. OG, pauschal = 2.350,00 EUR"),
            ("body", "Pos. 3: Firewall-Konfiguration und Sicherheitsaudit = 1.150,00 EUR"),
            ("body", ""),
            ("body", "Zwischensumme: 8.300,00 EUR"),
            ("body", "zzgl. 19% MwSt.: 1.577,00 EUR"),
            ("body", "Gesamtbetrag: 9.877,00 EUR"),
            ("body", ""),
            ("body", "Zahlungsziel: 30 Tage netto"),
            ("body", "Bankverbindung: Schulze GmbH, IBAN DE89 3704 0044 0532 0130 00"),
            ("body", ""),
            ("body", "Mit freundlichen Gruessen"),
            ("body", "Thomas Schulze"),
            ("body", "Geschaeftsfuehrer, Schulze GmbH"),
        ],
    },
    "arbeit_vertrag_fischer": {
        "context": "arbeit",
        "type": "Vertrag",
        "sender": "Fischer AG",
        "date": "2025-06-01",
        "content": [
            ("heading", "Fischer AG"),
            ("subheading", "Hauptstrasse 15, 80331 Muenchen"),
            ("body", ""),
            ("body", "Datum: 01. Juni 2025"),
            ("body", "Vertragsnummer: VA-2025-06-0088"),
            ("body", ""),
            ("heading", "VERTRAG"),
            ("subheading", "Rahmenvertrag ueber IT-Dienstleistungen"),
            ("body", ""),
            ("body", "Zwischen der Fischer AG, vertreten durch den Vorstand Herrn Martin Fischer,"),
            ("body", "nachfolgend Auftraggeber genannt,"),
            ("body", ""),
            ("body", "und"),
            ("body", ""),
            ("body", "dem Auftragnehmer,"),
            ("body", "nachfolgend Auftragnehmer genannt,"),
            ("body", ""),
            ("body", "wird folgender Rahmenvertrag geschlossen:"),
            ("body", ""),
            ("body", "Paragraph 1 - Gegenstand des Vertrages"),
            ("body", "Der Auftragnehmer erbringt IT-Beratungs- und Entwicklungsleistungen."),
            ("body", ""),
            ("body", "Paragraph 2 - Vertragslaufzeit"),
            ("body", "Der Vertrag beginnt am 01.06.2025 und laeuft bis zum 31.05.2026."),
            ("body", ""),
            ("body", "Paragraph 3 - Verguetung"),
            ("body", "Die Verguetung betraegt 150,00 EUR pro Stunde zzgl. MwSt."),
            ("body", ""),
            ("body", "Paragraph 4 - Kuendigung"),
            ("body", "Der Vertrag kann mit einer Frist von 3 Monaten zum Quartalsende gekuendigt werden."),
            ("body", ""),
            ("body", "Muenchen, den 01.06.2025"),
            ("body", ""),
            ("body", "Martin Fischer"),
            ("body", "Vorstand, Fischer AG"),
        ],
    },
    "arbeit_rechnung_keller": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Keller und Partner",
        "date": "2025-05-12",
        "content": [
            ("heading", "Keller und Partner"),
            ("subheading", "Bahnhofstrasse 7, 60329 Frankfurt am Main"),
            ("body", ""),
            ("body", "Datum: 12. Mai 2025"),
            ("body", "Rechnungsnummer: RE-2025-05-0112"),
            ("body", ""),
            ("heading", "RECHNUNG"),
            ("body", ""),
            ("body", "Sehr geehrte Damen und Herren,"),
            ("body", ""),
            ("body", "fuer die Lieferung und Montage der bestellten Bueromoebel stellen wir Ihnen folgendes in Rechnung:"),
            ("body", ""),
            ("body", "Pos. 1: 10x Schreibtisch hoehenverstellbar, Modell ErgoDesk Pro = 7.500,00 EUR"),
            ("body", "Pos. 2: 10x Buerostuhl ergonomisch, Modell SitWell 3000 = 5.000,00 EUR"),
            ("body", "Pos. 3: Lieferung und Montage, pauschal = 1.200,00 EUR"),
            ("body", ""),
            ("body", "Zwischensumme: 13.700,00 EUR"),
            ("body", "zzgl. 19% MwSt.: 2.603,00 EUR"),
            ("body", "Gesamtbetrag: 16.303,00 EUR"),
            ("body", ""),
            ("body", "Zahlungsziel: 14 Tage netto"),
            ("body", "Bankverbindung: Keller und Partner, IBAN DE52 5001 0517 0648 4898 90"),
            ("body", ""),
            ("body", "Mit freundlichen Gruessen"),
            ("body", "Sabine Keller"),
            ("body", "Keller und Partner"),
        ],
    },
    "arbeit_angebot_fischer": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Fischer AG",
        "date": "2025-08-05",
        "content": [
            ("heading", "Fischer AG"),
            ("subheading", "Hauptstrasse 15, 80331 Muenchen"),
            ("body", ""),
            ("body", "Datum: 05. August 2025"),
            ("body", "Angebotsnummer: AN-2025-08-0045"),
            ("body", ""),
            ("heading", "ANGEBOT"),
            ("subheading", "IT-Beratung und Systemintegration"),
            ("body", ""),
            ("body", "Sehr geehrte Damen und Herren,"),
            ("body", ""),
            ("body", "vielen Dank fuer Ihre Anfrage bezueglich IT-Beratungsleistungen."),
            ("body", "Gerne unterbreiten wir Ihnen folgendes Angebot:"),
            ("body", ""),
            ("body", "Pos. 1: IT-Strategieberatung, 20 Tage a 1.500,00 EUR = 30.000,00 EUR"),
            ("body", "Pos. 2: Systemintegration ERP-Anbindung, pauschal = 15.000,00 EUR"),
            ("body", "Pos. 3: Schulung Mitarbeiter, 5 Tage a 800,00 EUR = 4.000,00 EUR"),
            ("body", ""),
            ("body", "Gesamtangebot netto: 49.000,00 EUR"),
            ("body", "zzgl. 19% MwSt.: 9.310,00 EUR"),
            ("body", "Gesamtbetrag brutto: 58.310,00 EUR"),
            ("body", ""),
            ("body", "Dieses Angebot ist gueltig bis zum 05. September 2025."),
            ("body", ""),
            ("body", "Mit freundlichen Gruessen"),
            ("body", "Martin Fischer"),
            ("body", "Vorstand, Fischer AG"),
        ],
    },
    "arbeit_angebot_keller": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Keller und Partner",
        "date": "2025-09-20",
        "content": [
            ("heading", "Keller und Partner"),
            ("subheading", "Bahnhofstrasse 7, 60329 Frankfurt am Main"),
            ("body", ""),
            ("body", "Datum: 20. September 2025"),
            ("body", "Angebotsnummer: AN-2025-09-0234"),
            ("body", ""),
            ("heading", "ANGEBOT"),
            ("subheading", "Modernisierung der Bueroausstattung"),
            ("body", ""),
            ("body", "Sehr geehrte Damen und Herren,"),
            ("body", ""),
            ("body", "vielen Dank fuer Ihre Anfrage. Gerne unterbreiten wir Ihnen folgendes Angebot:"),
            ("body", ""),
            ("body", "Pos. 1: 25x Schreibtisch hoehenverstellbar, Modell ErgoDesk Pro = 18.750,00 EUR"),
            ("body", "Pos. 2: 25x Buerostuhl ergonomisch, Modell SitWell 3000 = 12.500,00 EUR"),
            ("body", "Pos. 3: 10x Besprechungstisch oval, 240x120cm = 8.900,00 EUR"),
            ("body", "Pos. 4: Lieferung und Montage, pauschal = 3.200,00 EUR"),
            ("body", ""),
            ("body", "Gesamtangebot netto: 43.350,00 EUR"),
            ("body", "zzgl. 19% MwSt.: 8.236,50 EUR"),
            ("body", "Gesamtbetrag brutto: 51.586,50 EUR"),
            ("body", ""),
            ("body", "Dieses Angebot ist gueltig bis zum 20. Oktober 2025."),
            ("body", "Lieferzeit: 6-8 Wochen nach Auftragserteilung."),
            ("body", ""),
            ("body", "Mit freundlichen Gruessen"),
            ("body", "Sabine Keller"),
            ("body", "Keller und Partner"),
        ],
    },
    "privat_arztbrief_braun": {
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Braun",
        "date": "2025-04-10",
        "content": [
            ("heading", "Dr. med. Heinrich Braun"),
            ("subheading", "Facharzt fuer Allgemeinmedizin"),
            ("subheading", "Lindenallee 23, 50668 Koeln"),
            ("body", ""),
            ("body", "Datum: 10. April 2025"),
            ("body", ""),
            ("heading", "ARZTBRIEF"),
            ("body", ""),
            ("body", "Patient: Max Mustermann, geb. 15.03.1985"),
            ("body", "Untersuchungsdatum: 10.04.2025"),
            ("body", ""),
            ("body", "Sehr geehrte Kolleginnen und Kollegen,"),
            ("body", ""),
            ("body", "ich berichte ueber o.g. Patienten, der sich am 10.04.2025 in meiner Praxis vorstellte."),
            ("body", ""),
            ("body", "Anamnese:"),
            ("body", "Der Patient berichtet ueber seit 2 Wochen bestehende Rueckenschmerzen im"),
            ("body", "Bereich der Lendenwirbelsaeule mit Ausstrahlung ins linke Bein."),
            ("body", ""),
            ("body", "Befund:"),
            ("body", "Druckschmerz L4/L5, Lasegue links positiv bei 40 Grad."),
            ("body", "Neurologischer Status der unteren Extremitaeten unauffaellig."),
            ("body", ""),
            ("body", "Diagnose:"),
            ("body", "Lumboischialgiesyndrom links, V.a. Bandscheibenprotrusion L4/L5."),
            ("body", ""),
            ("body", "Therapieempfehlung:"),
            ("body", "Physiotherapie 2x woechentlich, Ibuprofen 600mg bei Bedarf."),
            ("body", "MRT-Kontrolle empfohlen bei ausbleibender Besserung."),
            ("body", ""),
            ("body", "Mit kollegialen Gruessen"),
            ("body", "Dr. med. Heinrich Braun"),
        ],
    },
    "privat_versicherung_allianz": {
        "context": "privat",
        "type": "Versicherung",
        "sender": "Allianz",
        "date": "2025-07-22",
        "content": [
            ("heading", "Allianz Versicherungs-AG"),
            ("subheading", "Koeniginstrasse 28, 80802 Muenchen"),
            ("body", ""),
            ("body", "Datum: 22. Juli 2025"),
            ("body", "Versicherungsnummer: HV-2025-4471-8832"),
            ("body", ""),
            ("heading", "VERSICHERUNG"),
            ("subheading", "Versicherungsschein - Hausratversicherung"),
            ("body", ""),
            ("body", "Sehr geehrter Herr Mustermann,"),
            ("body", ""),
            ("body", "wir freuen uns, Ihnen Ihren neuen Versicherungsschein zu uebersenden."),
            ("body", ""),
            ("body", "Versicherungsnehmer: Max Mustermann"),
            ("body", "Versicherungsart: Hausratversicherung Premium"),
            ("body", "Versicherungssumme: 65.000,00 EUR"),
            ("body", "Versicherungsbeginn: 01.08.2025"),
            ("body", "Versicherungsende: 31.07.2026"),
            ("body", ""),
            ("body", "Jahresbeitrag: 287,40 EUR"),
            ("body", "Zahlungsweise: jaehrlich"),
            ("body", ""),
            ("body", "Versicherte Gefahren:"),
            ("body", "- Feuer, Blitzschlag, Explosion"),
            ("body", "- Einbruchdiebstahl, Raub, Vandalismus"),
            ("body", "- Leitungswasser"),
            ("body", "- Sturm und Hagel ab Windstaerke 8"),
            ("body", ""),
            ("body", "Selbstbeteiligung: 150,00 EUR je Schadensfall"),
            ("body", ""),
            ("body", "Mit freundlichen Gruessen"),
            ("body", "Allianz Versicherungs-AG"),
            ("body", "Abteilung Vertragsverwaltung"),
        ],
    },
    "privat_kontoauszug_sparkasse": {
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Sparkasse",
        "date": "2025-11-30",
        "content": [
            ("heading", "Sparkasse Koeln-Bonn"),
            ("subheading", "Hahnenstrasse 57, 50667 Koeln"),
            ("body", ""),
            ("body", "Datum: 30. November 2025"),
            ("body", "Kontonummer: DE91 3705 0198 0012 3456 78"),
            ("body", ""),
            ("heading", "KONTOAUSZUG"),
            ("subheading", "Auszug Nr. 11/2025"),
            ("body", ""),
            ("body", "Kontoinhaber: Max Mustermann"),
            ("body", "Kontoart: Girokonto Komfort"),
            ("body", "Auszugszeitraum: 01.11.2025 - 30.11.2025"),
            ("body", ""),
            ("body", "Alter Saldo: 3.245,67 EUR"),
            ("body", ""),
            ("body", "01.11.2025  Gehalt November          +3.850,00 EUR"),
            ("body", "03.11.2025  Miete Wohnung             -950,00 EUR"),
            ("body", "05.11.2025  Stadtwerke Koeln Strom     -87,50 EUR"),
            ("body", "08.11.2025  REWE Lebensmittel          -63,21 EUR"),
            ("body", "12.11.2025  Allianz Versicherung      -287,40 EUR"),
            ("body", "15.11.2025  Tankstelle Shell           -72,80 EUR"),
            ("body", "20.11.2025  Amazon Bestellung         -124,99 EUR"),
            ("body", "25.11.2025  Zahnarzt Dr. Mueller       -45,00 EUR"),
            ("body", "28.11.2025  Dauerauftrag Sparkonto    -500,00 EUR"),
            ("body", ""),
            ("body", "Neuer Saldo: 4.964,77 EUR"),
            ("body", ""),
            ("body", "Sparkasse Koeln-Bonn"),
            ("body", "Ihr Finanzpartner"),
        ],
    },
    "privat_arztbrief_mueller": {
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Mueller",
        "date": "2025-10-15",
        "content": [
            ("heading", "Dr. med. Claudia Mueller"),
            ("subheading", "Fachpraxis fuer Dermatologie"),
            ("subheading", "Mozartstrasse 11, 53115 Bonn"),
            ("body", ""),
            ("body", "Datum: 15. Oktober 2025"),
            ("body", ""),
            ("heading", "ARZTBRIEF"),
            ("subheading", "Ueberweisung zur dermatologischen Abklaerung"),
            ("body", ""),
            ("body", "Patient: Max Mustermann, geb. 15.03.1985"),
            ("body", "Untersuchungsdatum: 15.10.2025"),
            ("body", ""),
            ("body", "Sehr geehrte Kolleginnen und Kollegen,"),
            ("body", ""),
            ("body", "ich ueberweise Ihnen o.g. Patienten zur weiterfuehrenden Diagnostik."),
            ("body", ""),
            ("body", "Anamnese:"),
            ("body", "Der Patient stellt sich mit einer seit 4 Wochen bestehenden"),
            ("body", "Hautveraenderung am rechten Unterarm vor. Das Areal ist leicht"),
            ("body", "geroetet und zeigt eine schuppende Oberflaeche."),
            ("body", ""),
            ("body", "Befund:"),
            ("body", "Erythematoeser Plaque, ca. 3x2 cm, rechter Unterarm ventral."),
            ("body", "Keine Lymphknotenvergroesserung axillaer."),
            ("body", ""),
            ("body", "Verdachtsdiagnose:"),
            ("body", "Ekzem DD Psoriasis."),
            ("body", ""),
            ("body", "Empfehlung:"),
            ("body", "Biopsie zur histologischen Abklaerung empfohlen."),
            ("body", ""),
            ("body", "Mit kollegialen Gruessen"),
            ("body", "Dr. med. Claudia Mueller"),
        ],
    },
    "privat_kontoauszug_allianz": {
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Allianz",
        "date": "2025-02-18",
        "content": [
            ("heading", "Allianz Lebensversicherungs-AG"),
            ("subheading", "Reinsburgstrasse 19, 70178 Stuttgart"),
            ("body", ""),
            ("body", "Datum: 18. Februar 2025"),
            ("body", "Vertragsnummer: LV-2019-7834-2201"),
            ("body", ""),
            ("heading", "KONTOAUSZUG"),
            ("subheading", "Jahreskontoauszug Lebensversicherung 2024"),
            ("body", ""),
            ("body", "Versicherungsnehmer: Max Mustermann"),
            ("body", "Versicherungsart: Kapitallebensversicherung"),
            ("body", "Versicherungsbeginn: 01.01.2019"),
            ("body", ""),
            ("body", "Stand 31.12.2024:"),
            ("body", ""),
            ("body", "Eingezahlte Beitraege 2024: 2.400,00 EUR"),
            ("body", "Eingezahlte Beitraege gesamt: 14.400,00 EUR"),
            ("body", ""),
            ("body", "Garantiertes Kapital: 13.824,00 EUR"),
            ("body", "Ueberschussbeteiligung: 1.243,17 EUR"),
            ("body", "Aktueller Rueckkaufswert: 15.067,17 EUR"),
            ("body", ""),
            ("body", "Garantierte Ablaufleistung (01.01.2044): 72.000,00 EUR"),
            ("body", "Prognostizierte Ablaufleistung: 89.340,00 EUR"),
            ("body", ""),
            ("body", "Mit freundlichen Gruessen"),
            ("body", "Allianz Lebensversicherungs-AG"),
            ("body", "Abteilung Vertragsservice"),
        ],
    },
}


def _content_to_plaintext(content: list[tuple[str, str]]) -> str:
    """Convert content tuples to plain text."""
    lines = []
    for style, text in content:
        if style == "heading":
            lines.append(text.upper())
            lines.append("=" * len(text))
        elif style == "subheading":
            lines.append(text)
            lines.append("-" * len(text))
        else:
            lines.append(text)
    return "\n".join(lines) + "\n"


def _content_to_rtf(content: list[tuple[str, str]]) -> str:
    """Convert content tuples to minimal RTF."""

    def _rtf_escape(text: str) -> str:
        out = []
        for ch in text:
            cp = ord(ch)
            if cp > 127:
                out.append(f"\\u{cp}?")
            elif ch == "\\":
                out.append("\\\\")
            elif ch == "{":
                out.append("\\{")
            elif ch == "}":
                out.append("\\}")
            else:
                out.append(ch)
        return "".join(out)

    parts = ["{\\rtf1\\ansi\\deff0{\\fonttbl{\\f0 Arial;}}"]
    for style, text in content:
        escaped = _rtf_escape(text)
        if style == "heading":
            parts.append(f"{{\\b\\fs28 {escaped}}}\\par")
        elif style == "subheading":
            parts.append(f"{{\\i\\fs22 {escaped}}}\\par")
        else:
            parts.append(f"{escaped}\\par")
    parts.append("}")
    return "\n".join(parts)


def _generate_pdf(doc_id: str, content: list[tuple[str, str]], output_dir: Path) -> Path:
    """Generate a PDF using reportlab.  Skips if the file already exists."""
    path = output_dir / f"{doc_id}.pdf"
    if path.exists():
        return path
    styles = getSampleStyleSheet()
    heading_style = ParagraphStyle(
        "DocHeading", parent=styles["Heading1"], fontSize=16, spaceAfter=4
    )
    subheading_style = ParagraphStyle(
        "DocSubheading", parent=styles["Heading2"], fontSize=12, spaceAfter=2
    )
    body_style = ParagraphStyle(
        "DocBody", parent=styles["Normal"], fontSize=10, spaceAfter=2
    )

    doc = SimpleDocTemplate(
        str(path),
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
    )
    story = []
    for style, text in content:
        if not text:
            story.append(Spacer(1, 12))
            continue
        if style == "heading":
            story.append(Paragraph(text, heading_style))
        elif style == "subheading":
            story.append(Paragraph(text, subheading_style))
        else:
            story.append(Paragraph(text, body_style))

    doc.build(story)
    return path


def _generate_txt(doc_id: str, content: list[tuple[str, str]], output_dir: Path) -> Path:
    """Generate a plain TXT file.  Skips if the file already exists."""
    path = output_dir / f"{doc_id}.txt"
    if path.exists():
        return path
    path.write_text(_content_to_plaintext(content), encoding="utf-8")
    return path


def _generate_rtf(doc_id: str, content: list[tuple[str, str]], output_dir: Path) -> Path:
    """Generate a minimal RTF file.  Skips if the file already exists."""
    path = output_dir / f"{doc_id}.rtf"
    if path.exists():
        return path
    path.write_text(_content_to_rtf(content), encoding="utf-8")
    return path


def generate_all(output_dir: Path) -> list[Path]:
    """Generate test documents in PDF, TXT, and RTF formats.

    Skips files that already exist — only missing documents are generated.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    generated = []
    for doc_id, doc in DOCUMENTS.items():
        content = doc["content"]
        generated.append(_generate_pdf(doc_id, content, output_dir))
        generated.append(_generate_txt(doc_id, content, output_dir))
        generated.append(_generate_rtf(doc_id, content, output_dir))
    return generated


def main():
    parser = argparse.ArgumentParser(description="Generate MrDocument test documents")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "generated",
        help="Directory for generated files (default: generated/)",
    )
    args = parser.parse_args()

    expected = {
        f"{doc_id}.{ext}"
        for doc_id in DOCUMENTS
        for ext in ("pdf", "txt", "rtf")
    }
    existing_before = {p.name for p in args.output_dir.glob("*") if p.name in expected}

    files = generate_all(args.output_dir)
    new_files = [f for f in files if f.name not in existing_before]

    if new_files:
        print(f"Generated {len(new_files)} new files in {args.output_dir}:")
        for f in sorted(new_files):
            print(f"  {f.name}")
    else:
        print(f"All {len(files)} files already exist in {args.output_dir}, nothing to do.")


if __name__ == "__main__":
    main()
