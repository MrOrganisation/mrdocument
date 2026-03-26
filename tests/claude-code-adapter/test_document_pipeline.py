"""End-to-end tests that simulate the actual mrdocument-service request patterns.

These mirror what ai.rs sends: context classification followed by metadata
extraction, both with tool_use and the exact schema shapes used in production.
"""

import json

import pytest

from conftest import TEST_MODEL

# --- Tool definitions matching service-rs/src/ai.rs ---

CONTEXT_TOOL = {
    "name": "classify_context",
    "description": (
        "Classify the document into one of the available contexts "
        "based on its content and filename (if provided).\n\n"
        "Available contexts:\n"
        "- arbeit: Work-related documents (invoices, contracts, payslips)\n"
        "  type values: Rechnung, Vertrag, Gehaltsabrechnung\n"
        "- privat: Personal documents (medical, insurance, bank statements)\n"
        "  type values: Arztbrief, Versicherung, Kontoauszug"
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "context": {
                "type": "string",
                "enum": ["arbeit", "privat"],
                "description": "The context this document belongs to.",
            },
        },
        "required": ["context"],
    },
}

EXTRACTION_TOOL = {
    "name": "extract_metadata",
    "description": "Extract metadata from the document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "date": {
                "type": "string",
                "description": "Document date in YYYY-MM-DD format, or null if not found.",
            },
            "type": {
                "type": "string",
                "enum": ["Rechnung", "Vertrag", "Gehaltsabrechnung"],
                "description": "Identify the document type.",
            },
            "sender": {
                "type": "string",
                "description": "Identify the sender or issuing organization.",
            },
            "topic": {
                "type": "string",
                "description": "Identify the general topic or matter.",
            },
        },
        "required": ["type", "sender", "topic"],
    },
}


SAMPLE_INVOICE = """
Rechnung Nr. 2025-4711

Von: Müller & Söhne GmbH
     Industriestraße 42
     80331 München

An: Max Mustermann
    Beispielweg 7
    10115 Berlin

Datum: 15.03.2025

Posten:
  1x Beratungsdienstleistung IT-Infrastruktur    2.500,00 EUR
  1x Netzwerk-Audit                              1.200,00 EUR
                                          ──────────────────
  Gesamt (netto):                                3.700,00 EUR
  USt. 19%:                                        703,00 EUR
  Gesamt (brutto):                               4.403,00 EUR

Zahlungsziel: 30 Tage netto
IBAN: DE89 3704 0044 0532 0130 00
"""

SAMPLE_MEDICAL = """
Arztbrief

Patient: Maria Schmidt, geb. 12.05.1985
Untersuchungsdatum: 22.01.2025

Sehr geehrte Kollegen,

ich berichte über die Vorstellung von Frau Schmidt am 22.01.2025
in unserer kardiologischen Ambulanz.

Diagnosen:
- Arterielle Hypertonie, gut eingestellt
- Belastungsdyspnoe NYHA II

Therapie:
- Ramipril 5mg 1-0-0
- Bisoprolol 2,5mg 1-0-0

Mit freundlichen Grüßen,
Dr. med. Klaus Weber
Facharzt für Kardiologie
"""


@pytest.mark.timeout(180)
def test_classify_invoice_as_arbeit(post_messages):
    """A German invoice is classified into the 'arbeit' context."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "system": "You are a document classification assistant.",
        "messages": [{"role": "user", "content": f"Classify this document:\n\n{SAMPLE_INVOICE}"}],
        "tools": [CONTEXT_TOOL],
        "tool_choice": {"type": "tool", "name": "classify_context"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["content"][0]["input"]["context"] == "arbeit"


@pytest.mark.timeout(180)
def test_classify_medical_as_privat(post_messages):
    """A German medical letter is classified into the 'privat' context."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "system": "You are a document classification assistant.",
        "messages": [{"role": "user", "content": f"Classify this document:\n\n{SAMPLE_MEDICAL}"}],
        "tools": [CONTEXT_TOOL],
        "tool_choice": {"type": "tool", "name": "classify_context"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["content"][0]["input"]["context"] == "privat"


@pytest.mark.timeout(180)
def test_extract_invoice_metadata(post_messages):
    """Metadata extraction returns correct fields for a German invoice."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 512,
        "system": "You are a document metadata extraction assistant. All results shall be returned in German.",
        "messages": [{"role": "user", "content": f"Extract metadata from this document:\n\n{SAMPLE_INVOICE}"}],
        "tools": [EXTRACTION_TOOL],
        "tool_choice": {"type": "tool", "name": "extract_metadata"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["stop_reason"] == "tool_use"

    inp = body["content"][0]["input"]
    assert inp["type"] == "Rechnung"
    assert "Müller" in inp.get("sender", "") or "Muller" in inp.get("sender", "") or "müller" in inp.get("sender", "").lower()
    assert "2025" in inp.get("date", "")


@pytest.mark.timeout(180)
def test_full_classify_then_extract_pipeline(post_messages):
    """Simulates the full service pipeline: classify first, then extract."""
    # Step 1: Classify
    r1 = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "system": "You are a document classification assistant.",
        "messages": [{"role": "user", "content": f"Classify this document:\n\n{SAMPLE_INVOICE}"}],
        "tools": [CONTEXT_TOOL],
        "tool_choice": {"type": "tool", "name": "classify_context"},
    })
    assert r1.status_code == 200
    context = r1.json()["content"][0]["input"]["context"]
    assert context == "arbeit"

    # Step 2: Extract metadata
    r2 = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 512,
        "system": "You are a document metadata extraction assistant. All results shall be returned in German.",
        "messages": [{"role": "user", "content": f"Extract metadata from this document:\n\n{SAMPLE_INVOICE}"}],
        "tools": [EXTRACTION_TOOL],
        "tool_choice": {"type": "tool", "name": "extract_metadata"},
    })
    assert r2.status_code == 200
    inp = r2.json()["content"][0]["input"]

    # All required fields present
    assert inp.get("type")
    assert inp.get("sender")
    assert inp.get("topic")


@pytest.mark.timeout(180)
def test_streaming_classify_reconstruct(post_messages):
    """Streaming tool_use (as used with extended_thinking) reconstructs correctly."""
    r = post_messages(
        {
            "model": TEST_MODEL,
            "max_tokens": 256,
            "stream": True,
            "messages": [{"role": "user", "content": f"Classify this document:\n\n{SAMPLE_INVOICE}"}],
            "tools": [CONTEXT_TOOL],
            "tool_choice": {"type": "auto"},
        },
        stream=True,
    )
    assert r.status_code == 200

    # Reconstruct from SSE (same logic as parse_sse_response in ai.rs)
    json_accum = ""
    tool_name = ""
    stop_reason = ""
    for line in r.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data: "):
            continue
        event = json.loads(line[6:])
        etype = event.get("type", "")
        if etype == "content_block_start":
            cb = event.get("content_block", {})
            if cb.get("type") == "tool_use":
                tool_name = cb.get("name", "")
                json_accum = ""
        elif etype == "content_block_delta":
            delta = event.get("delta", {})
            if delta.get("type") == "input_json_delta":
                json_accum += delta.get("partial_json", "")
        elif etype == "message_delta":
            stop_reason = event.get("delta", {}).get("stop_reason", "")

    assert tool_name == "classify_context"
    assert stop_reason == "tool_use"
    inp = json.loads(json_accum)
    assert inp["context"] in ("arbeit", "privat")
