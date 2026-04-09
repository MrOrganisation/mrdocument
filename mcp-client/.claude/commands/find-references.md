---
name: find-references
description: Testverfahren und psychologische Methoden in einem Dokument identifizieren und Referenzdokumente dazu suchen.
allowed-tools: MCP
argument-hint: "<dokument-id oder beschreibung>"
---

Identifiziere alle Testverfahren, psychologischen Methoden und diagnostischen Instrumente in einem oder mehreren Dokumenten und suche dann nach den zugehörigen Referenzdokumenten.

**Eingabe:** `$ARGUMENTS`

## Schritt 1 — Quelldokument(e) bestimmen

Wenn `$ARGUMENTS` eine UUID ist: verwende diese direkt als Dokument-ID.

Wenn `$ARGUMENTS` eine Beschreibung ist: verwende den `/find`-Prozess (Schema ermitteln, Query bauen, suchen) um die relevanten Dokumente zu finden.

## Schritt 2 — Inhalt lesen und Verfahren extrahieren

Für jedes gefundene Dokument:

1. Rufe `get_document_content` auf.
2. Durchsuche den Inhalt nach Testverfahren und psychologischen Methoden. Suche nach:
   - Bekannten Testabkürzungen (z.B. CBCL, PSSI, FRT, MMPI, SCL-90, BDI, DT-MV, ET 6-6-R, BUEVA, HAWIK, WISC, SON-R, AFS, PFK, FBB, SDQ, KIDS, EBI, FEKS)
   - Formulierungen wie "es wurde durchgeführt", "Testverfahren", "diagnostisches Instrument", "Fragebogen", "Screening", "Inventar"
   - Vollständige Testnamen (z.B. "Child Behavior Checklist", "Persönlichkeits-Stil-und-Störungs-Inventar")
3. Erstelle eine Liste aller identifizierten Verfahren mit Name und ggf. Abkürzung.

## Schritt 3 — Referenzdokumente suchen

Für jedes identifizierte Verfahren:

1. Suche im Kontext `wd` (Weiterbildungsdokumente) nach Referenzmaterial:
   ```json
   {"$and": [
     {"context": {"$eq": "wd"}},
     {"content": {"$search": "<Verfahrensname oder Abkürzung>"}}
   ]}
   ```
2. Suche auch mit alternativen Bezeichnungen (Abkürzung UND vollständiger Name).
3. Falls im Kontext `wd` nichts gefunden wird, suche auch in anderen Kontexten.

## Schritt 4 — Ergebnisse anzeigen

Zeige für jedes identifizierte Verfahren:
- Name und Abkürzung des Verfahrens
- Wo es im Quelldokument erwähnt wird (kurzes Zitat oder Kontextbeschreibung)
- Gefundene Referenzdokumente (ID, Beschreibung, Datum) — oder "Kein Referenzdokument gefunden"

Fasse am Ende zusammen, wie viele Verfahren identifiziert wurden und für wie viele Referenzen gefunden wurden.
