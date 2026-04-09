---
name: find-sources
description: Alle Quelldokumente eines Falls zu einem bestimmten Thema finden.
allowed-tools: MCP
argument-hint: "<fall> <thema>"
---

Finde alle Quelldokumente in der Gutachtenakte zu einem bestimmten Fall und Thema.

**Eingabe:** `$ARGUMENTS`

## Schritt 1 — Argumente parsen

Zerlege `$ARGUMENTS` in:
- `FALL`: das erste Wort (Fallbezeichnung, z.B. "mueller")
- `THEMA`: alles nach dem ersten Wort (z.B. "kindesvater", "umgangsregelung", "schule")

## Schritt 2 — Falldokumente suchen

Suche alle Dokumente des Falls im Kontext `ga`:

```json
{"context": {"$eq": "ga"}, "metadata.case": {"$ilike": "%<FALL>%"}}
```

Falls keine Ergebnisse: versuche mit `$search` auf content nach dem Fallnamen.

## Schritt 3 — Thematisch filtern

Aus den Falldokumenten die zum Thema passenden herausfiltern. Verwende dafür eine kombinierte Abfrage:

```json
{"$and": [
  {"context": {"$eq": "ga"}},
  {"metadata.case": {"$ilike": "%<FALL>%"}},
  {"content": {"$search": "<THEMA>"}}
]}
```

Falls keine Ergebnisse mit `$search`: erweitere die Suchbegriffe (z.B. bei "kindesvater" auch "Vater", "KV", "Kindesvater" probieren).

## Schritt 4 — Ergebnisse anzeigen

Zeige die gefundenen Dokumente als Tabelle mit: ID, Dokumenttyp, Beschreibung, Datum. Fasse kurz zusammen, was gefunden wurde und wie die Dokumente zum Thema passen.
