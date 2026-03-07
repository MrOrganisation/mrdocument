# Smart Folders auf Root-Ebene — Benutzerhandbuch

## Was sind Smart Folders?

Smart Folders sind Verzeichnisse, die automatisch symbolische Links zu Dokumenten enthalten, die bestimmte Bedingungen erfuellen. Sie bieten verschiedene "Ansichten" auf sortierte Dokumente, ohne Dateien zu duplizieren.

Es gibt zwei Typen:

1. **Kontext-Smart-Folders** (bestehend): Konfiguriert pro Kontext in `sorted/{kontext}/smartfolders.yaml`. Erstellt in jedem Blattordner von `sorted/` ein Unterverzeichnis mit Links zu passenden Dateien.

2. **Root-Smart-Folders** (neu): Einmalig in einer einzelnen Datei im mrdocument-Stammverzeichnis konfiguriert. Erstellt einen Ordner an einem beliebigen Ort — auch ausserhalb des mrdocument-Verzeichnisbaums — mit Links zu passenden Dateien aus allen Blattordnern eines Kontexts.

## Root-Smart-Folders einrichten

Erstelle eine Datei namens `smartfolders.yaml` im mrdocument-Stammverzeichnis (dasselbe Verzeichnis, das `incoming/`, `sorted/` usw. enthaelt).

### Beispiel

```yaml
smart_folders:
  rechnungen_alle:
    context: arbeit
    path: /home/user/Desktop/Rechnungen
    condition:
      field: type
      value: Rechnung

  briefe:
    context: privat
    path: briefe_sammlung
    condition:
      field: type
      value: Brief
    filename_regex: "\.pdf$"
```

Das erzeugt:
- `/home/user/Desktop/Rechnungen/` — enthaelt Links zu allen "Rechnung"-Dokumenten aus dem Kontext "arbeit".
- `{mrdocument_root}/briefe_sammlung/` — enthaelt Links zu allen PDF-"Brief"-Dokumenten aus dem Kontext "privat".

### Konfigurationsfelder

| Feld | Pflicht | Beschreibung |
|------|---------|--------------|
| `context` | Ja | Welcher Kontext beruecksichtigt wird (z.B. `arbeit`, `privat`). |
| `path` | Ja | Wo der Smart Folder erstellt wird. Absolute Pfade funktionieren ueberall im System. Relative Pfade beziehen sich auf das mrdocument-Stammverzeichnis. |
| `condition` | Mindestens eins von condition oder filename_regex | Dokumente nach Metadaten filtern. Siehe "Bedingungen" unten. |
| `filename_regex` | Mindestens eins von condition oder filename_regex | Dokumente nach Dateiname filtern (Regex-Suche, Gross-/Kleinschreibung egal). |

### Bedingungen

**Einfacher Feldvergleich** — trifft zu, wenn das Metadatenfeld dem Wert entspricht (Regex, Gross-/Kleinschreibung egal):
```yaml
condition:
  field: type
  value: Rechnung
```

**AND** — alle Teilbedingungen muessen zutreffen:
```yaml
condition:
  operator: and
  operands:
    - field: type
      value: Rechnung
    - field: sender
      value: Schulze.*
```

**OR** — mindestens eine Teilbedingung muss zutreffen:
```yaml
condition:
  operator: or
  operands:
    - field: type
      value: Rechnung
    - field: type
      value: Angebot
```

**NOT** — negiert eine Bedingung:
```yaml
condition:
  operator: not
  operands:
    - field: type
      value: Vertrag
```

## Was passiert

### Wenn ein Dokument verarbeitet und einsortiert wird
Wenn Kontext und Metadaten eines Dokuments zu den Bedingungen eines Root-Smart-Folders passen, wird automatisch ein symbolischer Link im Verzeichnis des Smart Folders erstellt, der auf die Datei in `sorted/` zeigt.

### Wenn sich die Metadaten eines Dokuments aendern
Wenn ein Dokument in `sorted/` verschoben oder umbenannt wird (was die Metadaten aktualisiert), werden alle Smart Folders neu ausgewertet. Links werden in neu passenden Ordnern erstellt und aus nicht mehr passenden entfernt.

### Wenn die Konfiguration geaendert wird
Aenderungen an `smartfolders.yaml` werden automatisch erkannt. Das System laedt die Konfiguration neu und wertet alle Smart Folders im naechsten Zyklus erneut aus.

### Namenskollisionen
Wenn zwei Dokumente aus verschiedenen Blattordnern denselben Dateinamen haben, erhaelt das erste den Symlink. In der Praxis ist das selten, da zugewiesene Dateinamen Kontext und Metadaten enthalten.

### Aufraeumen
Das System raeumt regelmaessig auf:
- Defekte Symlinks (Zieldatei wurde geloescht oder verschoben) werden entfernt.
- Nur Symlinks, die in `sorted/` zeigen, werden angefasst. Manuell platzierte Symlinks oder Dateien, die woanders hinzeigen, bleiben unangetastet.
- Regulaere Dateien im Smart-Folder-Verzeichnis werden nie angefasst.

## Konfigurationsdateien

Alle Konfigurationsdateien liegen im mrdocument-Stammverzeichnis (dasselbe Verzeichnis, das `incoming/`, `sorted/` usw. enthaelt), sofern nicht anders angegeben.

| Datei | Zweck | Pflicht |
|-------|-------|---------|
| `sorted/{kontext}/context.yaml` | Definiert einen Kontext: Name, Metadatenfelder, Dateinamensmuster, Ordnerhierarchie. Eine Datei pro Kontext. | Ja (mind. ein Kontext) |
| `sorted/{kontext}/smartfolders.yaml` | Definiert Kontext-Smart-Folders (Symlink-Unterverzeichnisse in den Blattordnern von `sorted/`). | Nein |
| `contexts.yaml` | Legacy-Fallback: Indexdatei, die Kontext-YAML-Dateien im Stammverzeichnis auflistet (z.B. `["arbeit.yaml", "privat.yaml"]`). Wird nur verwendet, wenn keine `sorted/{kontext}/context.yaml`-Dateien existieren. | Nein (Legacy) |
| `smartfolders.yaml` | Root-Smart-Folders an beliebigen Pfaden. Siehe oben "Root-Smart-Folders einrichten". | Nein |
| `stt.yaml` | Einstellungen fuer Audio-Transkription: Sprache, Modell, Diarisierung. Erforderlich fuer die Verarbeitung von Audiodateien — ohne diese Datei werden Audiodateien uebersprungen. | Nur fuer Audio |

### Kontext-Konfiguration (`sorted/{kontext}/context.yaml`)

```yaml
name: arbeit
description: Geschaeftliche Dokumente
filename: "{context}-{type}-{date}-{sender}"
audio_filename: "{context}-{date}-{sender}-{type}"
folders:
  - context
  - sender

fields:
  type:
    instructions: "Bestimme den Dokumenttyp."
    candidates:
      - "Rechnung"
      - "Vertrag"
    allow_new_candidates: false
  sender:
    instructions: "Bestimme den Absender."
    candidates: []
    allow_new_candidates: true
```

- `filename` / `audio_filename`: Muster fuer den zugewiesenen Dateinamen. Felder in `{Klammern}` werden durch Metadatenwerte ersetzt.
- `folders`: Welche Metadatenfelder die Ordnerhierarchie unter `sorted/{kontext}/` bestimmen.
- `fields`: Metadatenfelder, die die KI extrahiert. `candidates` listet bekannte Werte; `allow_new_candidates` steuert, ob die KI neue vorschlagen darf.

### Kontext-Smart-Folders (`sorted/{kontext}/smartfolders.yaml`)

```yaml
smart_folders:
  rechnungen:
    condition:
      field: type
      value: Rechnung
```

Gleiches Format fuer condition/filename_regex wie Root-Smart-Folders (siehe oben), aber ohne `context` oder `path` — der Kontext ergibt sich aus dem Verzeichnis, und Symlinks werden in Unterverzeichnissen der Blattordner platziert.

### Audio-Konfiguration (`stt.yaml`)

```yaml
language: de-DE
elevenlabs_model: scribe_v2
enable_diarization: true
diarization_speaker_count: 2
```

Wenn diese Datei fehlt, werden Audiodateien in `incoming/` komplett uebersprungen.

## Kurzuebersicht: Ordnerverhalten

| Ordner | Was kommt rein | Was passiert |
|--------|---------------|--------------|
| `incoming/` | Neue Dateien ablegen | Automatisch klassifiziert, verarbeitet und einsortiert. Original wird archiviert. |
| `sorted/` | Nicht anfassen (vom System verwaltet) | Endgueltiger Speicherort verarbeiteter Dokumente. Umbenennen oder Verschieben innerhalb von sorted/ ist moeglich — das System passt sich an. |
| `reviewed/` | Dateien nach manueller Pruefung hierhin verschieben | System sortiert sie anhand ihres Dateinamens in `sorted/` ein. |
| `trash/` | Dateien zum Loeschen hierhin verschieben | Alle Kopien (Quelle + verarbeitete Datei) werden nach `void/` verschoben. |
| `archive/` | Nicht anfassen (vom System verwaltet) | Originale Quelldateien werden hier dauerhaft aufbewahrt. |
| `processed/` | Nicht anfassen (Legacy) | Zwischenspeicher fuer Verarbeitungsergebnisse vor dem Einsortieren. |
| `error/` | Bei Problemen pruefen | Fehlgeleitete Dateien und fehlgeschlagene Verarbeitungsergebnisse landen hier. |
| `void/` | Nicht anfassen | Archiv geloeschter Dateien. Wird vom System nicht ueberwacht. |
| Smart Folder (Root-Ebene) | Nur-Lesen-Ansicht | Symbolische Links zu passenden Dokumenten. Eigene Dateien koennen dort abgelegt werden — sie werden nicht angefasst. |
