# MrDocument -- Benutzerhandbuch

## Was ist MrDocument?

MrDocument ist ein KI-gestuetztes Dokumentenmanagementsystem. Sie legen Dateien in einen Ordner, und MrDocument erledigt automatisch:

1. Klassifizierung des Dokuments (bestimmt Kontext, Typ, Absender, Datum usw.).
2. Umbenennung nach einem einheitlichen Namensmuster.
3. Einsortierung in die richtige Ordnerstruktur.
4. Erstellung von Smart-Folder-Links fuer schnellen Zugriff.
5. Bei Audio-/Videodateien: Transkription und Verarbeitung des Transkripts.

Sie muessen lediglich Ihre Kontexte konfigurieren (welche Arten von Dokumenten Sie haben) und Dateien ablegen.


## Erste Schritte

### Ordnerstruktur

Ihr MrDocument-Stammverzeichnis enthaelt folgende Ordner:

```
mrdocument/
  incoming/       <-- Neue Dateien hier ablegen
  processed/      <-- KI-klassifizierte Dateien zur Ueberpruefung
  reviewed/       <-- Freigegebene Dateien, warten auf Einsortierung
  sorted/         <-- Endablage, nach Kontext organisiert
  archive/        <-- Originaldateien (dauerhafte Aufbewahrung)
  error/          <-- Dateien, deren Verarbeitung fehlgeschlagen ist
  duplicates/     <-- Duplikate von Quelldateien
  trash/          <-- Dateien hier ablegen zum Loeschen
  void/           <-- Systemabfall (geloeschte/verwaiste Dateien)
  missing/        <-- Quelldateien, deren verarbeitete Ergebnisse fehlen
```


## Wie Dateien verarbeitet werden

### Die Standard-Pipeline

```
incoming/ --> processed/ --> reviewed/ --> sorted/
```

1. **Legen Sie eine Datei in `incoming/` ab.** Jeder unterstuetzte Dateityp funktioniert.
2. **MrDocument verarbeitet sie.** Die KI extrahiert Metadaten (Kontext, Datum, Typ, Absender usw.) und benennt die Datei um. Das Ergebnis erscheint in `processed/`.
3. **Ueberpruefen Sie die Datei in `processed/`.** Kontrollieren Sie, ob Klassifizierung und Dateiname korrekt sind. Wenn zufrieden, verschieben Sie sie nach `reviewed/`.
4. **MrDocument sortiert sie ein.** Die Datei wird automatisch von `reviewed/` an den richtigen Ort unter `sorted/` verschoben, gemaess Ihrer konfigurierten Ordnerhierarchie.

Die Originaldatei wird immer in `archive/` aufbewahrt.

### Schnelles Einsortieren (ohne Ueberpruefung)

Sie koennen den Ueberpruefungsschritt ueberspringen, indem Sie Dateien direkt in `sorted/` ablegen:

- **Richtiger Kontextordner:** Ablegen in `sorted/{kontext}/`. MrDocument klassifiziert und platziert die Datei im richtigen Unterordner.
- **Beliebiger Ort in sorted/:** MrDocument erkennt den Kontext aus dem Ordnerpfad und klassifiziert entsprechend.

### Was in jedem Ordner passiert

| Ordner | Was Sie tun | Was MrDocument tut |
|--------|------------|-------------------|
| `incoming/` | Neue Dateien hier ablegen | Klassifiziert, benennt um, verschiebt Ergebnis nach `processed/`. Original kommt ins `archive/`. |
| `processed/` | KI-Klassifizierung ueberpruefen | Nichts -- wartet darauf, dass Sie nach `reviewed/` verschieben. |
| `reviewed/` | Freigegebene Dateien hierhin verschieben | Sortiert in `sorted/{kontext}/{unterordner}/` ein. |
| `sorted/` | Organisierte Dateien durchsuchen; Dateien zum Schnellsortieren ablegen | Klassifiziert und benennt um; verwaltet Smart-Folder-Links. |
| `archive/` | Nur-Lese-Referenz | Speichert alle Originaldateien dauerhaft. |
| `trash/` | Dateien hier ablegen zum Loeschen | Verschiebt alle zugehoerigen Dateien nach `void/` und entfernt den Datensatz. |
| `error/` | Fehlgeschlagene Dateien pruefen | Nicht verarbeitbare Dateien landen hier. Zurueck nach `incoming/` verschieben zum Wiederholen. |
| `duplicates/` | Duplikate pruefen | Wenn dieselbe Quelldatei mehrfach erscheint, kommen Kopien hierhin. |

### Umbenennung und Wiederherstellung

- Wenn Sie **eine Datei in `sorted/` umbenennen**, uebernimmt MrDocument den neuen Dateinamen.
- Wenn Sie **eine Datei in einen anderen Kontextordner** in `sorted/` verschieben, aktualisiert MrDocument den Kontext.
- Wenn eine verarbeitete Datei aus `sorted/` **verschwindet**, markiert MrDocument sie als fehlend. Taucht die Datei wieder auf, wird sie automatisch erkannt.
- Wenn die Verarbeitung **fehlschlaegt**, wird die Quelldatei nach `error/` verschoben. Verschieben Sie sie zurueck nach `incoming/` zum Wiederholen.


## Unterstuetzte Dateitypen

### Dokumente
PDF, DOCX, RTF, TXT, Markdown, EML (E-Mail), HTML und gaengige Bildformate (JPG, PNG, GIF, TIFF, BMP, WebP).

### Audio und Video
FLAC, WAV, MP3, OGG, WebM, MP4, M4A, MKV, AVI, MOV.

Audio- und Videodateien werden per Spracherkennung transkribiert, dann wird das Transkript als TXT-Datei klassifiziert und einsortiert. Ein Symlink zur Original-Audiodatei wird neben dem Transkript platziert.

### Nicht unterstuetzte Dateien
Dateien mit nicht unterstuetzten Endungen (Schriftarten, Tabellenkalkulationen, Binaerdateien usw.) werden automatisch nach `error/` verschoben.


## Konfigurationsdateien

### Kontext-Konfiguration: `sorted/{kontext}/context.yaml`

Dies ist die zentrale Konfigurationsdatei. Jeder Kontext repraesentiert eine Dokumentenkategorie (z.B. "arbeit", "privat", "gesundheit").

```yaml
name: arbeit
description: "Geschaeftliche Dokumente wie Rechnungen, Vertraege und Angebote"

filename: "{context}-{type}-{date}-{sender}"
audio_filename: "{context}-{date}-{sender}-{type}"

fields:
  type:
    instructions: "Bestimme den Dokumenttyp anhand des Inhalts."
    candidates:
      - "Rechnung"
      - "Vertrag"
      - "Angebot"
    allow_new_candidates: false

  sender:
    instructions: "Bestimme den Absender oder die Organisation."
    candidates:
      - "Schulze GmbH"
      - "Fischer AG"
    allow_new_candidates: true

filename_keywords:
  - "schulze"
  - "keller"

folders:
  - "context"
  - "sender"
```

**Felder erklaert:**

| Feld | Zweck |
|------|-------|
| `name` | Eindeutiger Kontext-Bezeichner. Muss dem Ordnernamen in `sorted/` entsprechen. |
| `description` | Hilft der KI zu verstehen, worum es in diesem Kontext geht. |
| `filename` | Vorlage fuer die Umbenennung von Dateien. Verwendet `{feldname}`-Platzhalter. |
| `audio_filename` | Alternative Vorlage fuer Audio-Transkripte (optional). |
| `fields` | Metadatenfelder, die die KI extrahieren soll. Jedes hat Anweisungen und Kandidatenwerte. |
| `allow_new_candidates` | Wenn `true`, kann die KI Werte vorschlagen, die nicht in der Kandidatenliste stehen. |
| `filename_keywords` | Schluesselwoerter, die im Dokumentinhalt gesucht und in den Dateinamen aufgenommen werden. |
| `folders` | Bestimmt die Unterordner-Hierarchie unter `sorted/{kontext}/`. |

**Neuen Kontext hinzufuegen:** Erstellen Sie `sorted/{neuer_kontext}/context.yaml`. MrDocument erkennt ihn automatisch.

**Kontext aendern:** Bearbeiten Sie die YAML-Datei. MrDocument laedt beim naechsten Zyklus neu.

**Kontext entfernen:** Loeschen Sie den Ordner aus `sorted/`. Vorhandene Dateien werden nicht beruehrt.

### Smart Folders: `sorted/{kontext}/smartfolders.yaml`

Smart Folders erstellen Symlinks zu Dokumenten, die bestimmte Bedingungen erfuellen, und bieten Ihnen so mehrere Sichten auf dieselben Dateien.

```yaml
smart_folders:
  rechnungen:
    condition:
      field: "type"
      value: "Rechnung"

  keller_rechnungen:
    condition:
      operator: "and"
      operands:
        - field: "type"
          value: "Rechnung"
        - field: "sender"
          value: "Keller.*"
    filename_regex: "2025"
```

**Bedingungen:**
- **Einfach:** `{field: "type", value: "Rechnung"}` -- trifft zu, wenn das Typ-Feld "Rechnung" entspricht (Regex, Gross-/Kleinschreibung egal).
- **AND (Und):** Alle Unterbedingungen muessen zutreffen.
- **OR (Oder):** Mindestens eine Unterbedingung muss zutreffen.
- **NOT (Nicht):** Die Unterbedingung darf nicht zutreffen.
- **filename_regex:** Zusaetzlicher Filter auf den Dateinamen (optional, Gross-/Kleinschreibung egal).

Smart Folders erscheinen als Unterverzeichnisse mit Symlinks. Die eigentlichen Dateien bleiben an ihrem urspruenglichen Ort.

### Root-Level Smart Folders: `smartfolders.yaml`

Root-Level Smart Folders platzieren Symlinks an beliebigen Orten ausserhalb von `sorted/`.

```yaml
smart_folders:
  - name: rechnungen
    context: arbeit
    path: /home/user/Desktop/Rechnungen
    condition:
      field: "type"
      value: "Rechnung"

  - name: arztbriefe
    context: privat
    path: ./arztbriefe
    condition:
      field: "type"
      value: "Arztbrief"
```

- `path` kann absolut oder relativ zum MrDocument-Stammverzeichnis sein.
- Jeder Eintrag muss angeben, fuer welchen `context` er gilt.
- Das Verzeichnis wird bei Bedarf automatisch erstellt.

### STT-Konfiguration: `stt.yaml`

Erforderlich fuer die Verarbeitung von Audio-/Videodateien. Im MrDocument-Stammverzeichnis ablegen.

```yaml
language: "de-DE"
elevenlabs_model: "scribe_v2"
enable_diarization: true
diarization_speaker_count: 2
```

| Feld | Standard | Beschreibung |
|------|----------|-------------|
| `language` | `de-DE` | Sprache fuer die Transkription. |
| `elevenlabs_model` | `scribe_v2` | Zu verwendendes STT-Modell. |
| `enable_diarization` | `true` | Verschiedene Sprecher identifizieren. |
| `diarization_speaker_count` | `2` | Erwartete Anzahl der Sprecher. |

Ohne diese Datei werden Audiodateien uebersprungen (nach `error/` verschoben).

### Generierte Daten: `sorted/{kontext}/generated.yaml`

Diese Datei wird automatisch von MrDocument verwaltet. Sie speichert neue Kandidaten und Hinweise, die waehrend der Verarbeitung entdeckt wurden.

- **Kandidaten:** Neue Feldwerte, die die KI entdeckt hat (wenn `allow_new_candidates: true`).
- **Hinweise (Clues):** Erkenntnisse der KI ueber Feldwerte (z.B. "Rechnungen von Schulze erwaehnen meist 'Projekt Alpha'").

Sie koennen diese Datei bearbeiten, um fehlerhafte Vorschlaege zu entfernen, aber normalerweise wird sie vom System verwaltet.


## Audio- und Videodateien

### Verarbeitungsablauf

1. Legen Sie eine Audio-/Videodatei in `incoming/` ab.
2. MrDocument sendet sie an den Spracherkennungsdienst zur Transkription.
3. Bei Dateien mit "intro" im Dateinamen: Eine Zwei-Pass-Transkription wird mit verbesserter Genauigkeit durchgefuehrt.
4. Das Transkript wird als `.txt`-Datei klassifiziert und einsortiert.
5. Ein Symlink zur Original-Audiodatei wird neben dem Transkript platziert.

### Audio-Links

Nach der Transkription sehen Sie zwei Dateien in `sorted/`:
```
sorted/arbeit/Schulze_GmbH/
  arbeit-Besprechung-2025-03-01-Schulze_GmbH.txt     <-- Transkript
  arbeit-Besprechung-2025-03-01-Schulze_GmbH.m4a     <-- Symlink zu archive/original.m4a
```

Der Audio-Link folgt dem Transkript: Wenn Sie das Transkript umbenennen oder verschieben, wird der Link automatisch aktualisiert.

### Intro-Zwei-Pass-Verfahren

Dateien mit "intro" im Dateinamen erhalten eine erweiterte Verarbeitung:
1. Erster Transkriptionspass (Standard).
2. KI klassifiziert das Transkript, um besseren Kontext, Schluesselwoerter und Sprecheranzahl zu ermitteln.
3. Zweiter Transkriptionspass mit verbesserten Parametern.

Dies liefert deutlich bessere Ergebnisse fuer Aufnahmen, die mit einem Einfuehrungssegment beginnen.


## Smart Folders im Detail

### Kontext-Level Smart Folders

Befinden sich innerhalb der Sorted-Ordnerstruktur. Beispiel:

```
sorted/arbeit/Schulze_GmbH/
  rechnungen/                          <-- Smart Folder
    arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf  --> ../arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf
  arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf     <-- Eigentliche Datei
  arbeit-Vertrag-2025-02-01-Schulze_GmbH.pdf      <-- Nicht in rechnungen (Typ != Rechnung)
```

### Root-Level Smart Folders

An beliebigen Pfaden platziert. Beispiel:

```
/home/user/Desktop/Rechnungen/
  arbeit-Rechnung-2025-01-15-Schulze_GmbH.pdf  --> ../../mrdocument/sorted/arbeit/Schulze_GmbH/arbeit-...pdf
```

### Wichtige Hinweise

- Smart Folders enthalten **nur Symlinks**. Die eigentlichen Dateien bleiben in `sorted/`.
- Sie koennen eigene Dateien in Smart-Folder-Verzeichnisse legen -- MrDocument fasst Nicht-Symlink-Dateien nie an.
- Defekte oder veraltete Symlinks werden automatisch bereinigt.
- Wenn eine Datei die Bedingung eines Smart Folders nicht mehr erfuellt (z.B. Metadaten geaendert), wird ihr Symlink entfernt.


## Duplikat-Behandlung

Wenn dieselbe Quelldatei mehrfach erscheint:
- Die erste Kopie wird normal verarbeitet.
- Weitere Kopien werden nach `duplicates/` verschoben, wobei die urspruengliche Pfadstruktur erhalten bleibt.

Beispiel: Wenn `incoming/rechnung.pdf` ein Duplikat einer bereits verarbeiteten Datei ist:
```
duplicates/incoming/rechnung.pdf
```


## Fehlerbehandlung

### Fehlgeschlagene Verarbeitung

Wenn die KI-Verarbeitung fehlschlaegt:
1. Die Quelldatei wird nach `error/` verschoben.
2. Zum Wiederholen: Verschieben Sie die Datei von `error/` zurueck nach `incoming/`.

### Fehlende Dateien

Wenn eine verarbeitete Datei aus `sorted/` verschwindet:
1. MrDocument markiert den Datensatz als "fehlend".
2. Taucht die Datei an ihrem erwarteten Ort wieder auf, wird sie automatisch erkannt.
3. Wenn Sie eine neue Kopie der Quelldatei in `incoming/` ablegen, wird sie erneut verarbeitet.

### Wiederherstellung fehlender Dateien

Wenn eine verarbeitete Datei fehlt, wird ihre Quelldatei von `archive/` nach `missing/` verschoben, damit Sie sie leicht finden koennen. Zum erneuten Verarbeiten verschieben Sie die Quelldatei von `missing/` zurueck nach `incoming/`.


## Kostenverfolgung

MrDocument verfolgt KI-API-Nutzungskosten pro Benutzer. Kostendaten werden geschrieben nach:
```
/costs/{benutzername}/mrdocument_costs.json
```

Die Datei enthaelt tageweise und kumulierte Nutzungsdaten:
- Eingabe- und Ausgabe-Tokens pro Modell.
- Kosten in USD pro Modell.
- Anzahl verarbeiteter Dokumente.
- Kosten pro Dokument.


## Mehrbenutzer-Unterstuetzung

MrDocument unterstuetzt mehrere Benutzer gleichzeitig. Jeder Benutzer hat eigene:
- Stammverzeichnisse (z.B. `/sync/alice/mrdocument/`).
- Kontext-Konfigurationen.
- Smart-Folder-Definitionen.
- Datenbankeintraege (nach Benutzername isoliert).
- Kostenverfolgung.

Neue Benutzerverzeichnisse werden automatisch erkannt, sobald sie unter dem Sync-Stammverzeichnis erscheinen.


## Tipps

- **Stapelverarbeitung:** Legen Sie mehrere Dateien gleichzeitig in `incoming/` ab. MrDocument verarbeitet sie parallel (standardmaessig bis zu 5).
- **Schnelle Klassifizierung:** Legen Sie Dateien direkt in `sorted/{kontext}/` ab, um den Ueberpruefungsschritt zu ueberspringen.
- **Kontext aus Ordner:** Wenn Sie eine Datei in `sorted/arbeit/` ablegen, weiss MrDocument, dass es ein Geschaeftsdokument ist, und beruecksichtigt nur diesen Kontext.
- **Dateinamenkonventionen:** Die KI beachtet Ihre Dateinamenvorlage. Daten werden als JJJJ-MM-TT formatiert. Sonderzeichen und Umlaute werden normalisiert.
- **Konfiguration im laufenden Betrieb:** Bearbeiten Sie `context.yaml` oder `smartfolders.yaml` und MrDocument uebernimmt die Aenderungen automatisch -- kein Neustart noetig.
- **Syncthing-kompatibel:** MrDocument verwendet atomare Dateioperationen und ignoriert temporaere Syncthing-Dateien, was es sicher fuer synchronisierte Ordner macht.
