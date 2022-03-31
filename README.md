# IIIMETS

## Zusammenfassung

Dieses Script erzeugt aus einer Liste von URLs zu IIIF-Manifesten zu Zeitungsausgaben der BSB [Zeitungsportal-valide METS/MODS Dateien](https://wiki.deutsche-digitale-bibliothek.de/display/DFD/Ausgabe+Zeitung+1.0) für Zeitungsausgaben.

### Basics

- Threading
- Erweiterter Metadaten Abruf über das Manifest der Zeitung und die ZDB
- Caching der erweiterten Metadaten auf Zeitungsebene

### Update-Funktionalität 

Wenn die URL einer IIIF-Collection übergeben wird und die Update Funktionalität nicht deaktiviert wird (per `--no-update`) werden diejenigen Manifeste übersprungen, für die schon einmal METS/MODS Dateien erstellt wurden.

## Installation

Benutzung einer [venv](https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/) ist zu empfehlen.

Braucht zur Laufzeit das System-Paket `openjdk-8-jre-headless`.

Das Python-Paket und seine Abhängigkeiten installiert man per


    pip install iiimets


## Benutzung


    iiimets --file=misc/bsb_newspapers_01.pkl --no-update --no-cache

(`--no-update` um alle Daten neu zu ziehen, nicht bereits gezogene zu überspringen)

