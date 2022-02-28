# IIIMETS

## Zusammenfassung

Dieses Script erzeugt aus einer Liste von URLs zu IIIF-Manifesten zu Zeitungsausgaben der BSB Zeitungsportal-valide METS/MODS Dateien für Zeitungsausgaben.

### Basics

- Threading
- Erweiterter Metadaten Abruf über das Manifest der Zeitung und die ZDB

### Update-Funktionalität 

Die URL einer IIIF-Collection

### Caching

Metadaten der Zeitungen (auch die, die aus der ZDB abgerufen werden)

### Nachgelagert: hOCR zu ALTO

Harvesting of IIIF Manifests and conversion to METS/MODS of the Bayrische Staatsbibliothek IIIF Collection of digitized newspapers

## Benutzung

```
python3 iiimets.py --file=/home/cloud/storage/iiimets/misc/bsb_newspapers_01.pkl --no-update
```

`--no-update` um alle Daten neu zu ziehen, nicht bereits gezogene zu überspringen