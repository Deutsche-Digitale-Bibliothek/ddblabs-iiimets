"""
Lädt alle in einer METS Datei in der Filegroup FULLTEXT verlinkten
Volltexte herunter. Liste die Links aus einem Ordner von METS Dateien aus.

TODO: Konversion der hOCR Dateien in ALTO mit Saxon

"""

import asyncio
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from timeit import default_timer

import lxml.etree as ET
from loguru import logger
from pkg_resources import resource_filename
from requests_futures.sessions import FuturesSession

from .helpers import setup_requests

NAMESPACES = {
    "alto": "http://www.loc.gov/standards/alto/ns-v3#",
    "xlink": "http://www.w3.org/1999/xlink",
    "mets": "http://www.loc.gov/METS/",
    "mods": "http://www.loc.gov/mods/v3",
}


def download_hocr(metsfolder, outfolder):
    start_time = default_timer()
    all_mets_files = [
        os.path.join(metsfolder, x) for x in os.listdir(metsfolder) if x.endswith("xml")
    ]

    alto_urls = []
    logger.info(f"Parse {len(all_mets_files)} Dateien nach Volltext Links")

    for mets_file in all_mets_files:
        tree = ET.parse(bytes(mets_file, encoding="utf8"))
        for alto_link in tree.findall(
            ".//mets:fileGrp[@USE='FULLTEXT']/mets:file[@MIMETYPE = 'text/xml']",
            NAMESPACES,
        ):
            for flocat in alto_link:
                altourl = flocat.attrib["{%s}href" % NAMESPACES["xlink"]]
                alto_urls.append(altourl)

    logger.info(f"Starte Download von {len(alto_urls)} urls")

    with FuturesSession(max_workers=8) as session:
        futures = [
            session.get(url, headers={"User-agent": "iiimets"}) for url in alto_urls
        ]
        for future in as_completed(futures):
            response = future.result()
            url = response.request.url
            response.encoding = "utf-8"
            data = response.text
            if response.status_code != 200:
                logger.critical(f"Statuscode {response.status_code} bei {url}")
            else:
                filename = re.sub(
                    r"https://api.digitale-sammlungen.de/ocr/(.+)/(.+)",
                    r"\1_\2.xml",
                    url,
                )
                with open(
                    os.path.join(outfolder, filename), "w", encoding="utf8"
                ) as of:
                    of.write(data)

    logger.debug(
        f"Vergangene Zeit für den OCR Abruf: {round((default_timer() - start_time) / 60, 2)} Minuten"
    )

    # Change Links
    for f in all_mets_files:
        with open(f, "r+", encoding="utf8") as fl:
            cont = fl.read()
            cont = re.sub(
                r'https://api.digitale-sammlungen.de/ocr/(.+?)/(.+)"',
                r'\1_\2.xml"',
                cont,
            )
            fl.write(cont)
    logger.info("Anpassen der Links in der fileGroup FULLTEXT erfolgt.")
