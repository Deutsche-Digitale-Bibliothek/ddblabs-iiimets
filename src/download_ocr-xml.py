'''
Lädt alle in einer METS Datei in der Filegroup FULLTEXT verlinkten
Volltexte herunter. Liste die Links aus einem Ordner von METS Dateien aus.

TODO: Konversion der hOCR Dateien in ALTO mit Saxon

'''

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
import os
from timeit import default_timer
import lxml.etree as ET
import sys
import time
import re

START_TIME = default_timer()

def fetch(session, url, outfolder):

    with session.get(url) as response:
        response.encoding = 'utf-8'
        data = response.text
        if response.status_code != 200:
            logger.critical(f"Statuscode {response.status_code} bei {url}")
        else:
            filename = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+)/(.+)', r'\1_\2.xml', url)
            with open(os.path.join(outfolder, filename), "w", encoding="utf8") as of:
                of.write(data)
        return url

async def get_data_asynchronous(alto_urls, outfolder):

    with ThreadPoolExecutor(max_workers=16) as executor:
        with requests.Session() as session:
            retry_strategy = Retry(
                total=6,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
                backoff_factor=1
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            loop = asyncio.get_event_loop()
            START_TIME = default_timer()
            tasks = [
                loop.run_in_executor(
                    executor,
                    fetch,
                    *(session, url, outfolder) # Allows us to pass in multiple arguments to `fetch`
                )
                for url in alto_urls
            ]



def main(alto_urls, outfolder):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(alto_urls, outfolder))
    loop.run_until_complete(future)
    logger.debug(f"Vergangene Zeit für den OCR Abruf: {round((default_timer() - START_TIME) / 60, 2)} Minuten")

# -----------------------------------------------------------------------------
logger.remove()
logname = time.strftime("%Y-%m-%d_%H%M") + ".log"
# Einen stderr-Logger initialisieren
logger.add(sys.stderr, format="<green>{time}</green> {level} {message}")
# Einen Logger initialisieren, der in eine Datei im ausgewhlten Verzeichnis schreibt
# logger.add(
#     logname,
#     format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>", encoding="utf8")
# -----------------------------------------------------------------------------

namespaces = {"alto": "http://www.loc.gov/standards/alto/ns-v3#", "xlink": "http://www.w3.org/1999/xlink",
              "mets": "http://www.loc.gov/METS/", "mods": "http://www.loc.gov/mods/v3"}

folder = '/Users/karl/Coding/iiimets/_METS/2022-01-25'
outfolder = '/Users/karl/Coding/iiimets/_OCR/2022-01-25'
files = [os.path.join(folder, x) for x in os.listdir(folder) if x.endswith('xml')]

alto_urls = []
logger.info(f"Parse {len(files)} Dateien nach Volltext Links")
for f in files:
    tree = ET.parse(bytes(f,encoding='utf8'))
    fulltextMets_file = tree.findall(f".//mets:fileGrp[@USE='FULLTEXT']/mets:file[@MIMETYPE = 'text/xml']", namespaces)
    for e in fulltextMets_file:
        for flocat in e:
            altourl = flocat.attrib['{http://www.w3.org/1999/xlink}href']
            alto_urls.append(altourl)

logger.info(f"Starte Download von {len(alto_urls)} urls")

main(alto_urls, outfolder)

# Change Links
# for f in files:
#     with open(f, 'r+', encoding='utf8') as fl:
#         cont = fl.read()
#         cont = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+?)/(.+)"', r'\1_\2.xml"', cont)
#         fl.write(cont)


# TODO hOCR zu ALTO transformieren
