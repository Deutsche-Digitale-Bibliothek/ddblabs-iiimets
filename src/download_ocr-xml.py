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

START_TIME = default_timer()

def fetch(session, url):

    with session.get(url) as response:
        data = response.text
        if response.status_code != 200:
            logger.critical(f"Statuscode {response.status_code} bei {url}")
        else:
            with open(os.path.join(os.getcwd(), "downloaded", url.split("/")[-1] + ".xml"), "w", encoding="utf8") as of:
                of.write(data)
        return url


async def get_data_asynchronous(alto_urls):

    with ThreadPoolExecutor(max_workers=16) as executor:
        with requests.Session() as session:
            retry_strategy = Retry(
                total=6,
                status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["GET"],
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
                    *(session, url) # Allows us to pass in multiple arguments to `fetch`
                )
                for url in alto_urls
            ]

            for url in await asyncio.gather(*tasks):
                pass
                # with open(os.path.join(os.getcwd(), "downloaded", url.split("/")[-1] + ".xml"), "w", encoding="utf8") as of:
                #     of.write(response)
            logger.debug(
                f"Vergangene Zeit: {round((default_timer() - START_TIME) / 60, 2)} Minuten")

def main(alto_urls):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(alto_urls))
    loop.run_until_complete(future)


namespaces = {"alto": "http://www.loc.gov/standards/alto/ns-v3#", "xlink": "http://www.w3.org/1999/xlink",
              "mets": "http://www.loc.gov/METS/", "mods": "http://www.loc.gov/mods/v3"}

files = [os.path.join(
    os.getcwd(), "data", x) for x in os.listdir(os.path.join(
        os.getcwd(), "data")) if x.endswith('xml')]

alto_urls = []
logger.remove()
logname = time.strftime("%Y-%m-%d_%H%M") + ".log"
# Einen stderr-Logger initialisieren
logger.add(sys.stderr, format="<green>{time}</green> {level} {message}")
# Einen Logger initialisieren, der in eine Datei im ausgewhlten Verzeichnis schreibt
logger.add(
    logname,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>", encoding="utf8")

for f in files:
    tree = ET.parse(bytes(f,encoding='utf8'))
    fulltextMets_file = tree.findall(f".//mets:fileGrp[@USE='FULLTEXT']/mets:file[@MIMETYPE = 'text/xml']", namespaces)

    for e in fulltextMets_file:
        for flocat in e:
            altourl = flocat.attrib['{http://www.w3.org/1999/xlink}href']
            alto_urls.append(altourl)
logger.info(f"Starte Download von {len(alto_urls)} urls")
main(alto_urls)

