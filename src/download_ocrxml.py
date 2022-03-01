'''
Lädt alle in einer METS Datei in der Filegroup FULLTEXT verlinkten
Volltexte herunter. Liste die Links aus einem Ordner von METS Dateien aus.

TODO: Konversion der hOCR Dateien in ALTO mit Saxon

'''

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import asyncio
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from loguru import logger
import os
import subprocess
from timeit import default_timer
import lxml.etree as ET
import sys
import time
import re
from pathlib import Path



def runXSLonFolder(hocrfolder, altofolder, cwd, saxonpath):

    xsl = Path(cwd, 'src', 'xslt', 'hOCR2ALTO.xsl')
    subprocessargs = f"{saxonpath} -s:{hocrfolder} -o:{altofolder} -xsl:{xsl}".split(' ')
    logger.info(f"Starte Saxon XSLT Processing")
    try:
        transformationoutput = subprocess.check_output(subprocessargs, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        transformationoutput = str(e.output, 'utf-8')
        if len(re.findall(r'\d+\stransformations failed', transformationoutput)) == 1:
            logger.warning(re.findall(r'\d+\stransformations failed', transformationoutput)[0])




def setup_requests() -> requests.Session:
        """Sets up a requests session to automatically retry on errors

        cf. <https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/>

        Returns
        -------
        http : requests.Session
            A fully configured requests Session object
        """
        http = requests.Session()
        assert_status_hook = (
            lambda response, *args, **kwargs: response.raise_for_status()
        )
        http.hooks["response"] = [assert_status_hook]
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        return http

START_TIME = default_timer()

metsfolder = "/home/cloud/storage/iiimets/_METS/2022-02-28"
outfolder = "/home/cloud/storage/iiimets/_OCR/2022-02-28/hOCR"
namespaces = {"alto": "http://www.loc.gov/standards/alto/ns-v3#", "xlink": "http://www.w3.org/1999/xlink",
            "mets": "http://www.loc.gov/METS/", "mods": "http://www.loc.gov/mods/v3"}

files = [os.path.join(metsfolder, x) for x in os.listdir(metsfolder) if x.endswith('xml')]

alto_urls = []
logger.info(f"Parse {len(files)} Dateien nach Volltext Links")
for f in files[:100]:
    tree = ET.parse(bytes(f,encoding='utf8'))
    fulltextMets_file = tree.findall(f".//mets:fileGrp[@USE='FULLTEXT']/mets:file[@MIMETYPE = 'text/xml']", namespaces)
    for e in fulltextMets_file:
        for flocat in e:
            altourl = flocat.attrib['{http://www.w3.org/1999/xlink}href']
            alto_urls.append(altourl)

logger.info(f"Starte Download von {len(alto_urls)} urls")

with FuturesSession(max_workers=16) as session:
    futures = [session.get(url) for url in alto_urls]
    for future in as_completed(futures):
        response = future.result()
        url = response.request.url
        print(url)
        response.encoding = 'utf-8'
        data = response.text
        if response.status_code != 200:
            logger.critical(f"Statuscode {response.status_code} bei {future}")
        else:
            filename = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+)/(.+)', r'\1_\2.xml', url)
            with open(os.path.join(outfolder, filename), "w", encoding="utf8") as of:
                of.write(data)


logger.debug(f"Vergangene Zeit für den OCR Abruf: {round((default_timer() - START_TIME) / 60, 2)} Minuten")

# Change Links
# for f in files:
#     with open(f, 'r+', encoding='utf8') as fl:
#         cont = fl.read()
#         cont = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+?)/(.+)"', r'\1_\2.xml"', cont)
#         fl.write(cont)
# logger.info(f"Anpassen der Links in der fileGroup FULLTEXT erfolgt.")
