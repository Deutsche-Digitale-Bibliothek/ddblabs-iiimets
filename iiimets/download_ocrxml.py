'''
Lädt alle in einer METS Datei in der Filegroup FULLTEXT verlinkten
Volltexte herunter. Liste die Links aus einem Ordner von METS Dateien aus.

TODO: Konversion der hOCR Dateien in ALTO mit Saxon

'''

import sys
import time
import os
import re
from timeit import default_timer
from pathlib import Path
from pkg_resources import resource_filename
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import subprocess
import asyncio
import requests
from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry
from loguru import logger
import lxml.etree as ET

HOCR2ALTO = resource_filename(__name__, 'res/xslt/hOCR2ALTO.xsl')

def runXSLonFolder(hocrfolder, altofolder, cwd, saxonpath):

    subprocessargs = f"{saxonpath} -s:{hocrfolder} -o:{altofolder} -xsl:{HOCR2ALTO}".split(' ')
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


NAMESPACES = {"alto": "http://www.loc.gov/standards/alto/ns-v3#",
              "xlink": "http://www.w3.org/1999/xlink",
              "mets": "http://www.loc.gov/METS/",
              "mods": "http://www.loc.gov/mods/v3"}

def downloadhOCR(metsfolder="_METS/2022-02-28", outfolder="_OCR/2022-02-28/hOCR"):
    start_time = default_timer()
    files = [os.path.join(metsfolder, x) for x in os.listdir(metsfolder) if x.endswith('xml')]

    alto_urls = []
    logger.info(f"Parse {len(files)} Dateien nach Volltext Links")
    # FIXME: why only first 100 files?
    for mets in files[:100]:
        tree = ET.parse(bytes(mets, encoding='utf8'))
        for alto in tree.findall(".//mets:fileGrp[@USE='FULLTEXT']/mets:file[@MIMETYPE = 'text/xml']", NAMESPACES):
            for flocat in alto:
                altourl = flocat.attrib['{%s}href' % NAMESPACES['xlink']]
                alto_urls.append(altourl)

    logger.info(f"Starte Download von {len(alto_urls)} urls")

    with FuturesSession(max_workers=8) as session:
        futures = [session.get(url, headers={'User-agent': 'iiimets'}) for url in alto_urls]
        for future in as_completed(futures):
            response = future.result()
            url = response.request.url
            response.encoding = 'utf-8'
            data = response.text
            if response.status_code != 200:
                # print(response.headers)
                logger.critical(f"Statuscode {response.status_code} bei {url}")
            else:
                filename = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+)/(.+)', r'\1_\2.xml', url)
                with open(os.path.join(outfolder, filename), "w", encoding="utf8") as of:
                    of.write(data)

    logger.debug(f"Vergangene Zeit für den OCR Abruf: {round((default_timer() - start_time) / 60, 2)} Minuten")

# Change Links
# for f in files:
#     with open(f, 'r+', encoding='utf8') as fl:
#         cont = fl.read()
#         cont = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+?)/(.+)"', r'\1_\2.xml"', cont)
#         fl.write(cont)
# logger.info(f"Anpassen der Links in der fileGroup FULLTEXT erfolgt.")
