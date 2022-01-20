from iiif_harvesting import getNewspaperManifests
from iiif_conversion import parseMetadata, setup_requests
from pathlib import Path
import pickle
import requests
from progress.bar import ChargingBar
import urllib3
import re
import time
from timeit import default_timer
import sys
sys.path.append('/home/cloud/python-saxon')
import saxonc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import asyncio
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def loadManifestURLsFromPickle(url, cwd, http, fname):

    logger.info(f"Getting Newspaper URLs from {fname}")

    if Path(cwd, fname).exists():
        with open(Path(cwd, fname), 'rb') as f:
            newspaper_urls = pickle.load(f)
            logger.info("Loaded urls from pickled file")
    else:
        newspaper_urls = getNewspaperManifests(url, http)

    logger.info(f"{len(newspaper_urls)} Newspaper Issues")
    return newspaper_urls


async def get_data_asynchronous(urls, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder, threads, proc, xsltproc):

    with ThreadPoolExecutor(max_workers=threads) as executor:
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
                    parseMetadata,
                    *(url, session, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder, proc, xsltproc) # Allows us to pass in multiple arguments to `parseMetadata`
                )
                for url in urls
            ]

            for url in await asyncio.gather(*tasks):
                pass
            logger.debug(
                f"Vergangene Zeit: {round((default_timer() - START_TIME) / 60, 2)} Minuten")

def start(newspaper_urls, cwd, http, metsfolder, altofolder, threads, proc, xsltproc):

    print(f"Generating METS Files with {threads} Threads.")

    if Path(cwd, 'newspaperdata.pkl').exists():
        with open(Path(cwd, 'newspaperdata.pkl'), 'rb') as f:
            newspaper = pickle.load(f)
            logger.info(f"Loaded {len(newspaper)} newspaperdata from pickled file")
    else:
        newspaper = []

    issues = []
    # Wenn wir das als Update laufen lassen dann wollen wir ja ggf. nicht alle METS Dateien neu erzeugen lassen
    # sondern nur die zu den IDs die wir noch nicht haben
    if Path(cwd, 'ids_of_generated_mets.txt').exists():
        alreadygeneratedids = [line.rstrip('\n') for line in open(Path(cwd, 'ids_of_generated_mets.txt'))]
        # alreadygeneratedids = []
    else:
        alreadygeneratedids = []

    # ----------------------------------------------------------------
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(newspaper_urls, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder, threads, proc, xsltproc))
    loop.run_until_complete(future)
    # ----------------------------------------------------------------
    with open('newspaperdata.pkl', 'wb') as f:
            pickle.dump(newspaper, f)
            logger.info(f"Wrote {len(newspaper)} newspaperdata to pickled file")

if __name__ == '__main__':
    # cli.run()
    cwd = Path.cwd()
    logname = Path(cwd, time.strftime("%Y-%m-%d_%H%M") + "_baytisify" + ".log")
    PARAMETER = logger.level("PARAMETER", no=38, color="<blue>")
    logger.add(
            logname,
            level=0,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            enqueue=True
        )
    http = setup_requests()

    # newspaper_urls = getNewspaperManifests('https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial', cwd, http)
    newspaper_urls = loadManifestURLsFromPickle('https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial', cwd, http, 'newspaper_urls.pkl')

    metsfolder = Path(cwd, 'METS', time.strftime("%Y-%m-%d"))
    if metsfolder.exists():
        pass
    else:
        metsfolder.mkdir()

    altofolder = Path(cwd, 'ALTO', time.strftime("%Y-%m-%d"))
    if altofolder.exists():
        pass
    else:
        altofolder.mkdir()
    # --------------------------------------------------
    proc = saxonc.PySaxonProcessor(license=False)
    xsltproc = proc.new_xslt_processor()
    xsltproc.compile_stylesheet(stylesheet_file="ocr_conversion/hOCR2ALTO.xsl")
    start(newspaper_urls, cwd, http, metsfolder, altofolder, 1, proc, xsltproc)


