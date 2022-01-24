from src.iiif_harvesting import getNewspaperManifests
from src.iiif_conversion import parseMetadata, setup_requests
from pathlib import Path
import pickle
import requests
from progress.bar import ChargingBar
import urllib3
import re
import time
from timeit import default_timer
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import asyncio
# import click
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# @click.command()
# @click.option('--url', default='https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial', help='IIIF Collection URL')
# @click.option('--fname', prompt='Filename', help='The person to greet.')
def loadManifestURLsFromPickle(url, cwd, http, fname):
    '''
    Braucht entweder eine IIIF-Collection URL oder eine Liste mit URLs als Pickle Datei
    '''
    if url is not None:
        logger.info(f"Getting Newspaper URLs from {fname}")
        newspaper_urls = getNewspaperManifests(url, http)
    else:
        if Path(cwd, 'cache', fname).exists():
            with open(Path(cwd, 'cache', fname), 'rb') as f:
                newspaper_urls = pickle.load(f)
                logger.info("Loaded urls from pickled file")
        else:
            logger.error(f"Keine Datei {Path(cwd, 'cache', fname)} gefunden und keine IIIF-Collection URL übergeben.")
            newspaper_urls = []

    logger.info(f"{len(newspaper_urls)} Newspaper Issues")
    return newspaper_urls


async def get_data_asynchronous(urls, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder, threads):

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
                    *(url, session, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder) # Allows us to pass in multiple arguments to `parseMetadata`
                )
                for url in urls
            ]

            for url in await asyncio.gather(*tasks):
                pass
            logger.debug(
                f"Vergangene Zeit: {round((default_timer() - START_TIME) / 60, 2)} Minuten")


def start(newspaper_urls, cwd, http, metsfolder, altofolder, threads):
    '''
    Übergibt die URLs der IIIF Manifeste und andere zuvor gesammelte Variablen der
    '''
    print(f"Generating METS Files with {threads} Threads.")

    # Inbfos aus dem Cache laden

    if Path(cwd, 'cache', 'newspaperdata.pkl').exists():
        with open(Path(cwd, 'cache', 'newspaperdata.pkl'), 'rb') as f:
            newspaper = pickle.load(f)
            logger.info(f"Loaded {len(newspaper)} newspaperdata from pickled file")
    else:
        newspaper = []

    # Wenn wir das als Update laufen lassen dann wollen wir ja ggf. nicht alle METS Dateien neu erzeugen lassen
    # sondern nur die zu den IDs die wir noch nicht haben

    if Path(cwd, 'cache', 'ids_of_generated_mets.txt').exists():
        alreadygeneratedids = [line.rstrip('\n') for line in open(Path(cwd, 'cache', 'ids_of_generated_mets.txt'))]
        # alreadygeneratedids = []
    else:
        alreadygeneratedids = []

    issues = []

    # ----------------------------------------------------------------
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(newspaper_urls, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder, threads))
    loop.run_until_complete(future)
    # ----------------------------------------------------------------
    # Cleanup: Infos pickeln
    with open(Path(cwd, 'cache', 'newspaperdata.pkl'), 'wb') as f:
            pickle.dump(newspaper, f)
            logger.info(f"Wrote {len(newspaper)} newspaperdata to pickled file")

if __name__ == '__main__':
    cwd = Path.cwd()

    # Log initialisieren
    logname = Path(cwd, time.strftime("%Y-%m-%d_%H%M") + "_iiimets" + ".log")
    PARAMETER = logger.level("PARAMETER", no=38, color="<blue>")
    logger.add(
            logname,
            level=0,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            enqueue=True
        )

    http = setup_requests()

    # IIIF Manifest-URLs bestimmen: Entweder Abruf über die Collection oder bereits gecachte aus einer gepickelten Liste lesen.

    # newspaper_urls = loadManifestURLsFromPickle('https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial', cwd, http, 'one_url_for_every_newspaper.pkl')
    newspaper_urls = loadManifestURLsFromPickle(None, cwd, http, 'newspaper_urls.pkl')
    if len(newspaper_urls) == 0:
        sys.exit()
    # Folder Creation

    metsfolder = Path(cwd, '_METS', time.strftime("%Y-%m-%d"))
    if metsfolder.exists():
        pass
    else:
        metsfolder.mkdir()

    altofolder = Path(cwd, '_OCR', time.strftime("%Y-%m-%d"))
    if altofolder.exists():
        pass
    else:
        altofolder.mkdir()

    # Cache lesen und Threading starten:

    start(newspaper_urls, cwd, http, metsfolder, altofolder, 1)


