import sys
from pathlib import Path
import pickle
from concurrent.futures import ThreadPoolExecutor
import re
import time
from timeit import default_timer
import asyncio
import argparse
import shutil
import requests
from requests.adapters import HTTPAdapter
from progress.bar import ChargingBar
import urllib3
from urllib3.util.retry import Retry
from loguru import logger

from .iiif_harvesting import getNewspaperManifests
from .iiif_conversion import parseMetadata, setup_requests
# from .download_ocrxml import downloadhOCR, runXSLonFolder

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def parseargs():
    urlregex = r"^(http|https)://((\S+):(\S+)@|.*)\.(com|de|org|edu|at|ch|fr|net|nrw).*$"
    parser = argparse.ArgumentParser(
        description='Convert IIIF Manifest URLs to METS/MODS, download hOCR and convert to ALTO.'
    )

    parser.add_argument('--url', dest='url', help='URL of IIIF Collection to harvest')
    parser.add_argument('--file', dest='file', help='Filename to pickeld URL list')
    parser.add_argument('--no-cache', required=False, dest='cache', action='store_false', help='If set, no cache is used')
    parser.add_argument('--no-update', required=False, dest='update', action='store_false', help='If set, update functionality is disabled')

    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error('No arguments provided.')
        sys.exit()
    else:
        url = args['url']
        file = args['file']
        cache = args['cache']
        update = args['update']
        if url is not None:
            if re.match(urlregex, url):
                url = url
            else:
                parser.error('URL is not valid.')
        return url, file, cache, update

def loadManifestURLsFromPickle(url: str, cwd: Path, http: requests.session, fname: str, filter, logger) -> list:
    '''
    Braucht entweder eine IIIF-Collection URL oder eine Liste mit URLs als Pickle Datei
    '''

    if Path(cwd, fname).exists():
        with open(Path(cwd, fname), 'rb') as f:
            newspaper_urls = pickle.load(f)
            logger.info("Loaded urls from pickled file")
    else:
        logger.error(f"Keine Datei {Path(cwd, fname)} gefunden und keine IIIF-Collection URL übergeben.")
        newspaper_urls = []

    logger.info(f"{len(newspaper_urls)} Newspaper Issues")
    return newspaper_urls


async def get_data_asynchronous(urls, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, threads):

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
                    *(url, session, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder) # Allows us to pass in multiple arguments to `parseMetadata`
                )
                for url in urls
            ]

            for url in await asyncio.gather(*tasks):
                pass
            logger.debug(
                f"Vergangene Zeit: {round((default_timer() - START_TIME) / 60, 2)} Minuten")


def start(newspaper_urls: list, cwd: Path, metsfolder: Path, threads: int, caching: bool, update: bool):
    '''
    Übergibt die URLs der IIIF Manifeste und andere zuvor gesammelte Variablen der
    '''
    logger.debug(f"Generating METS Files with {threads} Threads.")

    # Inbfos aus dem Cache laden
    if caching == True:
        if Path(cwd, 'cache', 'newspaperdata.pkl').exists():
            with open(Path(cwd, 'cache', 'newspaperdata.pkl'), 'rb') as f:
                newspaper = pickle.load(f)
                logger.info(f"Loaded {len(newspaper)} newspaperdata from pickled file")
        else:
            newspaper = []
    else:
        newspaper = []

    # Wenn wir das als Update laufen lassen dann wollen wir ja ggf. nicht alle METS Dateien neu erzeugen lassen
    # sondern nur die zu den IDs die wir noch nicht haben
    if update == True:
        logger.info("Running as Update")
        if Path(cwd, 'cache', 'ids_of_generated_mets.txt').exists():
            alreadygeneratedids = [line.rstrip('\n') for line in open(Path(cwd, 'cache', 'ids_of_generated_mets.txt'))]
        else:
            Path(cwd, 'cache', 'ids_of_generated_mets.txt').touch()
            logger.info("")
            alreadygeneratedids = []
    else:
        alreadygeneratedids = []

    issues = []

    # ----------------------------------------------------------------
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(newspaper_urls, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, threads))
    loop.run_until_complete(future)
    # ----------------------------------------------------------------
    # Cleanup: Infos pickeln
    with open(Path(cwd, 'cache', 'newspaperdata.pkl'), 'wb') as f:
            pickle.dump(newspaper, f)
            logger.info(f"Wrote {len(newspaper)} newspaperdata to pickled file")

def main():
    cwd = Path.cwd()
    saxonpath = 'java -jar saxon-he-10.6.jar'
    # IIIF Manifest-URLs bestimmen: Entweder Abruf über die Collection oder bereits gecachte aus einer gepickelten Liste lesen.

    url, file, cache, update = parseargs()
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
    date = time.strftime("%Y-%m-%d")
    if file is None and url is None:
        sys.exit("You need either an URL to an IIIF Collection or a the Path to a file containg links to IIIF Manifests")
    elif file is not None:
        if file.endswith('.txt'):
            newspaper_urls = [line.rstrip('\n') for line in open(file)]
        else:
            newspaper_urls = loadManifestURLsFromPickle(url, cwd, http, file, '##', logger)
    elif url is not None:
        logger.info(f"Getting Newspaper URLs from {url}")
        newspaper_urls = getNewspaperManifests(url, http, filter, cwd, logger)

    if len(newspaper_urls) == 0:
        sys.exit()

    # Folder Creation

    metsfolder_main = Path(cwd, '_METS')
    if metsfolder_main.exists():
        pass
    else:
        metsfolder_main.mkdir()

    ocrfolder_main = Path(cwd, '_OCR')
    if ocrfolder_main.exists():
        pass
    else:
        ocrfolder_main.mkdir()

    metsfolder = Path(cwd, '_METS', date)
    if metsfolder.exists():
        pass
    else:
        metsfolder.mkdir()

    hocrfolder = Path(cwd, '_OCR', date, 'hOCR')
    if hocrfolder.exists():
        pass
    else:
        Path(cwd, '_OCR', date).mkdir()
        hocrfolder.mkdir()

    altofolder = Path(hocrfolder.parent, 'ALTO')
    if altofolder.exists():
        pass
    else:
        altofolder.mkdir()

    # Cache lesen und Threading starten:

    start(newspaper_urls, cwd, metsfolder, 16, cache, update)

    # erstellte METS Dateien zippen:

    shutil.make_archive(f'{date}_METS', 'zip', metsfolder)
    # downloadhOCR(metsfolder, hocrfolder)
    # shutil.make_archive(f'{date}_hOCR', 'zip', hocrfolder)
    # runXSLonFolder(hocrfolder, altofolder, cwd, saxonpath)
    # logger.info('Erstelle ZIP Dateien')
    # shutil.make_archive(f'{date}_ALTO', 'zip', altofolder)
    # Cleanup
    # logger.info('Starte Cleanup')
    # shutil.rmtree(hocrfolder)
    # shutil.rmtree(altofolder)
    # shutil.rmtree(metsfolder)
    # shutil.rmtree(Path(cwd, '_OCR', date))
    logger.info('Vorgang abgeschlossen')

if __name__ == '__main__':
    main()
