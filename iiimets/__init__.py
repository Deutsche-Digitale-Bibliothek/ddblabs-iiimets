import argparse
import asyncio
import pickle
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from timeit import default_timer

import requests
import urllib3
from loguru import logger
from pkg_resources import resource_filename
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .iiif_conversion import parseMetadata, setup_requests
from .iiif_harvesting import getListOfManifests
from .download_ocrxml import downloadhOCR, runXSLonFolder
from .convert_ocr import transformHOCR

# from .download_ocrxml import downloadhOCR, runXSLonFolder

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def parseargs():
    urlregex = (
        r"^(http|https)://((\S+):(\S+)@|.*)\.(com|de|org|edu|at|ch|fr|net|nrw).*$"
    )
    parser = argparse.ArgumentParser(
        description="Convert IIIF Manifest URLs to METS/MODS, download hOCR and convert to ALTO."
    )

    parser.add_argument("--url", dest="url", help="URL of IIIF Collection to harvest")
    parser.add_argument("--file", dest="file", help="Filename to pickeld URL list")
    parser.add_argument("--filter", dest="filter", help="String to filter Manifests by")
    parser.add_argument(
        "--no-cache",
        required=False,
        dest="cache",
        action="store_false",
        help="If set, no cache is used",
    )
    parser.add_argument(
        "--no-update",
        required=False,
        dest="update",
        action="store_false",
        help="If set, update functionality is disabled",
    )

    args = vars(parser.parse_args())
    if not any(args.values()):
        parser.error("No arguments provided.")
        sys.exit()
    else:
        url = args["url"]
        file = args["file"]
        filter = args["filter"]
        cache = args["cache"]
        update = args["update"]
        if url is not None:
            if re.match(urlregex, url):
                url = url
            else:
                parser.error("URL is not valid.")
        return url, file, cache, update, filter


def loadManifestURLsFromPickle(cwd: Path, fname: str, logger) -> list:
    if Path(Path(__file__).parent.parent, fname).exists():
        with open(Path(Path(__file__).parent.parent, fname), "rb") as f:
            newspaper_urls = pickle.load(f)
            logger.info("Loaded urls from pickled file")
    else:
        logger.error(
            f"Keine Datei {Path(Path(__file__).parent.parent, fname)} gefunden und keine IIIF-Collection URL übergeben."
        )
        newspaper_urls = []

    logger.info(f"{len(newspaper_urls)} Newspaper Issues")
    return newspaper_urls


async def get_data_asynchronous(
    urls, newspaper, issues, alreadygeneratedids, logger, metsfolder, threads
):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        with requests.Session() as session:
            retry_strategy = Retry(
                total=6,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
                backoff_factor=1,
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
                    *(
                        url,
                        session,
                        newspaper,
                        issues,
                        alreadygeneratedids,
                        logger,
                        Path(__file__).parent.parent,
                        metsfolder,
                    ),  # Allows us to pass in multiple arguments to `parseMetadata`
                )
                for url in urls
            ]

            for url in await asyncio.gather(*tasks):
                pass
            logger.debug(
                f"Vergangene Zeit: {round((default_timer() - START_TIME) / 60, 2)} Minuten"
            )


def start(
    newspaper_urls: list,
    cwd: Path,
    metsfolder: Path,
    threads: int,
    caching: bool,
    update: bool,
):
    """
    Übergibt die URLs der IIIF Manifeste und andere zuvor gesammelte Variablen
    parallelisiert der parseMetadata Funktion.
    """
    logger.debug(f"Generating METS Files with {threads} Threads.")

    # Infos aus dem Cache laden
    if caching is True:
        if Path(Path(__file__).parent.parent, "cache", "newspaperdata.pkl").exists():
            with open(
                Path(Path(__file__).parent.parent, "cache", "newspaperdata.pkl"), "rb"
            ) as f:
                newspaper = pickle.load(f)
                logger.info(f"Loaded {len(newspaper)} newspaperdata from pickled file")
        else:
            newspaper = []
    else:
        newspaper = []

    # Wenn wir das als Update laufen lassen dann wollen wir ja ggf. nicht alle METS Dateien neu erzeugen lassen
    # sondern nur die zu den IDs die wir noch nicht haben
    if update is True:
        logger.info("Running as Update")
        if Path(
            Path(__file__).parent.parent, "cache", "ids_of_generated_mets.txt"
        ).exists():
            alreadygeneratedids = [
                line.rstrip("\n")
                for line in open(
                    Path(
                        Path(__file__).parent.parent,
                        "cache",
                        "ids_of_generated_mets.txt",
                    )
                )
            ]
        else:
            Path(
                Path(__file__).parent.parent, "cache", "ids_of_generated_mets.txt"
            ).touch()
            logger.info(
                f"Created empty list of generated METS IDs in {Path(Path(__file__).parent.parent, 'cache', 'ids_of_generated_mets.txt')}"
            )
            alreadygeneratedids = []
    else:
        alreadygeneratedids = []

    issues = []

    # ----------------------------------------------------------------
    # For each URL in newspaperurls: get
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        get_data_asynchronous(
            newspaper_urls,
            newspaper,
            issues,
            alreadygeneratedids,
            logger,
            metsfolder,
            threads,
        )
    )
    loop.run_until_complete(future)
    # ----------------------------------------------------------------
    # Cleanup: Infos pickeln
    with open(
        Path(Path(__file__).parent.parent, "cache", "newspaperdata.pkl"), "wb"
    ) as f:
        pickle.dump(newspaper, f)
        logger.info(f"Wrote {len(newspaper)} newspaperdata to pickled file")


def main():
    # saxon JAR is need for the hOCR to ALTO Conversion
    # https://github.com/Saxonica/Saxon-HE/releases/download/SaxonHE12-3/SaxonHE12-3J.zip
    saxonpath = "java -jar " + resource_filename(__name__, "res/saxon.jar")
    # First Step: Parse Arguments from CLI call
    url, file, cache, update, filter = parseargs()
    # Initialize Logger
    logname = Path(
        Path(
            Path(__file__).parent.parent,
            time.strftime("%Y-%m-%d_%H%M") + "_iiimets" + ".log",
        )
    )
    # configure logger
    PARAMETER = logger.level("PARAMETER", no=38, color="<blue>")
    logger.add(
        logname,
        level=0,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        enqueue=True,
    )
    # Define filterstring: Wenn der String im Newspaper Titel vorkommt ist es eine Zeitung
    # TODO Wird grade nicht benutzt?
    # filterstring = "##"
    filterstring = None

    # Setup requests Session
    # FIXME rename
    http = setup_requests()
    date = time.strftime("%Y-%m-%d")

    # Get IIIF Manifest-URLs
    # Either harvest a IIIF Collection (WIP) or read Manifest URLs from list (pickled or text file)
    if file is None and url is None:
        sys.exit(
            "Error: You either need to pass an URL to an IIIF Collection or the Path to a file containg links to IIIF Manifests"
        )
    elif file is not None:
        # when we pass the path to a file containing IIIF Manifest URLs
        if file.endswith(".txt"):
            # if it’s a text file
            manifest_urls = [line.rstrip("\n") for line in open(file)]
        elif file.endswith(".pkl"):
            # if it ends with pkl we assume it’s pickled
            manifest_urls = loadManifestURLsFromPickle(
                Path(__file__).parent.parent, file, logger
            )
        else:
            sys.exit("Can’t use the input file.")
    elif url is not None:
        logger.info(f"Getting Manifest URLs from {url}")
        manifest_urls = getListOfManifests(
            url + "?cursor=initial", http, filter, Path(__file__).parent.parent, logger
        )
    # at this point manifest_urls contains a list of IIIF Manifest URLs
    if len(manifest_urls) == 0:
        logger.warning("No manifest URLs to process. Exit.")
        sys.exit()
    # ----------------------------------------------------
    # Folder Creation
    # ----------------------------------------------------
    # FIXME use Pathlib
    metsfolder_main = Path(Path(__file__).parent.parent, "_METS")
    if metsfolder_main.exists():
        pass
    else:
        metsfolder_main.mkdir()

    ocrfolder_main = Path(Path(__file__).parent.parent, "_OCR")
    if ocrfolder_main.exists():
        pass
    else:
        ocrfolder_main.mkdir()

    metsfolder = Path(Path(__file__).parent.parent, "_METS", date)
    if metsfolder.exists():
        pass
    else:
        metsfolder.mkdir()

    hocrfolder = Path(Path(__file__).parent.parent, "_OCR", date, "hOCR")
    if hocrfolder.exists():
        pass
    else:
        Path(Path(__file__).parent.parent, "_OCR", date).mkdir()
        hocrfolder.mkdir()

    altofolder = Path(hocrfolder.parent, "ALTO")
    if altofolder.exists():
        pass
    else:
        altofolder.mkdir()
    # ----------------------------------------------------
    # Processing
    # ----------------------------------------------------

    # IIIF Manifest URL -> METS/MODS XML
    start(manifest_urls, Path(__file__).parent.parent, metsfolder, 16, cache, update)

    # Compress generated METS files:
    # TODO The following steps need to be toggled by commandline options
    # shutil.make_archive(f'{date}_METS', 'zip', metsfolder)

    # ----------------------------------------------------
    # Process Fulltext XML

    # 1. Download linked hOCR files
    downloadhOCR(metsfolder, hocrfolder)
    # shutil.make_archive(f'{date}_hOCR', 'zip', hocrfolder)

    # 2. hORC to ALTO
    runXSLonFolder(hocrfolder, altofolder, Path(__file__).parent.parent, saxonpath)
    # logger.info('Erstelle ZIP Dateien')
    # shutil.make_archive(f'{date}_ALTO', 'zip', altofolder)

    # ----------------------------------------------------
    # Cleanup
    # logger.info('Clean up temp files')
    # shutil.rmtree(hocrfolder)
    # shutil.rmtree(altofolder)
    # shutil.rmtree(metsfolder)
    # shutil.rmtree(Path(Path(__file__).parent.parent, '_OCR', date))
    logger.info("Process completed")


if __name__ == "__main__":
    main()
