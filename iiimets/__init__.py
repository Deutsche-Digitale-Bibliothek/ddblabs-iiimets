import argparse
import asyncio
import pickle
import re
import shutil
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from timeit import default_timer

import requests
import urllib3
from loguru import logger
from pkg_resources import resource_filename
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .convert_ocr import run_xsl_on_folder, transformHOCR
from .download_ocrxml import download_hocr
from .helpers import setup_requests
from .iiif_conversion import iiif_to_metsmods
from .iiif_harvesting import getListOfManifests

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
    parser.add_argument(
        "--outputfolder",
        dest="output_folder_main",
        help="Folder to save the extracted Data to",
        default=None,
    )
    parser.add_argument(
        "--filter", dest="filter", help="String to filter Manifests by", default="##"
    )
    parser.add_argument(
        "--no-cache",
        required=False,
        dest="cache",
        action="store_false",
        help="If set, no cache is used",
    )
    parser.add_argument(
        "--no-fulltext-processing",
        required=False,
        dest="fulltext",
        action="store_false",
        help="If set, no processing of fulltext XML is done",
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
        filterstring = args["filter"]
        cache = args["cache"]
        fulltext = args["fulltext"]
        update = args["update"]
        output_folder_main = args["output_folder_main"]
        if url is not None:
            if re.match(urlregex, url):
                url = url
            else:
                parser.error("URL is not valid.")
        return url, file, cache, update, filterstring, output_folder_main, fulltext


def load_manifest_urls_from_pickle(cwd: Path, fname: str, logger) -> list:
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
                    iiif_to_metsmods,
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


def start_iiif_to_metsmods_conversion(
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

    list_of_processed_issues = []

    # ----------------------------------------------------------------
    # Start parallel processing of URLs (unnecessarily complex/bloated)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        get_data_asynchronous(
            newspaper_urls,
            newspaper,
            list_of_processed_issues,
            alreadygeneratedids,
            logger,
            metsfolder,
            threads,
        )
    )
    loop.run_until_complete(future)
    # ----------------------------------------------------------------
    # Cleanup: Pickle metadata about the newspapers (not the issues!) to use as a cache in subsequent runs
    with open(
        Path(Path(__file__).parent.parent, "cache", "newspaperdata.pkl"), "wb"
    ) as f:
        pickle.dump(newspaper, f)
        logger.info(f"Wrote {len(newspaper)} newspaperdata to pickled file")


def main():
    # First Step: Parse Arguments from CLI call
    (
        url,
        file,
        cache,
        update,
        filterstring,
        output_folder_main,
        fulltext_processing,
    ) = parseargs()

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
    # Setup requests Session
    REQUESTS_SESSION = setup_requests()
    DATE = time.strftime("%Y-%m-%d")
    # check if saxon.jar is present
    if fulltext_processing is True:
        res_folder = Path(Path(__file__).parent, "res")
        saxon_files = res_folder.glob("saxon-he-1*")
        saxon_file = next(saxon_files, None)
        if saxon_file is not None:
            saxon_file_name = saxon_file
            saxonpath = "java -jar " + str(saxon_file_name)
        else:
            logger.info(
                f"To run hOCR to ALTO Conversion you need a saxon.jar file in the folder {Path(Path(__file__), 'res')}."
            )
            latest_saxon_he_release = REQUESTS_SESSION.get(
                "https://api.github.com/repos/Saxonica/Saxon-HE/releases"
            ).json()[0]["assets"][2]["browser_download_url"]
            # download file to res folder
            logger.info(f"Downloading {latest_saxon_he_release}")
            r = REQUESTS_SESSION.get(latest_saxon_he_release, stream=True)
            with open(Path(Path(__file__).parent, "res", "saxon.zip"), "wb") as f:
                for chunk in r.iter_content(chunk_size=128):
                    f.write(chunk)
            # Extract only files ending in ".jar" from the ZIP file
            with zipfile.ZipFile(
                Path(Path(__file__).parent, "res", "saxon.zip")
            ) as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith(".jar"):
                        if file_name.startswith("saxon-he-1"):
                            saxon_file_name = file_name
                            zip_ref.extract(
                                file_name, Path(Path(__file__).parent, "res")
                            )
            # delete zip file
            Path(Path(__file__).parent, "res", "saxon.zip").unlink()
            # check now if saxon.jar is present
            if not Path(Path(__file__).parent, "res", saxon_file_name).exists():
                logger.info("Couldn’t download saxon.jar. Please download it manually.")
                sys.exit()
            else:
                saxonpath = "java -jar " + str(
                    Path(Path(__file__).parent, "res", saxon_file_name)
                )

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
            manifest_urls = load_manifest_urls_from_pickle(
                Path(__file__).parent.parent, file, logger
            )
        else:
            sys.exit("Can’t use the input file.")
    elif url is not None:
        logger.info(f"Getting Manifest URLs from {url}")
        manifest_urls = getListOfManifests(
            url + "?cursor=initial",
            REQUESTS_SESSION,
            filter,
            Path(__file__).parent.parent,
            logger,
        )
    # at this point manifest_urls contains a list of IIIF Manifest URLs
    if len(manifest_urls) == 0:
        logger.warning("No manifest URLs to process. Exit.")
        sys.exit()
    # ----------------------------------------------------
    # Outputfolder Creation
    # ----------------------------------------------------
    if not output_folder_main:
        output_folder_main = Path(__file__).parent.parent

    metsfolder_main = Path(output_folder_main, "_METS")
    metsfolder_main.mkdir(parents=True, exist_ok=True)

    ocrfolder_main = Path(output_folder_main, "_OCR")
    ocrfolder_main.mkdir(parents=True, exist_ok=True)

    metsfolder = Path(output_folder_main, "_METS", DATE)
    metsfolder.mkdir(parents=True, exist_ok=True)

    hocrfolder = Path(output_folder_main, "_OCR", DATE, "hOCR")
    hocrfolder.mkdir(parents=True, exist_ok=True)

    altofolder = Path(output_folder_main, "_OCR", DATE, "ALTO")
    altofolder.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------
    # Processing
    # ----------------------------------------------------

    # IIIF Manifest URL -> METS/MODS XML
    start_iiif_to_metsmods_conversion(
        manifest_urls, Path(__file__).parent.parent, metsfolder, 16, cache, update
    )
    # Compress generated METS files:
    shutil.make_archive(f"{DATE}_METS", "zip", metsfolder)

    # ----------------------------------------------------
    # Process Fulltext XML
    if fulltext_processing is True:
        # 1. Download linked hOCR files
        download_hocr(metsfolder, hocrfolder)
        shutil.make_archive(f"{DATE}_hOCR", "zip", hocrfolder)

        # 2. hORC to ALTO
        run_xsl_on_folder(
            hocrfolder, altofolder, Path(__file__).parent.parent, saxonpath, logger
        )
        shutil.make_archive(f"{DATE}_ALTO", "zip", altofolder)

        shutil.rmtree(hocrfolder)
        shutil.rmtree(altofolder)

    # ----------------------------------------------------
    # Cleanup
    logger.info("Clean up temp files")
    shutil.rmtree(metsfolder)
    shutil.rmtree(Path(Path(__file__).parent.parent, "_OCR", DATE))
    logger.info("Process completed")


if __name__ == "__main__":
    main()
