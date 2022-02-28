from src.iiif_harvesting import getNewspaperManifests
from src.iiif_conversion import parseMetadata, setup_requests
from src.download_ocrxml import downloadhOCR, runXSLonFolder
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
import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if __name__ == '__main__':
    cwd = Path.cwd()
    saxonpath = 'java -jar saxon-he-10.6.jar'
    # IIIF Manifest-URLs bestimmen: Entweder Abruf über die Collection oder bereits gecachte aus einer gepickelten Liste lesen.

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
    hocrfolder = '/home/cloud/iiimets/_OCR/2022-01-30/hOCR'
    altofolder = '/home/cloud/iiimets/_OCR/2022-01-30/ALTO'
    metsfolder = '/home/cloud/iiimets/_METS/2022-01-30'
    runXSLonFolder(hocrfolder, altofolder, cwd, saxonpath)
    # erstelle ZIPs
    logger.info('Erstelle ZIP Dateien')
    shutil.make_archive(f'{date}_ALTO', 'zip', altofolder)
    shutil.make_archive(f'{date}_METS', 'zip', metsfolder)
    # Cleanup
    logger.info('Starte Cleanup')

