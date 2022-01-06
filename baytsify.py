from iiif_harvesting import getNewspaperManifests
from iiif_conversion import convertManifestsToMETS, parseMetadata, setup_requests
from pathlib import Path
import pickle
from piou import Cli, Option
from progress.bar import ChargingBar
import urllib3
import re
import time
import sys
from loguru import logger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# cli = Cli(description='A CLI tool')

# cli.add_option('-q', '--quiet', help='Do not output any message')


# @cli.command(cmd='start',
#              help='Run command')
# def foo_main(
#         quiet: bool,
#         url: str = Option(..., '-u', '--url', help='IIIF Collection URL')
# ):
#     urlregex = r"^(http|https)://((\S+):(\S+)@|.*)\.(com|de|org|edu|at|ch|fr|net|nrw).*$"
#     validurl = re.search(urlregex, url)
#     # https://regex101.com/r/2CCthx/1

#     if validurl is None:
#         sys.exit(f'{url} ist keine valide URL.')
#     else:
#         startbaytsing(url)

def startbaytsing(url, cwd):
    http = setup_requests()
    logger.info("Getting Newspaper URLs")

    if Path(cwd, 'newspaper_urls.pkl').exists():
        with open(Path(cwd, 'newspaper_urls.pkl'), 'rb') as f:
            newspaper_urls = pickle.load(f)
            logger.info("Loaded urls from pickled file")
    else:
        newspaper_urls = getNewspaperManifests(url, http)

    logger.info(len(newspaper_urls))

    print("Generating METS Files")

    newspaper = []
    issues = []
    # Wenn wir das als Update laufen lassen dann wollen wir ja ggf. nicht alle METS Dateien neu erzeugen lassen
    # sondern nur die zu den IDs die wir noch nicht haben
    if Path(cwd, 'ids_of_generated_mets.txt').exists():
        # alreadygeneratedids = [line.rstrip('\n') for line in open(Path(cwd, 'ids_of_generated_mets.txt'))]
        alreadygeneratedids = []
    else:
        alreadygeneratedids = []
    for u in newspaper_urls:
        parseMetadata(u, http, newspaper, issues, alreadygeneratedids, logger, cwd)

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
    startbaytsing('https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial', cwd)


