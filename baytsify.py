from iiif_harvesting import getNewspaperManifests
from iiif_conversion import convertManifestsToMETS, parseMetadata, setup_requests
from pathlib import Path
import pickle

http = setup_requests()

print("Getting Newspaper URLs")

if Path('/Users/karl/Coding/baytsify/newspaper_urls.pkl').exists():
    print("loaded urls from pickled file")
    with open('/Users/karl/Coding/baytsify/newspaper_urls.pkl', 'rb') as f:
        newspaper_urls = pickle.load(f)
else:
    newspaper_urls = getNewspaperManifests('https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial', http)

print(len(newspaper_urls))

print("Generating METS Files")

newspaper = []
issues = []

for u in newspaper_urls:
    parseMetadata(u, http, newspaper, issues)
