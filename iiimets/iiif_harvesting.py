#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_harvesting.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:27
from datetime import datetime
import sys
import pickle
import time
from pathlib import Path
import pandas as pd
from pathlib import Path

"""
- Get first (https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial)
- for m in /manifests: Log @id
- work on /next: https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=AoIIP4AAACtic2IxMTczMzAwMg==
- total number is available
- return list with Manifest URLs
"""


def getIdentifier(collectionurl: str, session, logger) -> list:
    """ """
    # Die Liste nutzen wir nur zum Anzeigen des Fortschritts.
    manifests = []

    def getManifestURLs(response):
        apireturn = response.json()
        if len(apireturn["manifests"]) != 0:
            for i in apireturn["manifests"]:
                d = {}
                d["url"] = i["@id"]
                d["name"] = i["label"]
                manifests.append(d)
            # print(len(manifests))

    try:
        response = session.get(collectionurl, verify=False, timeout=(20, 80))
    except Exception as e:
        logger.error(f"The collection URL is not reachable: {e}")
        sys.exit()
    else:

        print(f'total number of Manifests: {response.json()["total"]}')

        # Jetzt kommt die eigentliche Schleife: Solange wir da nicht per break rausgehen, läuft die.
        # Immer mit neuer URL
        while True:
            # time.sleep(0.5)
            # Verbindungsversuch inkl. Errorhandling
            try:
                print(f" Getting data from {collectionurl}")
                response = session.get(collectionurl, verify=False, timeout=(20, 80))
            except Exception as e:
                logger.error(f"The collection URL is not reachable: {e}")
                break
            else:
                if response.status_code == 404:
                    if input != "":
                        sys.exit()
                    else:
                        sys.exit()
                else:
                    getManifestURLs(response)
                    try:
                        # schauen, ob es noch einen Token gibt oder ob wir aus der Schleife rausmüssen:
                        collectionurl = response.json()["next"]
                        # if len(manifests) > 300:
                        #     break
                    except:
                        # wir sind fertig und gehen per break aus dem while-Loop raus.
                        print(
                            f"Identifier Harvesting beendet. Insgesamt {len(manifests)} IDs bekommen."
                        )
                        break
        return manifests

def getNewspaperManifests(
    collectionurl: str, session, filterstring: str, cwd, logger
) -> list:
    """
    Gets IIIF Manifest URLs form IIIF Collection.
    """
    manifests = getIdentifier(collectionurl, session, logger)
    if manifests != None:
        df = pd.DataFrame.from_records(manifests)

        # df["name"].to_csv("manifest_names.csv")
        df.to_pickle("allmanifests.pkl")
        if filterstring is not None:
            logger.info(f"Filter Manifests by '{filterstring}'")
            logger.debug(f"Dataframe is {len(df)} rows.")
            newspaper_urls = df.query(f'name.str.contains("{filterstring}")', engine="python")[
                "url"
            ].to_list()
            logger.debug(f"{len(newspaper_urls)} rows kept.")
        else:
            newspaper_urls = df["url"].to_list()
            logger.debug(f"{len(newspaper_urls)} rows kept.")

        cache_picklefile = Path(cwd, "cache", "newspaper_urls.pkl")
        cache_picklefile.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_picklefile, "wb") as f:
            pickle.dump(newspaper_urls, f)
        logger.info(f'Pickled newspaper_urls to {str(cache_picklefile)}')
        return newspaper_urls
    else:
        sys.exit()


def generateNewspaperURLfromFile(fpath):
    import random

    newspaper_urls = [
        "https://api.digitale-sammlungen.de/iiif/presentation/v2/"
        + line.rstrip("\n")
        + "/manifest"
        for line in open(fpath)
    ]
    with open("newspaper_urls.pkl", "wb") as f:
        pickle.dump(random.sample(newspaper_urls, int(len(newspaper_urls) / 1000)), f)


if __name__ == "__main__":
    pass
