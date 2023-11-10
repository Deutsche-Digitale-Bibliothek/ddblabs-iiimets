#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_harvesting.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:27
import pickle
import sys
from pathlib import Path

import pandas as pd
import requests


def getIdentifier(url: str, session: requests.Session, logger) -> list:
    """
    - Get first (https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial)
    - for m in /manifests: Log @id
    - work on /next: https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=AoIIP4AAACtic2IxMTczMzAwMg==
    - total number is available
    - return list with Manifest URLs
    """
    manifests = []
    print("getIdentifier with url: ", url)

    def getManifestURLs(response):
        apireturn = response.json()
        if len(apireturn["manifests"]) != 0:
            for i in apireturn["manifests"]:
                d = {}
                d["url"] = i["@id"]
                d["name"] = i["label"]
                manifests.append(d)

    try:
        response = session.get(url, verify=False, timeout=(20, 80))
    except Exception as e:
        logger.error(f"The collection URL is not reachable: {e}")
        sys.exit()
    else:
        try:
            response.json()["total"]
        except:
            pass
        else:
            print(f'Total number of Manifests: {response.json()["total"]}')

        # Jetzt kommt die eigentliche Schleife: Solange wir da nicht per break rausgehen, läuft die.
        # Immer mit neuer URL
        while True:
            # time.sleep(0.5)
            # Verbindungsversuch inkl. Errorhandling
            if len(manifests) > 50:
                break
            try:
                response = session.get(url, verify=False, timeout=(20, 80))
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
                        url = response.json()["next"]
                        # This is for testing purposes
                        # if len(manifests) > 300:
                        #     break
                    except:
                        # wir sind fertig und gehen per break aus dem while-Loop raus.
                        logger.info(
                            f"Identifier Harvesting beendet. Insgesamt {len(manifests)} IDs bekommen."
                        )
                        break
        # manifests is a list of dictionaries with keys `url` and `name` of resource
        return manifests


def getListOfManifests(
    url: str, session: requests.Session, filter: str, cwd: Path, logger
) -> list:
    """
    - Retrieve Manifest URLs form IIIF Collection URL
    - Filter the Manifests by title if filter is passes
    - Pickle results
    - Return a List of Manifest URLs
    """
    manifests = getIdentifier(url, session, logger)
    df = pd.DataFrame.from_records(manifests)
    df.to_pickle(Path(Path(__file__).parent.parent, "cache", "allmanifests.pkl"))

    if filter is not None:
        logger.info(f"Filter Manifests by {filter}")
        manifest_urls = df.query(f'name.str.contains("{filter}")', engine="python")[
            "url"
        ].to_list()
    else:
        manifest_urls = df["url"].to_list()

    with open(
        Path(Path(__file__).parent.parent, "cache", "manifest_urls.pkl"), "wb"
    ) as f:
        pickle.dump(manifest_urls, f)
    return manifest_urls
