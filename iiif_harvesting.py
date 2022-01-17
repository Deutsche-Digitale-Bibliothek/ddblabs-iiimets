#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_harvesting.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:27
from datetime import datetime
import pandas as pd
import sys
import pickle
import time


'''
- Get first (https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=initial)
- for m in /manifests: Log @id
- work on /next: https://api.digitale-sammlungen.de/iiif/presentation/v2/collection/top?cursor=AoIIP4AAACtic2IxMTczMzAwMg==
- total number is available
- return list with Manifest URLs
'''

def getIdentifier(url, session):

    # Die Liste nutzen wir nur zum Anzeigen des Fortschritts.
    manifests = []

    def getManifestURLs(response):
        apireturn = response.json()
        if len(apireturn["manifests"]) != 0:
            for i in apireturn["manifests"]:
                d = {}
                d['url'] = i['@id']
                d['name'] = i['label']
                manifests.append(d)
            print(len(manifests))

    response = session.get(url,
                                    verify=False,
                                    timeout=(20, 80))
    print(f'total number of Manifests: {response.json()["total"]}')

    # Jetzt kommt die eigentliche Schleife: Solange wir da nicht per break rausgehen, läuft die.
    # Immer mit neuer URL
    while True:
        # time.sleep(0.5)
        # Verbindungsversuch inkl. Errorhandling
        try:
            print(url)
            response = session.get(url,
                                    verify=False,
                                    timeout=(20, 80))
        except Exception as e:
            print(f'{url} hat nicht geklappt: {e}')
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
                    # if len(manifests) > 300:
                    #     break
                except:
                    # wir sind fertig und gehen per break aus dem while-Loop raus.
                    print(f"Identifier Harvesting beendet. Insgesamt {len(manifests)} IDs bekommen.")
                    break
    return manifests

def getNewspaperManifests(url, session):
    manifests = getIdentifier(url, session)
    df = pd.DataFrame.from_records(manifests)
    df.to_pickle('allmanifests.pkl')
    newspaper_urls = df.query('name.str.contains("##")', engine="python")['url'].to_list()
    with open('newspaper_urls.pkl', 'wb') as f:
        pickle.dump(newspaper_urls, f)
    return newspaper_urls

def generateNewspaperURLfromFile(fpath):
    import random
    newspaper_urls = ['https://api.digitale-sammlungen.de/iiif/presentation/v2/' + line.rstrip('\n') + '/manifest' for line in open(fpath)]
    with open('newspaper_urls.pkl', 'wb') as f:
        pickle.dump(random.sample(newspaper_urls, int(len(newspaper_urls)/1000)), f)

if __name__ == '__main__':
    generateNewspaperURLfromFile('periodikaausgaben_bsb_20211220.txt')
