#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_conversion.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:16

import json
import yaml
import click
import csv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import lxml.etree as etree
from progress.bar import ChargingBar
import sys
import os
import pprint
import re
from dataclasses import dataclass

def setup_requests() -> requests.Session:
    """Sets up a requests session to automatically retry on errors

    cf. <https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/>

    Returns
    -------
    http : requests.Session
        A fully configured requests Session object
    """
    http = requests.Session()
    assert_status_hook = lambda response, * \
        args, **kwargs: response.raise_for_status()
    http.hooks["response"] = [assert_status_hook]
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http



# -------------------------------------------------------------------------------------

newspaper = []
issues = []

'''
newspaper = [
    {
        'id': 'bsb10485202',
        'metadata' : {
            'zdbid': '1252612-5'
        }
    },
    {
        'id': '...',
        'metadata' : {
            'zdbid': '...'
        }
    },
    ...
]
'''

def getNewspaperData(id, session):
    for np in newspaper:
        if id in np['id']:
            zdbid = np['metadata']['zdbid']
            sprache = np['metadata']['sprache']
    else:
        metadata = {}
        newspaper_manifest_url = f'https://api.digitale-sammlungen.de/iiif/presentation/v2/{id}/manifest'
        jsonmetadata = json.loads(session.get(newspaper_manifest_url).text)
        metadata[id] = id
        for e in jsonmetadata['metadata']:
            # pprint.pprint(e)
            if type(e['value']) == list:
                metadata[e['label'][0]['@value']] = e['value'][0]['@value']
            else:
                metadata[e['label'][0]['@value']] = e['value']
        zdbid = re.sub('ZDB ','',metadata['Identifikator'])
        sprache = metadata['Sprache']
        standort = metadata['Standort']
        # pprint.pprint(metadata)
    return zdbid, sprache, standort


def createMETS(manifesturl, session):
    # Daten laden
    jsondata = json.loads(session.get(manifesturl).text)
    jsonmetadata = jsondata['metadata']
    newspaperid = re.sub(r'(https://digitale-sammlungen\.de/details/)(.+)', r'\2', jsondata['seeAlso'][1]['@id'])
    # Dict aufmachen
    metadata = {}
    # Erweiterte Infos über das Manifest der Zeitung auslesen
    zdbid, sprache, standort = getNewspaperData(newspaperid, session)
    # Dictionary befüllen
    metadata['zdbid'] = zdbid
    metadata['sprache'] = sprache
    metadata['standort'] = standort
    for e in jsonmetadata:
        if e['label'][0]['@value'] == 'Identifikator des digitalen Objekts':
            metadata['id'] = re.sub(r"(<a href=.+>)(.+)(</a>)", r'\2', e['value'])
            metadata['purl'] = 'https://mdz-nbn-resolving.de/view:' + re.sub(r"(<a href=.+>)(.+)(</a>)", r'\2', e['value'])
        else:
            metadata[e['label'][0]['@value']] = e['value']
    metadata['dateIssued'] = jsondata['navDate']
    metadata['license'] = jsondata['license']
    metadata['dataprovider'] = jsondata['attribution']
    images = []
    for s in jsondata['sequences']:
        for c in s['canvases']:
            images.append(c['images'][0]['resource']['@id'])
    metadata['images'] = images
    # fertiges Dict an die Liste der Issues appenden
    issues.append(metadata)

# -------------------------------------------------------------------------------------
if __name__ == '__main__':
    http = setup_requests()
    manifesturls = ['https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb10485202_00127_u001/manifest']
    for u in manifesturls:
        createMETS(u, http)
    pprint.pprint(issues)