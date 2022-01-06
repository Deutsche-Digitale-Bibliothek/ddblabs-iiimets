#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_conversion.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:16

from dataclasses import dataclass
from datetime import datetime
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from loguru import logger
# import lxml.etree as etree
from pathlib import Path
import re
import requests
import time
import sys
import pprint

def generateMETS(metadata, logger, cwd):

    def flgrp_default(images):
        n = 0
        x = ''

        for i in images:
            n += 1
            x += f'''<mets:file MIMETYPE="image/jpg" ID="default_{n}">
    <mets:FLocat LOCTYPE="URL" xlink:href="{i}" />
    </mets:file>'''
        return x

    def flgrp_fulltext(ocr):
        n = 0
        x = ''

        for i in ocr:
            n += 1
            x += f'''<mets:file MIMETYPE="text/xml" ID="{"ocr_" + str(n)}" SEQ="{n}">
    <mets:FLocat LOCTYPE="URL" xlink:href="{i}" />
    </mets:file>'''
        return x

    def flgrp_thumbs(images):
        n = 0
        x = ''

        for i in images:
            n += 1
            x += f'''<mets:file MIMETYPE="image/jpg" ID="{"thumb_" +str(n)}" SEQ="{n}">
    <mets:FLocat LOCTYPE="URL" xlink:href="{re.sub(r'full/full', 'full/250,', i)}" />
    </mets:file>'''
        return x

    def structMapPhysical(images):
        n = 0
        x = ''

        for i in images:
            n += 1
            x += f'''<mets:div xmlns:xs="http://www.w3.org/2001/XMLSchema" TYPE="page" ID="phys_{n}" CONTENTIDS="NULL" ORDER="{n}" ORDERLABEL="{n}">
    <mets:fptr FILEID="default_{n}" />
    <mets:fptr FILEID="ocr_{n}" />
    <mets:fptr FILEID="thumb_{n}" />
    </mets:div> '''
        return x

    def structLink(images):
        n = 0
        x = ''

        for i in images:
            n += 1
            x += f'<mets:smLink xlink:from="log" xlink:to="phys_{n}" />\n'
        return x

    '''
    XML Template zum füllen
    '''
    volume=issue_no=ausgabe=IdentifierSource=originInfo=digitizationyear = ''
    url = 'https://digipress.digitale-sammlungen.de'
    logo = 'https://api.digitale-sammlungen.de/iiif/images/bsb_logo.png'
    r = r'(.+)##\s(.+),(.+)'
    if re.match(r, metadata['Titel']):
        ausgabe = re.sub(r, r'\2', metadata['Titel'])
        ausgabe_titel = re.sub(r, r'\1', metadata['Titel']) + ', ' + ausgabe
    else:
        ausgabe = None
        ausgabe_titel = None
    isodate = re.sub(r'(\d{4}-\d{2}-\d{2})(.+)', r'\1', metadata['dateIssued'])
    physlocation = re.sub(r'(.+)\s--\s(.+)', r'\1', metadata['standort'])
    shelfloc = re.sub(r'(.+)\s--\s(.+)', r'\2', metadata['standort'])
    language_dict = {'Deutsch': 'ger', 'Italienisch': 'ita'}
    sprache = language_dict[metadata['sprache']]

    xmltemplate = f'''
        <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:mods="http://www.loc.gov/mods/v3" xmlns:mets="http://www.loc.gov/METS/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>{time.strftime("%Y-%m-%dT%H:%M:%SZ")}</responseDate>
    <request verb="GetRecord" metadataPrefix="mets" identifier="{metadata['id']}">https://unbekannteschnittstelle.de/</request>
    <GetRecord>
    <record>
    <header>
    <identifier>{metadata['id']}</identifier>
    <datestamp>{time.strftime("%Y-%m-%dT%H:%M:%SZ")}</datestamp>
    </header>
    <metadata>
    <mets:mets OBJID="{metadata['id']}" TYPE="newspaper" xmlns:mets="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:mods="http://www.loc.gov/mods/v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-7.xsd http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd">
    <mets:metsHdr LASTMODDATE="{time.strftime("%Y-%m-%dT%H:%M:%SZ")}">
    <mets:agent xmlns:dv="http://dfg-viewer.de/" ROLE="CREATOR" TYPE="ORGANIZATION">
    <mets:name>{metadata['dataprovider']}</mets:name>
    </mets:agent>
    </mets:metsHdr>
    <mets:dmdSec ID="dmd">
    <mets:mdWrap MDTYPE="MODS">
    <mets:xmlData>
    <mods:mods>
    <mods:typeOfResource>text</mods:typeOfResource>
    <mods:part order="{re.sub("-", "", isodate)}">
    <mods:detail type="volume">
    <mods:number>{volume}</mods:number>
    </mods:detail>
    <mods:detail type="issue">
    <mods:number>{issue_no}, {metadata['Zeitraum']}</mods:number>
    <mods:title>{ausgabe_titel}</mods:title>
    </mods:detail>
    </mods:part>
    <mods:originInfo eventType="publication">
    <mods:dateIssued encoding="iso8601">{isodate}</mods:dateIssued>
    <mods:publisher>{metadata['publisher']}</mods:publisher>
    <mods:place>
    <mods:placeTerm>{originInfo}</mods:placeTerm>
    </mods:place>
    </mods:originInfo>
    <mods:originInfo eventType="digitization">
    <mods:dateCaptured encoding="iso8601">{digitizationyear}</mods:dateCaptured>
    <mods:publisher>{metadata['dataprovider']}</mods:publisher>
    </mods:originInfo>
    <mods:language>
    <mods:languageTerm type="code" valueURI="http://id.loc.gov/vocabulary/iso639-2/ger">{sprache}</mods:languageTerm>
    </mods:language>
    <mods:physicalDescription>
    <mods:extent>{len(metadata['images'])} Seiten</mods:extent>
    </mods:physicalDescription>
    <mods:relatedItem type="host">
    <mods:recordInfo>
    <mods:recordIdentifier source="zdb-ppn">{metadata['purl']}</mods:recordIdentifier>
    </mods:recordInfo>
    <mods:identifier type="zdb">{metadata['zdbid_digital']}</mods:identifier>
    <mods:relatedItem type="original">
    <mods:identifier type="zdb">{metadata['zdbid_print']}</mods:identifier>
    </mods:relatedItem>
    <mods:titleInfo>
    <mods:title>{metadata['Titel']}</mods:title>
    </mods:titleInfo>
    </mods:relatedItem>
    <mods:identifier type="purl">{metadata['purl']}</mods:identifier>
    <mods:accessCondition type="use and reproduction" xlink:href="https://creativecommons.org/publicdomain/mark/1.0/">Public Domain Mark 1.0</mods:accessCondition>
    <mods:recordInfo>
    <mods:recordIdentifier source="DE-12">{metadata['id']}</mods:recordIdentifier>
    <mods:recordContentSource valueURI="http://ld.zdb-services.de/resource/organisations/DE-Bo133">DE-Bo133</mods:recordContentSource>
    <mods:recordChangeDate encoding="iso8601">{isodate}</mods:recordChangeDate>
    <mods:recordInfoNote type="license">{metadata['license']}</mods:recordInfoNote>
    </mods:recordInfo>
    <mods:genre valueURI="http://ddb.vocnet.org/hierarchietyp/ht014" displayLabel="document type">issue</mods:genre>
    <mods:typeOfResource valueURI="http://ddb.vocnet.org/medientyp/mt003">text</mods:typeOfResource>
    <mods:location>
    <mods:physicalLocation>{physlocation}</mods:physicalLocation>
    <mods:shelfLocator>{shelfloc}</mods:shelfLocator>
    </mods:location>
    </mods:mods>
    </mets:xmlData>
    </mets:mdWrap>
    </mets:dmdSec>
    <mets:amdSec xmlns="http://www.w3.org/TR/xhtml1/strict" xmlns:dv="http://dfg-viewer.de/" xmlns:xs="http://www.w3.org/2001/XMLSchema" ID="amd">
    <mets:rightsMD ID="rights">
    <mets:mdWrap MIMETYPE="text/xml" MDTYPE="OTHER" OTHERMDTYPE="DVRIGHTS">
    <mets:xmlData>
    <dv:rights>
    <dv:owner>{metadata['dataprovider']}</dv:owner>
    <dv:ownerLogo>{logo}</dv:ownerLogo>
    <dv:ownerSiteURL>{url}</dv:ownerSiteURL>
    <dv:license>{metadata['license']}</dv:license>
    </dv:rights>
    </mets:xmlData>
    </mets:mdWrap>
    </mets:rightsMD>
    <mets:digiprovMD ID="DIGIPROV">
    <mets:mdWrap MIMETYPE="text/xml" MDTYPE="OTHER" OTHERMDTYPE="DVLINKS">
    <mets:xmlData>
    <dv:links>
    <dv:presentation>{metadata['purl']}</dv:presentation>
    </dv:links>
    </mets:xmlData>
    </mets:mdWrap>
    </mets:digiprovMD>
    </mets:amdSec>
    <mets:fileSec xmlns:dv="http://dfg-viewer.de/">
    <mets:fileGrp xmlns="http://www.w3.org/TR/xhtml1/strict" xmlns:xs="http://www.w3.org/2001/XMLSchema" USE="DEFAULT">
                {flgrp_default(metadata['images'])}
            </mets:fileGrp>
    <mets:fileGrp USE="FULLTEXT">
                {flgrp_fulltext(metadata['ocr'])}
            </mets:fileGrp>
    <mets:fileGrp USE="THUMBS">
                {flgrp_thumbs(metadata['images'])}
            </mets:fileGrp>
    </mets:fileSec>
    <mets:structMap xmlns="http://www.w3.org/TR/xhtml1/strict" xmlns:dv="http://dfg-viewer.de/" ID="psmp" TYPE="PHYSICAL">
    <mets:div ID="phys" CONTENTIDS="NULL" TYPE="physSequence">
                {structMapPhysical(metadata['images'])}
            </mets:div>
    </mets:structMap>
    <mets:structMap TYPE="LOGICAL">
    <mets:div TYPE="issue" ID="log" DMDID="dmd" ADMID="amd" ORDER="1" ORDERLABEL="{isodate}" LABEL="{issue_no}, {metadata['Zeitraum']}"></mets:div>
    </mets:structMap>
    <mets:structLink xmlns="http://www.w3.org/TR/xhtml1/strict" xmlns:dv="http://dfg-viewer.de/" xmlns:xs="http://www.w3.org/2001/XMLSchema">
                {structLink(metadata['images'])}
        </mets:structLink>
    </mets:mets>
        </metadata>
    </record>
    </GetRecord>
    </OAI-PMH>
        '''


    '''
    Output auf Validität prüfen und speichern
    '''
    xmltemplate = re.sub('&', '&amp;', xmltemplate)
    folder = Path(cwd, 'METS', time.strftime("%Y-%m-%d"))
    if folder.exists():
        pass
    else:
        folder.mkdir()
    try:
        newtree = etree.fromstring(xmltemplate)
    except etree.XMLSyntaxError as e:
        logger.warning(f"Fehler beim parsen des erstellen XML: {e}")
        return
    else:

        with open(Path(folder, metadata['id'] + ".xml"), "w", encoding="utf8") as f:
            f.write(xmltemplate)
        # log which IDs have already been generated as METS
        with open(Path(cwd, 'ids_of_generated_mets.txt'), 'a', encoding='utf8') as f:
            f.write(metadata['id'] + "\n")
        logger.info(f"{metadata['id']}.xml erfolgreich erstellt")

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


def getNewspaperData(id, session, newspaper):

    def get_data_from_zdbsru(zdbid):
        baseurl = 'http://services.dnb.de/sru/zdb?version=1.1&operation=searchRetrieve&query=zdbid%3D' + \
            zdbid + '&recordSchema=MARC21-xml'
        response = requests.get(baseurl)
        root = etree.XML(response.content)
        namespaces = {"marc": "http://www.loc.gov/MARC21/slim"}
        zdbid_print = root.findall(f".//marc:datafield[@tag='776']/marc:subfield[@code='w']", namespaces)
        if not zdbid_print:
            return ""
        else:
            return re.sub(r'\(.+\)', '', zdbid_print[0].text)

    for np in newspaper:

        if id in np['id']:
            zdbid_print = np['metadata']['zdbid_print']
            sprache = np['metadata']['sprache']
            standort = np['metadata']['standort']
            zdbid_digital = np['metadata']['zdbid_digital']
            publisher = np['metadata']['publisher']

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
        zdbid_print = re.sub('ZDB ','',metadata['Identifikator'])
        zdbid_digital = get_data_from_zdbsru(zdbid_print)
        sprache = metadata['Sprache']
        standort = metadata['Standort']
        try:
            publisher = metadata['Von']
        except:
            publisher = ''
        urn = re.sub(r'<.+?()>(.+)<\/a>', r'\2', metadata['URN'])
        # Newspaper Dict erstellen, damit wir beim nächsten mal nicht wieder alles parsen müssen
        npdict = {}
        npdict['id'] = id
        npdict['metadata'] = {}
        npdict['metadata']['zdbid_print'] = zdbid_print
        npdict['metadata']['sprache'] = sprache
        npdict['metadata']['standort'] = standort
        npdict['metadata']['zdbid_digital'] = zdbid_digital
        npdict['metadata']['publisher'] = publisher
        npdict['metadata']['urn'] = urn

        newspaper.append(npdict)

    return zdbid_print, sprache, standort, publisher, urn, zdbid_digital

def parseMetadata(manifesturl, session, newspaper, issues, alreadygeneratedids, logger, cwd):
    # Daten laden
    jsondata = json.loads(session.get(manifesturl).text)
    jsonmetadata = jsondata['metadata']
    newspaperid = re.sub(r'(https://digitale-sammlungen\.de/details/)(.+)', r'\2', jsondata['seeAlso'][1]['@id'])
    # Dict aufmachen
    metadata = {}
    for e in jsonmetadata:
        if e['label'][0]['@value'] == 'Identifikator des digitalen Objekts':
            metadata['id'] = re.sub(r"(<a href=.+>)(.+)(</a>)", r'\2', e['value'])
            metadata['purl'] = 'https://mdz-nbn-resolving.de/view:' + re.sub(r"(<a href=.+>)(.+)(</a>)", r'\2', e['value'])
        else:
            metadata[e['label'][0]['@value']] = e['value']

    if metadata['id'] in alreadygeneratedids:
        logger.info(f"Skip conversion of previously generated METS file for {metadata['id']}")
        pass
    else:
        # Erweiterte Infos über das Manifest der Zeitung auslesen
        zdbid_print, sprache, standort, publisher, urn, zdbid_digital = getNewspaperData(newspaperid, session, newspaper)
        # Dictionary befüllen
        metadata['zdbid_print'] = zdbid_print
        metadata['sprache'] = sprache
        metadata['standort'] = standort
        metadata['publisher'] = publisher
        metadata['urn'] = urn
        metadata['zdbid_digital'] = zdbid_digital
        # -----------------
        metadata['dateIssued'] = jsondata['navDate']
        metadata['license'] = jsondata['license']
        metadata['dataprovider'] = jsondata['attribution']
        images = []
        ocr = []
        for s in jsondata['sequences']:
            for c in s['canvases']:
                images.append(c['images'][0]['resource']['@id'])
            for s in jsondata['sequences']:
                for c in s['canvases']:
                    ocr.append(c['seeAlso']['@id'])
        metadata['images'] = images
        metadata['ocr'] = ocr
        # fertiges Dict an die Liste der Issues appenden
        generateMETS(metadata, logger, cwd)
        issues.append(metadata)

def convertManifestsToMETS(manifesturls):
    for u in manifesturls:
        parseMetadata(u, http)
# -------------------------------------------------------------------------------------
if __name__ == '__main__':
    newspaper = []
    issues = []

    http = setup_requests()
    logger.add(sys.stderr, format="{time} {level} {message}", filter="my_module", level="INFO")
    alreadygeneratedids = ""
    manifesturls = ['https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb11380429_01239_u001/manifest', 'https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb10485202_00127_u001/manifest', 'https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb10485202_00799_u001/manifest', 'https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb11308295_00001_u001/manifest', 'https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb10530945_00260_u001/manifest']
    for u in manifesturls:
        parseMetadata(u, http, newspaper, issues, alreadygeneratedids, logger, Path.cwd())