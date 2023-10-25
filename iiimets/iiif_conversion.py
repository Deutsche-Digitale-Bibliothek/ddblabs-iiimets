#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_conversion.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:16

import json
import re
import time
from operator import itemgetter
from pathlib import Path

import requests
from lxml import etree
from pkg_resources import resource_filename
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .templates import METS

LANGUAGES_JSON = resource_filename(__name__, "res/languages.json")


def generateMETS(metadata, logger, metsfolder):
    """
    Input: Dictionary mit den Metadaten
    Output: METS XML
    """

    def flgrp_default(images):
        n = 0
        x = ""

        for i in images:
            n += 1
            x += f"""<mets:file MIMETYPE="image/jpg" ID="default_{n}">
    <mets:FLocat LOCTYPE="URL" xlink:href="{i}" />
    </mets:file>"""
        return x

    def flgrp_fulltext(ocr):
        n = 0
        x = ""
        # Filegroup erstellen mit lokalen Links
        for i in ocr:
            # i ist eine URL, bspw. https://api.digitale-sammlungen.de/ocr/bsb00001098/193
            n += 1
            x += f"""<mets:file MIMETYPE="text/xml" ID="{"ocr_" + str(n)}" SEQ="{n}">
    <mets:FLocat LOCTYPE="URL" xlink:href="{i}" />
    </mets:file>"""
        return x

    def flgrp_thumbs(images):
        n = 0
        x = ""

        for i in images:
            n += 1
            x += f"""<mets:file MIMETYPE="image/jpg" ID="{"thumb_" +str(n)}" SEQ="{n}">
    <mets:FLocat LOCTYPE="URL" xlink:href="{re.sub(r'full/full', 'full/250,', i)}" />
    </mets:file>"""
        return x

    def structMapPhysical(images):
        n = 0
        x = ""

        for i in images:
            n += 1
            x += f"""<mets:div xmlns:xs="http://www.w3.org/2001/XMLSchema" TYPE="page" ID="phys_{n}" CONTENTIDS="NULL" ORDER="{n}" ORDERLABEL="{n}">
    <mets:fptr FILEID="default_{n}" />
    <mets:fptr FILEID="ocr_{n}" />
    <mets:fptr FILEID="thumb_{n}" />
    </mets:div> """
        return x

    def structLink(images):
        n = 0
        x = ""

        for i in images:
            n += 1
            x += f'<mets:smLink xlink:from="log" xlink:to="phys_{n}" />\n'
        return x

    print("---------------------------")
    print(metadata)
    # FIXME metadata["Zeitraum"] fehlt
    metadata["Zeitraum"] = "1900-2000"
    # ! TODO issue_no
    volume = issue_no = originInfo = digitizationyear = ""
    url = "https://digipress.digitale-sammlungen.de"
    logo = "https://api.digitale-sammlungen.de/iiif/images/bsb_logo.png"
    r = r"(.+)##\s(.+),(.+)"
    if re.match(r, metadata["newspapertitel"]):
        ausgabe = re.sub(r, r"\2", metadata["newspapertitel"])
        ausgabe_titel = re.sub(r, r"\1", metadata["newspapertitel"]) + ", " + ausgabe
    else:
        ausgabe = None
        ausgabe_titel = metadata["newspapertitel"]
    isodate = re.sub(r"(\d{4}-\d{2}-\d{2})(.+)", r"\1", metadata["dateIssued"])
    physlocation = re.sub(r"(.+)\s--\s(.+)", r"\1", metadata["standort"])
    shelfloc = re.sub(r"(.+)\s--\s(.+)", r"\2", metadata["standort"])
    with open(LANGUAGES_JSON) as langjson:
        language_dict = json.load(langjson)
    sprache = language_dict[metadata["sprache"]]
    dvlicense = metadata["license"].replace(
        "http://creativecommons.org/licenses/by-nc-sa/4.0/deed.de",
        "https://creativecommons.org/licenses/by-nc-sa/4.0/",
    )
    # xmltemplate = templates.METS()
    xmltemplate = f"""
        <mets:mets OBJID="{metadata['id']}" TYPE="newspaper" xmlns:mets="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:mods="http://www.loc.gov/mods/v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-7.xsd http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd">
        <mets:metsHdr LASTMODDATE="{time.strftime("%Y-%m-%dT%H:%M:%SZ")}">
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
        <mods:number>{issue_no + ', ' if issue_no else ''}{metadata['Zeitraum']}</mods:number>
        <mods:title>{ausgabe_titel}</mods:title>
        </mods:detail>
        </mods:part>
        <mods:originInfo eventType="publication">
        <mods:dateIssued encoding="iso8601">{isodate}</mods:dateIssued>
        {'<mods:publisher>' + metadata['publisher'] + '</mods:publisher>' if 'publisher' in metadata and metadata['publisher'] != '' else ''}
        </mods:originInfo>
        <mods:originInfo eventType="digitization">
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
        <mods:recordIdentifier source="DE-12">{metadata['id'].split('_')[0]}</mods:recordIdentifier>
        </mods:recordInfo>
        <mods:identifier type="zdb">{metadata['zdbid_digital']}</mods:identifier>
        <mods:relatedItem type="original">
        <mods:identifier type="zdb">{metadata['zdbid_print']}</mods:identifier>
        </mods:relatedItem>
        <mods:titleInfo>
        <mods:title>{metadata['newspapertitel']}</mods:title>
        </mods:titleInfo>
        </mods:relatedItem>
        <mods:identifier type="purl">{metadata['purl']}</mods:identifier>
        <mods:accessCondition type="use and reproduction" xlink:href="https://creativecommons.org/publicdomain/mark/1.0/">Public Domain Mark 1.0</mods:accessCondition>
        <mods:recordInfo>
        <mods:recordIdentifier source="DE-12">{metadata['id']}</mods:recordIdentifier>
        <mods:recordInfoNote type="license">http://creativecommons.org/publicdomain/zero/1.0</mods:recordInfoNote>
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
        <dv:license>{dvlicense}</dv:license>
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
        <mets:div TYPE="issue" ID="log" DMDID="dmd" ADMID="amd" ORDER="1" ORDERLABEL="{isodate}" LABEL="{issue_no + ', ' if issue_no else ''}{metadata['Zeitraum']}"></mets:div>
        </mets:structMap>
        <mets:structLink xmlns="http://www.w3.org/TR/xhtml1/strict" xmlns:dv="http://dfg-viewer.de/" xmlns:xs="http://www.w3.org/2001/XMLSchema">
                    {structLink(metadata['images'])}
            </mets:structLink>
        </mets:mets>
        """

    """
    Output auf Validität prüfen und speichern
    """
    xmltemplate = re.sub("&", "&amp;", xmltemplate)

    try:
        newtree = etree.fromstring(xmltemplate)
    except etree.XMLSyntaxError as e:
        logger.warning(f"Errors were reported while parsing the generated XML: {e}")
        return
    else:
        metsfile = Path(metsfolder, metadata["id"] + ".xml")
        with open(metsfile, "w") as f:
            indentedxml = etree.tostring(
                newtree, method="xml", encoding="unicode", pretty_print=True
            )
            f.write(indentedxml)
            logger.info(f"{metadata['id']}.xml created successfully.")
        # log which IDs have already been generated as METS
        with open(
            Path(Path(__file__).parent.parent, "ids_of_generated_mets.txt"),
            "a",
            encoding="utf8",
        ) as f:
            f.write(metadata["id"] + "\n")


def setup_requests() -> requests.Session:
    """Sets up a requests session to automatically retry on errors

    cf. <https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/>

    Returns
    -------
    http : requests.Session
        A fully configured requests Session object
    """
    http = requests.Session()
    assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
    http.hooks["response"] = [assert_status_hook]
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


def getNewspaperData(id, session, newspaper, zdb_id_print):
    """
    bekommt die id des newspapers und die Liste mit den zuvor ggf. schon gesammelten Infos zu den Zeitungen
    Ruft Informationen über die SRU Schnittstelle der ZDB ab
    """

    def get_data_from_zdbsru(zdb_id_print: str):
        """
        returns zdbid_digital, newspapertitel
        """
        baseurl = (
            "http://services.dnb.de/sru/zdb?version=1.1&operation=searchRetrieve&query=zdbid%3D"
            + zdb_id_print
            + "&recordSchema=MARC21-xml"
        )
        response = requests.get(baseurl)
        root = etree.XML(response.content)
        namespaces = {"marc": "http://www.loc.gov/MARC21/slim"}
        zdbid_digital = root.findall(
            ".//marc:datafield[@tag='776']/marc:subfield[@code='w']", namespaces
        )
        newspapertitel = root.findall(
            ".//marc:datafield[@tag='245']/marc:subfield[@code='a']", namespaces
        )

        if not zdbid_digital:
            zdbid_digital = ""
        else:
            zdbid_digital = re.sub(r"\(.+\)", "", zdbid_digital[0].text)
        if not newspapertitel:
            newspapertitel = ""
        else:
            newspapertitel = newspapertitel[0].text
        # ! TODO newspaper Titel noch die Nicht-Sortierzeichen entfernen
        return zdbid_digital, newspapertitel

    def gatherinfos(zdb_id_print):
        """
        Again loads JSON Manifest from URL and extracts Metadata

        Returns a tuple with the following values:
        zdbid_print, sprache, standort, publisher, urn, zdbid_digital, newspapertitel
        """
        metadata = {}
        # TODO this needs to be a parameter
        newspaper_manifest_url = (
            f"https://api.digitale-sammlungen.de/iiif/presentation/v2/{id}/manifest"
        )
        issue_metadata = json.loads(session.get(newspaper_manifest_url).text)
        metadata[id] = id
        for e in issue_metadata["metadata"]:
            if isinstance(e["value"], list):
                metadata[e["label"][0]["@value"]] = e["value"][0]["@value"]
            else:
                metadata[e["label"][0]["@value"]] = e["value"]
        # FIXME metadata["Identifikator"] is not there
        zdbid_digital, newspapertitel = get_data_from_zdbsru(zdb_id_print)
        try:
            sprache = metadata["Sprache"]
        except KeyError:
            sprache = "Nicht zu entscheiden"
        standort = ""
        metadata["Newspapertitle"] = newspapertitel
        try:
            publisher = metadata["Von"]
        except KeyError:
            publisher = ""
        urn = re.sub(r"<.+?()>(.+)<\/a>", r"\2", metadata["URN"])
        # Newspaper Dict erstellen, damit wir beim nächsten mal nicht wieder alles parsen müssen
        npdict = {}
        npdict["id"] = id
        npdict["metadata"] = {}
        npdict["metadata"]["zdbid_print"] = zdb_id_print
        npdict["metadata"]["sprache"] = sprache
        npdict["metadata"]["standort"] = standort
        npdict["metadata"]["zdbid_digital"] = zdbid_digital
        npdict["metadata"]["publisher"] = publisher
        npdict["metadata"]["urn"] = urn
        npdict["metadata"]["newspapertitel"] = newspapertitel
        newspaper.append(npdict)

        return (
            sprache,
            standort,
            publisher,
            urn,
            zdbid_digital,
            newspapertitel,
        )

    if len(newspaper) == 0:
        # das passiert nur bei der allerersten Issue
        (
            sprache,
            standort,
            publisher,
            urn,
            zdbid_digital,
            newspapertitel,
        ) = gatherinfos(zdb_id_print)
    else:
        # schauen, ob wir zu der Zeitung die Metadaten schon abgerufen haben
        # newspaper ist eine Liste mit Dicts.
        try:
            pos = list(map(itemgetter("id"), newspaper)).index(id)
        except:
            (
                sprache,
                standort,
                publisher,
                urn,
                zdbid_digital,
                newspapertitel,
            ) = gatherinfos(zdb_id_print)
        else:
            zdbid_digital = newspaper[pos]["metadata"]["zdbid_digital"]
            sprache = newspaper[pos]["metadata"]["sprache"]
            standort = newspaper[pos]["metadata"]["standort"]
            publisher = newspaper[pos]["metadata"]["publisher"]
            newspapertitel = newspaper[pos]["metadata"]["newspapertitel"]
            urn = newspaper[pos]["metadata"]["urn"]

    return (
        sprache,
        standort,
        publisher,
        urn,
        zdbid_digital,
        newspapertitel,
        newspaper,
    )


def parseMetadata(
    manifesturl: str,
    session: requests.session,
    newspaper,
    issues,
    alreadygeneratedids: list,
    logger,
    cwd: Path,
    metsfolder,
):
    """
    Load IIIF JSON Manifest from URL, extract Metadata and generate METS XML.
    FIXME Pass already downloaded JSON
    """

    try:
        issue_json = json.loads(session.get(manifesturl).text)
    except json.decoder.JSONDecodeError:
        logger.error(f"Failed to JSON Decode {manifesturl}")
        return
    issue_metadata = issue_json["metadata"]
    #  get MDZ Newspaper ID
    if isinstance(issue_json["seeAlso"], list):
        # if there are multiple seeAlso links, we need to find the one that points to the MDZ
        # and extract the ID from there
        for i in issue_json["seeAlso"]:
            if i["@id"].startswith("https://digitale-sammlungen.de"):
                newspaperid = re.sub(
                    r"(https://digitale-sammlungen\.de/details/)(.+)", r"\2", i["@id"]
                )
    else:
        # only one seeAlso link, so we can extract the ID directly
        try:
            issue_json["seeAlso"]["@id"]
        except KeyError:
            logger.error("Problem beim parsen der Newspaper ID")
        else:
            if issue_json["seeAlso"]["@id"].startswith(
                "https://digitale-sammlungen.de"
            ):
                newspaperid = re.sub(
                    r"(https://digitale-sammlungen\.de/details/)(.+)",
                    r"\2",
                    issue_json["seeAlso"]["@id"],
                )
            else:
                # no luck getting the ID, so we need to skip this issue
                logger.error("Problem beim parsen der Newspaper ID")
                return
    # Empty dict for collected metadata
    extracted_metadata = {}
    # ZDB ID bekommen wir über issue_metadata["seeAlso"][0]["@id"]
    zdbid_print = issue_json["seeAlso"][0]["@id"].split("/")[-1]
    for e in issue_metadata:
        # for each subdict in the metadata list check if the english label is "Digital Object Identifier"
        if e["label"][0]["@value"] == "Digital Object Identifier":
            # if it is, extract the ID from there
            extracted_metadata["id"] = re.sub(
                r"(<a href=.+>)(.+)(</a>)", r"\2", e["value"]
            )
            extracted_metadata["purl"] = "https://mdz-nbn-resolving.de/view:" + re.sub(
                r"(<a href=.+>)(.+)(</a>)", r"\2", e["value"]
            )
    # Now check if we have the ID
    try:
        extracted_metadata["id"]

    except KeyError:
        logger.error(f"Keine ID für {manifesturl} gefunden")
        return
    else:
        # check if we have already generated a METS file for this issue
        if extracted_metadata["id"] in alreadygeneratedids:
            logger.info(
                f"Skip conversion of previously generated METS file for {extracted_metadata['id']}"
            )
            pass
        else:
            issue_title = issue_metadata[0]["value"]

            if not re.search(r"\s##\s", issue_title):
                logger.warning(f"{manifesturl} wahrscheinlich keine Zeitung!")
                return
            # Erweiterte Infos über das Manifest der Zeitung auslesen
            (
                sprache,
                standort,
                publisher,
                urn,
                zdbid_digital,
                newspapertitel,
                newspaper,
            ) = getNewspaperData(newspaperid, session, newspaper, zdbid_print)
            # Dictionary befüllen
            extracted_metadata["zdbid_print"] = zdbid_print
            extracted_metadata["sprache"] = sprache
            extracted_metadata["standort"] = standort
            extracted_metadata["publisher"] = publisher
            extracted_metadata["urn"] = urn
            extracted_metadata["newspapertitel"] = issue_title
            extracted_metadata["zdbid_digital"] = zdbid_digital
            # -----------------
            extracted_metadata["dateIssued"] = issue_json["navDate"]
            extracted_metadata["license"] = issue_json["license"][0]
            extracted_metadata["dataprovider"] = issue_json["attribution"][0]["@value"]
            images = []
            ocr = []
            for s in issue_json["sequences"]:
                for c in s["canvases"]:
                    images.append(c["images"][0]["resource"]["@id"])
                for s in issue_json["sequences"]:
                    for c in s["canvases"]:
                        try:
                            c["seeAlso"]
                        except KeyError:
                            logger.warning(f"Kein OCR bei {manifesturl}")
                        else:
                            ocr.append(c["seeAlso"]["@id"])
            extracted_metadata["images"] = images
            extracted_metadata["ocr"] = ocr
            # fertiges Dict an die Liste der Issues appenden
            generateMETS(extracted_metadata, logger, metsfolder)
            issues.append(extracted_metadata)
