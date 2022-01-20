#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# iiif_conversion.py
# @Author : Karl Kraegelin (karlkraegelin@outlook.com)
# @Link   :
# @Date   : 17.12.2021, 13:01:16

from datetime import datetime
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from loguru import logger
from pathlib import Path
import re
import requests
import time
import sys
import pickle
from operator import itemgetter

def generateMETS(metadata, logger, cwd, metsfolder, altofolder):

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
        # Filegroup erstellen mit lokalen Links
        for i in ocr:
            # i ist eine URL, bspw. https://api.digitale-sammlungen.de/ocr/bsb00001098/193
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
    # ! TODO issue_no
    volume=issue_no=ausgabe=IdentifierSource=originInfo=digitizationyear = ''
    url = 'https://digipress.digitale-sammlungen.de'
    logo = 'https://api.digitale-sammlungen.de/iiif/images/bsb_logo.png'
    r = r'(.+)##\s(.+),(.+)'
    if re.match(r, metadata['Titel']):
        ausgabe = re.sub(r, r'\2', metadata['Titel'])
        ausgabe_titel = re.sub(r, r'\1', metadata['Titel']) + ', ' + ausgabe
    else:
        ausgabe = None
        ausgabe_titel = metadata['Titel']
    isodate = re.sub(r'(\d{4}-\d{2}-\d{2})(.+)', r'\1', metadata['dateIssued'])
    physlocation = re.sub(r'(.+)\s--\s(.+)', r'\1', metadata['standort'])
    shelfloc = re.sub(r'(.+)\s--\s(.+)', r'\2', metadata['standort'])
    language_dict = {'Abchasisch':'abk',
'Aceh-Sprache':'ace',
'Acholi-Sprache':'ach',
'Adangme-Sprache':'ada',
'Adygisch':'ady',
'Afrihili':'afh',
'Afrikaans':'afr',
'Ägyptisch':'egy',
'Ainu-Sprache':'ain',
'Akan-Sprache':'aka',
'Akkadisch':'akk',
'Albanisch':'sqi',
'Aleutisch':'ale',
'Algonkin-Sprachen (Andere)':'alg',
'Alt-Newari':'nwc',
'Altaisch':'alt',
'Altaische Sprachen (Andere)':'tut',
'Altäthiopisch':'gez',
'Altenglisch':'ang',
'Altfranzösisch':'fro',
'Altgriechisch':'grc',
'Althochdeutsch':'goh',
'Altirisch':'sga',
'Altnorwegisch':'non',
'Altokzitanisch':'pro',
'Altpersisch':'peo',
'Amharisch':'amh',
'Anga-Sprache':'anp',
'Apachen-Sprachen':'apa',
'Arabisch':'ara',
'Aragonesisch':'arg',
'Aramäisch':'arc',
'Arapaho-Sprache':'arp',
'Arauka-Sprachen':'arn',
'Arawak-Sprachen':'arw',
'Armenisch':'hye',
'Aromunisch':'rup',
'Aserbeidschanisch':'aze',
'Assamesisch':'asm',
'Asturisch':'ast',
'Athapaskische Sprachen (Andere)':'ath',
'Australische Sprachen':'aus',
'Austronesische Sprachen (Andere)':'map',
'Avestisch':'ave',
'Awadhi':'awa',
'Awarisch':'ava',
'Aymará-Sprache':'aym',
'Bahasa Indonesia':'ind',
'Balinesisch':'ban',
'Baltische Sprachen (Andere)':'bat',
'Bambara-Sprache':'bam',
'Bamileke-Sprachen':'bai',
'Banda-Sprachen (Ubangi-Sprachen)':'bad',
'Bantusprachen (Andere)':'bnt',
'Basaa-Sprache':'bas',
'Baschkirisch':'bak',
'Baskisch':'eus',
'Batak-Sprache':'btk',
'Beach-la-mar':'bis',
'Bedauye':'bej',
'Belutschisch':'bal',
'Bemba-Sprache':'bem',
'Bengali':'ben',
'Berbersprachen (Andere)':'ber',
'Bhojpuri':'bho',
'Bihari (Andere)':'bih',
'Bikol-Sprache':'bik',
'Bilin-Sprache':'byn',
'Birmanisch':'mya',
'Blackfoot-Sprache':'bla',
'Bliss-Symbol':'zbl',
'Bokmål':'nob',
'Bosnisch':'bos',
'Braj-Bhakha':'bra',
'Bretonisch':'bre',
'Bugi-Sprache':'bug',
'Bulgarisch':'bul',
'Burjatisch':'bua',
'Caddo-Sprachen':'cad',
'Cebuano':'ceb',
'Cham-Sprachen':'cmc',
'Chamorro-Sprache':'cha',
'Cherokee-Sprache':'chr',
'Cheyenne-Sprache':'chy',
'Chibcha-Sprachen':'chb',
'Chinesisch':'zho',
'Chinook-Jargon':'chn',
'Chipewyan-Sprache':'chp',
'Choctaw-Sprache':'cho',
'Cree-Sprache':'cre',
'Dajakisch':'day',
'Dakota-Sprache':'dak',
'Danakil-Sprache':'aar',
'Dänisch':'dan',
'Darginisch':'dar',
'Delaware-Sprache':'del',
'Deutsch':'deu',
'Dinka-Sprache':'din',
'Dogri':'doi',
'Dogrib-Sprache':'dgr',
'Drawidische Sprachen (Andere)':'dra',
'Duala-Sprachen':'dua',
'Dyula-Sprache':'dyu',
'Dzongkha':'dzo',
'Edo-Sprache':'bin',
'Efik':'efi',
'Einzelne andere Sprachen':'mis',
'Ekajuk':'eka',
'Elamisch':'elx',
'Elliceanisch':'tvl',
'Englisch':'eng',
'Erza-Mordwinisch':'myv',
'Esperanto':'epo',
'Estnisch':'est',
'Ewe-Sprache':'ewe',
'Ewondo':'ewo',
'Fante-Sprache':'fat',
'Färöisch':'fao',
'Fidschi-Sprache':'fij',
'Finnisch':'fin',
'Finnougrische Sprachen (Andere)':'fiu',
'Fon-Sprache':'fon',
'Französisch':'fra',
'Friesisch':'fry',
'Friulisch':'fur',
'Ful':'ful',
'Ga-Sprache':'gaa',
'Galicisch':'glg',
'Gälisch-Schottisch':'gla',
'Galla-Sprache':'orm',
'Ganda-Sprache':'lug',
'Gayo-Sprache':'gay',
'Gbaya-Sprache':'gba',
'Georgisch':'kat',
'Germanische Sprachen (Andere)':'gem',
'Gilbertesisch':'gil',
'Gondi-Sprache':'gon',
'Gorontalesisch':'gor',
'Gotisch':'got',
'Grebo-Sprache':'grb',
'Griechisch':'grc',
'Grönländisch':'kal',
'Guaraní-Sprache':'grn',
'Gujarati-Sprache':'guj',
'Haida-Sprache':'hai',
'Haïtien (Haiti-Kreolisch)':'hat',
'Hamitosemitische Sprachen (Andere)':'afa',
'Haussa-Sprache':'hau',
'Hawaiisch':'haw',
'Hebräisch':'heb',
'Herero-Sprache':'her',
'Hethitisch':'hit',
'Hiligaynon-Sprache':'hil',
'Himachali':'him',
'Hindi':'hin',
'Hiri-Motu':'hmo',
'Hupa-Sprache':'hup',
'Iban-Sprache':'iba',
'Ibo-Sprache':'ibo',
'Ido':'ido',
'Ijo-Sprache':'ijo',
'Ilokano-Sprache':'ilo',
'Inarisaamisch':'smn',
'Indoarische Sprachen (Andere)':'inc',
'Indogermanische Sprachen (Andere)':'ine',
'Inguschisch':'inh',
'Interlingua':'ina',
'Interlingue':'ile',
'Inuktitut':'iku',
'Inupik':'ipk',
'Iranische Sprachen (Andere)':'ira',
'Irisch':'gle',
'Irokesische Sprachen':'iro',
'Isländisch':'isl',
'Italienisch':'ita',
'Jakutisch':'sah',
'Japanisch':'jpn',
'Javanisch':'jav',
'Jiddisch':'yid',
'Judenspanisch':'lad',
'Jüdisch-Arabisch':'jrb',
'Jüdisch-Persisch':'jpr',
'Kabardinisch':'kbd',
'Kabylisch':'kab',
'Kachin-Sprache':'kac',
'Kalmückisch':'xal',
'Kamba-Sprache':'kam',
'Kambodschanisch':'khm',
'Kannada':'kan',
'Kanuri-Sprache':'kau',
'Karakalpakisch':'kaa',
'Karatschaiisch-Balkarisch':'krc',
'Karelisch':'krl',
'Karenisch':'kar',
'Karibische Sprachen':'car',
'Kasachisch':'kaz',
'Kaschmiri':'kas',
'Kaschubisch':'csb',
'Katalanisch':'cat',
'Kaukasische Sprachen (Andere)':'cau',
'Kawi':'kaw',
'Kein linguistischer Inhalt':'zxx',
'Keltische Sprachen (Andere)':'cel',
'Khasi-Sprache':'kha',
'Khoisan-Sprachen (Andere)':'khi',
'Khotta':'mag',
'Kikuyu-Sprache':'kik',
'Kimbundu-Sprache':'kmb',
'Kirchenslawisch':'chu',
'Kirgisisch':'kir',
'Klingonisch':'tlh',
'Komi-Sprache':'kom',
'Kongo-Sprache':'kon',
'Konkani':'kok',
'Koptisch':'cop',
'Koreanisch':'kor',
'Kornisch':'cor',
'Korsisch':'cos',
'Kosraeanisch':'kos',
'Kpelle-Sprache':'kpe',
'Kreolisch-Englisch (Andere)':'cpe',
'Kreolisch-Französisch (Andere)':'cpf',
'Kreolisch-Portugiesisch (Andere)':'cpp',
'Kreolische Sprachen; Pidginsprachen (Andere)':'crp',
'Krimtatarisch':'crh',
'Kroatisch':'hrv',
'Kru-Sprachen (Andere)':'kro',
'Kumükisch':'kum',
'Kunstsprachen (Andere)':'art',
'Kurdisch':'kur',
'Kuschitische Sprachen (Andere)':'cus',
'Kutchin-Sprache':'gwi',
'Kutenai-Sprache':'kut',
'Kwanyama-Sprache':'kua',
'Kymrisch':'cym',
'Lahnda':'lah',
'Lalo-Sprache':'iii',
'Lamba-Sprache (Bantusprache)':'lam',
'Laotisch':'lao',
'Latein':'lat',
'Lesgisch':'lez',
'Lettisch':'lav',
'Limburgisch':'lim',
'Lingala':'lin',
'Litauisch':'lit',
'Lojban':'jbo',
'Luba-Katanga-Sprache':'lub',
'Luiseño-Sprache':'lui',
'Lulesaamisch':'smj',
'Lulua-Sprache':'lua',
'Lunda-Sprache':'lun',
'Luo-Sprache':'luo',
'Lushai-Sprache':'lus',
'Luxemburgisch':'ltz',
'Maduresisch':'mad',
'Maithili':'mai',
'Makassarisch':'mak',
'Makedonisch':'mkd',
'Malagassi-Sprache':'mlg',
'Malaiisch':'msa',
'Malayalam':'mal',
'Maledivisch':'div',
'Malinke-Sprache':'man',
'Maltesisch':'mlt',
'Mandaresisch':'mdr',
'Mandschurisch':'mnc',
'Manobo-Sprachen':'mno',
'Manx':'glv',
'Maori-Sprache':'mri',
'Marathi':'mar',
'Marschallesisch':'mah',
'Marwari':'mwr',
'Massai-Sprache':'mas',
'Maya-Sprachen':'myn',
'Mbundu-Sprache':'umb',
'Mehrere Sprachen':'mul',
'Meithei-Sprache':'mni',
'Mende-Sprache':'men',
'Miao-Sprachen':'hmn',
'Micmac-Sprache':'mic',
'Minangkabau-Sprache':'min',
'Mirandesisch':'mwl',
'Mittelenglisch':'enm',
'Mittelfranzösisch':'frm',
'Mittelhochdeutsch':'gmh',
'Mittelirisch':'mga',
'Mittelniederländisch':'dum',
'Mittelpersisch':'pal',
'Mohawk-Sprache':'moh',
'Mokscha-Sprache':'mdf',
'Mon-Khmer-Sprachen (Andere)':'mkh',
'Mongo-Sprache':'lol',
'Mongolisch':'mon',
'Mossi-Sprache':'mos',
'Mundasprachen (Andere)':'mun',
'Muskogisch':'mus',
'Nahuatl':'nah',
'Nauruanisch':'nau',
'Navajo-Sprache':'nav',
'Ndebele-Sprache (Simbabwe)':'nde',
'Ndebele-Sprache (Transvaal)':'nbl',
'Ndonga':'ndo',
'Neapel / Mundart':'nap',
'Nepali':'nep',
'Neugriechisch':'ell',
'Neumelanesisch':'tpi',
'Neuostaramäisch':'syr',
'Newari':'new',
'Nias-Sprache':'nia',
'Nicht zu entscheiden':'und',
'Niederdeutsch':'nds',
'Niederländisch':'nld',
'Niedersorbisch':'dsb',
'Nigerkordofanische Sprachen (Andere)':'nic',
'Nilosaharanische Sprachen (Andere)':'ssa',
'Niue-Sprache':'niu',
'Nkole-Sprache':'nyn',
'Nogaisch':'nog',
'Nordfriesisch':'frr',
'Nordsaamisch':'sme',
'Norwegisch':'nor',
'Nubische Sprachen':'nub',
'Nyamwezi-Sprache':'nym',
'Nyanja-Sprache':'nya',
'Nynorsk':'nno',
'Nyoro-Sprache':'nyo',
'Nzima-Sprache':'nzi',
'Obersorbisch':'hsb',
'Ojibwa-Sprache':'oji',
'Okzitanisch':'oci',
'Oraon-Sprache':'kru',
'Oriya-Sprache':'ori',
'Osage-Sprache':'osa',
'Osmanisch':'ota',
'Ossetisch':'oss',
'Osterinsel-Sprache':'rap',
'Ostfriesisch':'frs',
'Otomangue-Sprachen':'oto',
'Palau-Sprache':'pau',
'Pali':'pli',
'Pampanggan-Sprache':'pam',
'Pandschabi-Sprache':'pan',
'Pangasinan-Sprache':'pag',
'Pangwe-Sprache':'fan',
'Papiamento':'pap',
'Papuasprachen (Andere)':'paa',
'Paschtu':'pus',
'Pedi-Sprache':'nso',
'Persisch':'fas',
'Philippinisch-Austronesisch (Andere)':'phi',
'Phönikisch':'phn',
'Pilipino':'fil',
'Polnisch':'pol',
'Ponapeanisch':'pon',
'Portugiesisch':'por',
'Prakrit':'pra',
'Quechua-Sprache':'que',
'Rajasthani':'raj',
'Rarotonganisch':'rar',
'Rätoromanisch':'roh',
'Romani (Sprache)':'rom',
'Romanische Sprachen (Andere)':'roa',
'Rotse-Sprache':'loz',
'Rumänisch':'ron',
'Rundi-Sprache':'run',
'Russisch':'rus',
'Rwanda-Sprache':'kin',
'Saamisch':'smi',
'Sakisch':'kho',
'Salish-Sprache':'sal',
'Samaritanisch':'sam',
'Samoanisch':'smo',
'Sandawe-Sprache':'sad',
'Sango-Sprache':'sag',
'Sanskrit':'san',
'Santali':'sat',
'Sardisch':'srd',
'Sasak':'sas',
'Schan-Sprache':'shn',
'Schona-Sprache':'sna',
'Schottisch':'sco',
'Schwedisch':'swe',
'Schweizerdeutsch':'gsw',
'Selkupisch':'sel',
'Semitische Sprachen (Andere)':'sem',
'Serbisch':'srp',
'Serer-Sprache':'srr',
'Sidamo-Sprache':'sid',
'Sindhi-Sprache':'snd',
'Singhalesisch':'sin',
'Sinotibetische Sprachen (Andere)':'sit',
'Sioux-Sprachen (Andere)':'sio',
'Sizilianisch':'scn',
'Skoltsaamisch':'sms',
'Slave-Sprache':'den',
'Slawische Sprachen (Andere)':'sla',
'Slowakisch':'slk',
'Slowenisch':'slv',
'Sogdisch':'sog',
'Somali':'som',
'Songhai-Sprache':'son',
'Soninke-Sprache':'snk',
'Sorbisch (Andere)':'wen',
'Spanisch':'spa',
'Sranantongo':'srn',
'Standard-marokkanischen Tamazight':'zgh',
'Süd-Sotho-Sprache':'sot',
'Südsaamisch':'sma',
'Sukuma-Sprache':'suk',
'Sumerisch':'sux',
'Sundanesisch':'sun',
'Susu':'sus',
'Swahili':'swa',
'Swasi-Sprache':'ssw',
'Syrisch':'syc',
'Tadschikisch':'tgk',
'Tagalog':'tgl',
'Tahitisch':'tah',
'Tama':'tmh',
'Tamil':'tam',
'Tatarisch':'tat',
'Telugu-Sprache':'tel',
'Temne-Sprache':'tem',
'Tereno-Sprache':'ter',
'Tetum-Sprache':'tet',
'Thailändisch':'tha',
'Thaisprachen (Andere)':'tai',
'Tibetisch':'bod',
'Tigre-Sprache':'tig',
'Tigrinja-Sprache':'tir',
'Tiv-Sprache':'tiv',
'Tlingit-Sprache':'tli',
'Tokelauanisch':'tkl',
'Tongaisch':'ton',
'Trukesisch':'chk',
'Tschagataisch':'chg',
'Tschechisch':'ces',
'Tscheremissisch':'chm',
'Tschetschenisch':'che',
'Tschuwaschisch':'chv',
'Tsimshian-Sprache':'tsi',
'Tsonga-Sprache':'tso',
'Tswana-Sprache':'tsn',
'Tumbuka-Sprache':'tum',
'Tupi-Sprache':'tup',
'Türkisch':'tur',
'Turkmenisch':'tuk',
'Tuwinisch':'tyv',
'Twi-Sprache':'twi',
'Udmurtisch':'udm',
'Ugaritisch':'uga',
'Uigurisch':'uig',
'Ukrainisch':'ukr',
'Ungarisch':'hun',
'Urdu':'urd',
'Usbekisch':'uzb',
'Vai-Sprache':'vai',
'Venda-Sprache':'ven',
'Vietnamesisch':'vie',
'Volapük':'vol',
'Wakash-Sprachen':'wak',
'Walamo-Sprache':'wal',
'Wallonisch':'wln',
'Waray':'war',
'Washo-Sprache':'was',
'Weißrussisch':'bel',
'Wolof-Sprache':'wol',
'Wotisch':'vot',
'Xhosa-Sprache':'xho',
'Yao-Sprache (Bantusprache)':'yao',
'Yapesisch':'yap',
'Yoruba-Sprache':'yor',
'Ypik-Sprachen':'ypk',
'Zande-Sprachen':'znd',
'Zapotekisch':'zap',
'Zazaki':'zza',
'Zeichensprachen':'sgn',
'Zenaga':'zen',
'Zhuang':'zha',
'Zulu-Sprache':'zul',
'Zuñi-Sprache':'zun'
}
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
    <mods:title>{metadata['newspapertitel']}</mods:title>
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

    try:
        newtree = etree.fromstring(xmltemplate)
    except etree.XMLSyntaxError as e:
        logger.warning(f"Fehler beim parsen des erstellen XML: {e}")
        return
    else:

        with open(Path(metsfolder, metadata['id'] + ".xml"), "w", encoding="utf8") as f:
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
    '''
    bekommt die id des newspapers und die Liste mit den zuvor ggf. schon gesammelten Infos zu den Zeitungen
    '''
    def get_data_from_zdbsru(zdbid):
        baseurl = 'http://services.dnb.de/sru/zdb?version=1.1&operation=searchRetrieve&query=zdbid%3D' + \
            zdbid + '&recordSchema=MARC21-xml'
        # print(zdbid)
        response = requests.get(baseurl)
        root = etree.XML(response.content)
        namespaces = {"marc": "http://www.loc.gov/MARC21/slim"}
        zdbid_digital = root.findall(f".//marc:datafield[@tag='776']/marc:subfield[@code='w']", namespaces)
        newspapertitel = root.findall(f".//marc:datafield[@tag='245']/marc:subfield[@code='a']", namespaces)

        if not zdbid_digital:
            zdbid_digital = ""
        else:
            zdbid_digital = re.sub(r'\(.+\)', '', zdbid_digital[0].text)
        if not newspapertitel:
            newspapertitel = ""
        else:
            newspapertitel = newspapertitel[0].text
        # ! TODO newspaper Titelk noch die nicht sortierzeichen entfernen
        return zdbid_digital, newspapertitel

    def gatherinfos():
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
        zdbid_print = re.sub('ZDB ','', metadata['Identifikator'])
        zdbid_digital, newspapertitel = get_data_from_zdbsru(zdbid_print)
        try:
            sprache = metadata['Sprache']
        except:
            sprache = 'Nicht zu entscheiden'
        standort = metadata['Standort']
        metadata['Newspapertitle'] = newspapertitel
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
        npdict['metadata']['newspapertitel'] = newspapertitel
        newspaper.append(npdict)

        return zdbid_print, sprache, standort, publisher, urn, zdbid_digital, newspapertitel

    if len(newspaper) == 0:
        # das passiert nur bei der allerersten Issue
        # print("First Issue!")
        zdbid_print, sprache, standort, publisher, urn, zdbid_digital, newspapertitel = gatherinfos()
    else:
        # schauen, ob wir zu der Zeitung die Metadaten schon abgerufen haben
        # newspaper ist eine Liste mit Dicts.
        try:
            pos = list(map(itemgetter('id'), newspaper)).index(id)
        except:
            # print("Rufe die Infos ab über das Manifest der Zeitung und die ZDB")
            zdbid_print, sprache, standort, publisher, urn, zdbid_digital, newspapertitel = gatherinfos()
        else:
            # print(f"Cache für {id}")
            zdbid_digital = newspaper[pos]['metadata']['zdbid_digital']
            sprache = newspaper[pos]['metadata']['sprache']
            standort = newspaper[pos]['metadata']['standort']
            zdbid_print = newspaper[pos]['metadata']['zdbid_print']
            publisher = newspaper[pos]['metadata']['publisher']
            newspapertitel = newspaper[pos]['metadata']['newspapertitel']
            urn = newspaper[pos]['metadata']['urn']

    return zdbid_print, sprache, standort, publisher, urn, zdbid_digital, newspapertitel, newspaper

def parseMetadata(manifesturl, session, newspaper, issues, alreadygeneratedids, logger, cwd, metsfolder, altofolder):
    # Daten laden
    # logger.debug(manifesturl)
    try:
        jsondata = json.loads(session.get(manifesturl).text)
    except:
        logger.error(f'Failed to JSON Decode {manifesturl}')
        return
    jsonmetadata = jsondata['metadata']
    #  get MDZ Newspaper ID
    if isinstance(jsondata['seeAlso'], list):
        for i in jsondata['seeAlso']:
            if i['@id'].startswith('https://digitale-sammlungen.de'):
                newspaperid = re.sub(r'(https://digitale-sammlungen\.de/details/)(.+)', r'\2', i['@id'])
    else:
        try:
            jsondata['seeAlso']['@id']
        except KeyError:
            logger.error('Problem beim parsen der Newspaper ID')
        else:
            if jsondata['seeAlso']['@id'].startswith('https://digitale-sammlungen.de'):
                newspaperid = re.sub(r'(https://digitale-sammlungen\.de/details/)(.+)', r'\2', jsondata['seeAlso']['@id'])
            else:
                logger.error('Problem beim parsen der Newspaper ID')
                return
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
        if not re.search(r'\s##\s', metadata['Titel']):
            logger.warning(f'{manifesturl} wahrscheinlich keine Zeitung!')
            return
        # Erweiterte Infos über das Manifest der Zeitung auslesen
        zdbid_print, sprache, standort, publisher, urn, zdbid_digital, newspapertitel, newspaper = getNewspaperData(newspaperid, session, newspaper)
        # Dictionary befüllen
        metadata['zdbid_print'] = zdbid_print
        metadata['sprache'] = sprache
        metadata['standort'] = standort
        metadata['publisher'] = publisher
        metadata['urn'] = urn
        metadata['newspapertitel'] = newspapertitel
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
                    try:
                        c['seeAlso']
                    except KeyError:
                        logger.warning(f'Kein OCR bei {manifesturl}')
                    else:
                        ocr.append(c['seeAlso']['@id'])
        metadata['images'] = images
        metadata['ocr'] = ocr
        # fertiges Dict an die Liste der Issues appenden
        generateMETS(metadata, logger, cwd, metsfolder, altofolder)
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
    manifesturls = ['https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb11327001_00003_u001/manifest', 'https://api.digitale-sammlungen.de/iiif/presentation/v2/bsb11327001_00037_u001/manifest']
    for u in manifesturls:
        parseMetadata(u, http, newspaper, issues, alreadygeneratedids, logger, Path.cwd(), '/Users/karl/Coding/baytsify/METS', '/Users/karl/Coding/baytsify/ALTO')
    with open('newspaperdata.pkl', 'wb') as f:
            pickle.dump(newspaper, f)