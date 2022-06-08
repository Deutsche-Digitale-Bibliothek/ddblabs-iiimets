class METS:

    def __init__(self, itemid):
      self.itemid = itemid
      self.time = time

    def returnXML(self):
        return f'''
        <mets:mets OBJID="{itemid}" TYPE="newspaper" xmlns:mets="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:mods="http://www.loc.gov/mods/v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-7.xsd http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd">
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
        <mods:recordIdentifier source="DE-12">{itemid.split('_')[0]}</mods:recordIdentifier>
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
        <mods:recordIdentifier source="DE-12">{itemid}</mods:recordIdentifier>
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
            '''
