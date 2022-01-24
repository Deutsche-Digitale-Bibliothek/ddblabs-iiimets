import sys
from urllib import request
sys.path.append('/home/cloud/python-saxon')
import saxonc
import requests
import re
import os

def transformHOCR(urls, folder, logger, proc, xsltproc):
    print("Transform hOCR")

    # xsltproc.compile_stylesheet(stylesheet_file="xslt/hOCR2ALTO.xsl")
    xsltproc.compile_stylesheet(stylesheet_file="xslt/hocr__alto3.xsl")

    for u in urls:
        filename = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+)/(.+)', r'\1_\2.xml', u)
        try:
            r = requests.get(u)
        except:
            print(f"Konnte {u} nicht abrufen")
            pass
        else:
            r.encoding = 'utf-8'
            try:
                document = proc.parse_xml(xml_text=r.text)
                xsltproc.set_source(xdm_node=document)
                result = xsltproc.transform_to_string()
            except:
                print(f"Konnte {u} nicht transformieren")
                pass
            else:
                with open(os.path.join(folder, filename), 'w') as f:
                    f.write(result)
                    print(f"Wrote {filename}")

if __name__ == '__main__':
    proc = saxonc.PySaxonProcessor(license=False)
    xsltproc = proc.new_xslt_processor()
    urls = ['https://api.digitale-sammlungen.de/ocr/bsb00063967/740', 'https://api.digitale-sammlungen.de/ocr/bsb00063967/739']
    urls2 = ['https://api.digitale-sammlungen.de/ocr/bsb00063967/741', 'https://api.digitale-sammlungen.de/ocr/bsb00063967/742', 'hxtps://api.digitale-sammlungen.de/ocr/bsb00063967/742']
    transformHOCR(urls, '/home/cloud/baytsify/ALTO', None, proc, xsltproc)
    transformHOCR(urls2, '/home/cloud/baytsify/ALTO', None, proc, xsltproc)
    del proc