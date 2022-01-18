import sys
from urllib import request
sys.path.append('/home/cloud/python-saxon')
import saxonc
import requests
import re

def transformHOCR(urls, folder):
    with saxonc.PySaxonProcessor(license=False) as proc:
        xdmAtomicval = proc.make_boolean_value(False)
        xsltproc = proc.new_xslt_processor()
        xsltproc.compile_stylesheet(stylesheet_file="/home/cloud/baytsify/ocr_conversion/hOCR2ALTO.xsl")

        for u in urls:
            filename = re.sub(r'https://api.digitale-sammlungen.de/ocr/(.+)/(.+)', r'\1_\2.xml', u)
            r = requests.get(u)
            r.encoding = 'utf-8'
            document = proc.parse_xml(xml_text=r.text)
            xsltproc.set_source(xdm_node=document)
            result = xsltproc.transform_to_string()
            with open(os.path.join(folder, filename), 'w') as f:
                f.write(result)
                print(f"Wrote {filename}")

if __name__ == '__main__':
    urls = ['https://api.digitale-sammlungen.de/ocr/bsb00063967/740', 'https://api.digitale-sammlungen.de/ocr/bsb00063967/739']
    transformHOCR(urls, '/home/cloud/baytsify/OCR')