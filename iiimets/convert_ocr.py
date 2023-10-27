import os
import re
import subprocess

import requests
from pkg_resources import resource_filename

HOCR2ALTO = resource_filename(__name__, "res/xslt/hOCR2ALTO.xsl")


def run_xsl_on_folder(hocrfolder, altofolder, cwd, saxonpath, logger):
    subprocessargs = (
        f"{saxonpath} -s:{hocrfolder} -o:{altofolder} -xsl:{HOCR2ALTO}".split(" ")
    )
    logger.info("Starte Saxon XSLT Processing")
    try:
        transformationoutput = subprocess.check_output(
            subprocessargs, stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        transformationoutput = str(e.output, "utf-8")
        if len(re.findall(r"\d+\stransformations failed", transformationoutput)) == 1:
            logger.warning(
                re.findall(r"\d+\stransformations failed", transformationoutput)[0]
            )


def transformHOCR(urls, folder, logger):
    logger.info("Loading Saxon")
    try:
        import saxonc
    except ImportError as err:
        logger.error(err)
        return
    with saxonc.PySaxonProcessor(license=False) as proc:
        xsltproc = proc.new_xslt_processor()
        xsltproc.compile_stylesheet(stylesheet_file=HOCR2ALTO)

        for u in urls:
            logger.debug("Transform hOCR for %s", u)
            filename = re.sub(
                r"https://api.digitale-sammlungen.de/ocr/(.+)/(.+)", r"\1_\2.xml", u
            )
            try:
                r = requests.get(u)
            except:
                logger.error(f"Konnte {u} nicht abrufen")
                pass
            else:
                r.encoding = "utf-8"
                try:
                    document = proc.parse_xml(xml_text=r.text)
                    xsltproc.set_source(xdm_node=document)
                    result = xsltproc.transform_to_string()
                except:
                    logger.error(f"Konnte {u} nicht transformieren")
                    pass
                else:
                    with open(os.path.join(folder, filename), "w") as f:
                        f.write(result)
                        logger.debug(f"Wrote {filename}")


if __name__ == "__main__":
    urls = [
        "https://api.digitale-sammlungen.de/ocr/bsb00063967/740",
        "https://api.digitale-sammlungen.de/ocr/bsb00063967/739",
    ]
    transformHOCR(urls, "OCR")
