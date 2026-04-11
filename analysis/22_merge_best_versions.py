#!/usr/bin/env python3
"""
Merge the improved chapter 1-2 text from the chapter-polished version
into the later improved review/submission version.
"""

from __future__ import annotations

import copy
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree


NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}

CH12_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_前两章润色版.docx")
BASE_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_送审版.docx")
OUTPUT_DOC = Path("/Users/dxk/Downloads/段晓坤 毕业论文二稿_统一总版.docx")


def paragraph_text(p: etree._Element) -> str:
    return "".join(t.text or "" for t in p.xpath(".//w:t", namespaces=NS)).strip()


def main() -> None:
    if not CH12_DOC.exists():
        raise FileNotFoundError(CH12_DOC)
    if not BASE_DOC.exists():
        raise FileNotFoundError(BASE_DOC)

    with ZipFile(CH12_DOC) as z1, ZipFile(BASE_DOC) as z2:
        src_doc = etree.fromstring(z1.read("word/document.xml"))
        dst_doc = etree.fromstring(z2.read("word/document.xml"))

        src_paras = src_doc.xpath("//w:body/w:p", namespaces=NS)
        dst_paras = dst_doc.xpath("//w:body/w:p", namespaces=NS)

        # Replace first two chapters by paragraph position. Up to and including 2.3.4 body.
        # This range is stable across the current versions.
        start = 56
        end = 112
        for i in range(start, end):
            dst_paras[i].getparent().replace(dst_paras[i], copy.deepcopy(src_paras[i]))

        new_doc_xml = etree.tostring(dst_doc, xml_declaration=True, encoding="UTF-8", standalone="yes")

        with ZipFile(OUTPUT_DOC, "w", compression=ZIP_DEFLATED) as zout:
            for item in z2.infolist():
                if item.filename == "word/document.xml":
                    zout.writestr(item, new_doc_xml)
                else:
                    zout.writestr(item, z2.read(item.filename))

    print(OUTPUT_DOC)


if __name__ == "__main__":
    main()
