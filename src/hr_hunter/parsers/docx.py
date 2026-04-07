from __future__ import annotations

import re
from io import BytesIO
import xml.etree.ElementTree as ET
from pathlib import Path
from zipfile import ZipFile


WORD_NAMESPACE = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def extract_docx_text_bytes(content: bytes) -> str:
    with ZipFile(BytesIO(content)) as archive:
        xml_bytes = archive.read("word/document.xml")

    root = ET.fromstring(xml_bytes)
    paragraphs = []

    for paragraph in root.findall(".//w:p", WORD_NAMESPACE):
        pieces = []
        for node in paragraph.iter():
            if node.tag == "{%s}t" % WORD_NAMESPACE["w"]:
                pieces.append(node.text or "")
            elif node.tag == "{%s}tab" % WORD_NAMESPACE["w"]:
                pieces.append("\t")
            elif node.tag == "{%s}br" % WORD_NAMESPACE["w"]:
                pieces.append("\n")
        text = "".join(pieces)
        text = re.sub(r"[ \t]+", " ", text).strip()
        if text:
            paragraphs.append(text)

    return "\n".join(paragraphs)


def extract_docx_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"DOCX file not found: {path}")
    return extract_docx_text_bytes(path.read_bytes())
