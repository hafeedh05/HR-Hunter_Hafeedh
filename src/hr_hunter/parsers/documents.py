from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path
from typing import Any, Dict

from hr_hunter.parsers.docx import extract_docx_text, extract_docx_text_bytes


SUPPORTED_JD_UPLOAD_EXTENSIONS = {".pdf", ".doc", ".docx", ".txt", ".md", ".rtf"}


def _normalize_document_text(text: str) -> str:
    lines = []
    for raw_line in str(text or "").splitlines():
        cleaned = re.sub(r"\s+", " ", raw_line).strip()
        if cleaned:
            lines.append(cleaned)
    normalized = "\n".join(lines)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
    return normalized


def _score_text_candidate(text: str) -> tuple[int, int]:
    normalized = _normalize_document_text(text)
    words = re.findall(r"[A-Za-z][A-Za-z0-9&/\-]{2,}", normalized)
    return (len(words), len(normalized))


def _extract_text_text_bytes(content: bytes) -> str:
    candidates = []
    for encoding in ("utf-8-sig", "utf-8", "utf-16", "utf-16le", "cp1252", "latin-1"):
        try:
            decoded = content.decode(encoding)
        except UnicodeDecodeError:
            continue
        normalized = _normalize_document_text(decoded)
        if normalized:
            candidates.append(normalized)
    if not candidates:
        decoded = content.decode("utf-8", errors="ignore")
        normalized = _normalize_document_text(decoded)
        if normalized:
            candidates.append(normalized)
    if not candidates:
        raise ValueError("The uploaded text file did not contain readable text.")
    return max(candidates, key=_score_text_candidate)


def _extract_pdf_text_bytes(content: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:  # pragma: no cover - dependency verified in runtime
        raise RuntimeError("PDF upload support is not installed. Install `pypdf` to enable it.") from exc

    reader = PdfReader(BytesIO(content))
    pages = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    normalized = _normalize_document_text("\n\n".join(pages))
    if not normalized:
        raise ValueError("The uploaded PDF did not contain extractable text.")
    return normalized


def _extract_doc_text_bytes(content: bytes) -> str:
    candidates = []

    for encoding in ("utf-16le", "utf-8", "cp1252", "latin-1"):
        decoded = content.decode(encoding, errors="ignore")
        normalized = _normalize_document_text(decoded)
        if normalized:
            candidates.append(normalized)

    printable_chunks = re.findall(rb"(?:[\x20-\x7E]|[\x80-\xFF]){5,}", content)
    if printable_chunks:
        joined = "\n".join(
            chunk.decode("cp1252", errors="ignore")
            for chunk in printable_chunks
        )
        normalized = _normalize_document_text(joined)
        if normalized:
            candidates.append(normalized)

    candidates = [candidate for candidate in candidates if _score_text_candidate(candidate)[0] >= 12]
    if not candidates:
        raise ValueError("The uploaded .doc file could not be read reliably. Try saving it as PDF or DOCX.")
    return max(candidates, key=_score_text_candidate)


def extract_document_text_from_bytes(filename: str, content: bytes) -> Dict[str, Any]:
    if not str(filename or "").strip():
        raise ValueError("The uploaded JD file must have a name.")

    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_JD_UPLOAD_EXTENSIONS:
        allowed = ", ".join(sorted(SUPPORTED_JD_UPLOAD_EXTENSIONS))
        raise ValueError(f"Unsupported JD file type `{suffix or 'unknown'}`. Supported types: {allowed}.")

    if suffix == ".pdf":
        text = _extract_pdf_text_bytes(content)
        parser = "pdf"
    elif suffix == ".docx":
        text = extract_docx_text_bytes(content)
        parser = "docx"
    elif suffix == ".doc":
        text = _extract_doc_text_bytes(content)
        parser = "doc_best_effort"
    else:
        text = _extract_text_text_bytes(content)
        parser = "text"

    if not text.strip():
        raise ValueError("The uploaded JD file did not contain any usable text.")

    return {
        "file_name": Path(filename).name,
        "file_extension": suffix,
        "parser": parser,
        "text": text,
    }


def extract_document_text_from_path(path: Path) -> str:
    file_path = Path(path).expanduser().resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"Document file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".docx":
        return extract_docx_text(file_path)
    return extract_document_text_from_bytes(file_path.name, file_path.read_bytes())["text"]
