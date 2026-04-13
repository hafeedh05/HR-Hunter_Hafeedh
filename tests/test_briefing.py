from pathlib import Path
from zipfile import ZipFile

from hr_hunter.briefing import build_search_brief
from hr_hunter.parsers.docx import extract_docx_text


def write_minimal_docx(path: Path, text: str) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""
    document = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>{text}</w:t></w:r></w:p>
  </w:body>
</w:document>
"""
    with ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", rels)
        archive.writestr("word/document.xml", document)


def test_extract_docx_text_reads_document_text(tmp_path: Path) -> None:
    document_path = tmp_path / "brief.docx"
    write_minimal_docx(document_path, "Hello Search")

    assert extract_docx_text(document_path) == "Hello Search"


def test_build_search_brief_merges_aliases_and_doc_text(tmp_path: Path) -> None:
    document_path = tmp_path / "brief.docx"
    write_minimal_docx(document_path, "Matrix leadership across consumer products")

    brief = build_search_brief(
        {
            "id": "test-brief",
            "role_title": "Global Product Manager",
            "brief_document_path": str(document_path),
            "titles": ["Global Product Manager"],
            "company_targets": ["Procter & Gamble"],
            "company_aliases": {"Procter & Gamble": ["P&G"]},
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
            "required_keywords": ["product strategy"],
        }
    )

    assert brief.document_text == "Matrix leadership across consumer products"
    assert "P&G" in brief.company_aliases["Procter & Gamble"]
    assert "Procter and Gamble" in brief.company_aliases["Procter & Gamble"]


def test_build_search_brief_ignores_missing_doc_path() -> None:
    brief = build_search_brief(
        {
            "id": "test-brief-missing-doc",
            "role_title": "Global Product Manager",
            "brief_document_path": "/does/not/exist.docx",
            "brief_summary": "Fallback summary",
            "titles": ["Global Product Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )

    assert brief.document_text == ""
    assert brief.brief_summary == "Fallback summary"


def test_build_search_brief_infers_adjacent_fmcg_titles() -> None:
    brief = build_search_brief(
        {
            "id": "test-brief-adjacent-titles",
            "role_title": "Brand / Category Lead",
            "titles": ["Brand Manager", "Category Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )

    assert "shopper marketing manager" in [value.lower() for value in brief.title_keywords]
    assert "customer marketing manager" in [value.lower() for value in brief.title_keywords]
    assert "category and insights manager" in [value.lower() for value in brief.title_keywords]


def test_build_search_brief_strips_generic_singleton_title_keywords() -> None:
    brief = build_search_brief(
        {
            "id": "test-brief-generic-singleton-title-keyword",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst", "Product Analyst"],
            "title_keywords": ["product"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )

    lowered_keywords = [value.lower() for value in brief.title_keywords]

    assert "product" not in lowered_keywords
    assert "product analyst" in lowered_keywords
    assert "product development manager" not in lowered_keywords


def test_build_search_brief_resolves_anchor_priorities_and_numeric_weights() -> None:
    brief = build_search_brief(
        {
            "id": "test-brief-anchors",
            "role_title": "Product Lead",
            "titles": ["Product Lead"],
            "company_targets": ["Acme"],
            "geography": {"location_name": "Dubai", "country": "UAE"},
            "anchors": {
                "title": "critical",
                "company": "preferred",
                "location": "important",
            },
            "anchor_weights": {
                "skills": 0.9,
            },
        }
    )

    assert brief.anchor_weights["title_similarity"] == 1.0
    assert brief.anchor_weights["company_match"] == 0.6
    assert brief.anchor_weights["location_match"] == 0.75
    assert brief.anchor_weights["skill_overlap"] == 0.9


def test_build_search_brief_ignores_legacy_company_interest_fields() -> None:
    brief = build_search_brief(
        {
            "id": "test-brief-company-interest",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "hiring_company_name": "OpenAI",
            "hiring_company_aliases": ["Open AI"],
            "candidate_interest_required": True,
            "anchors": {
                "candidate_interest": "important",
            },
        }
    )

    assert brief.role_title == "Senior Data Analyst"
    assert "company_interest" not in brief.anchor_weights
