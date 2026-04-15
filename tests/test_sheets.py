from pathlib import Path

from hr_hunter.models import CandidateProfile, SearchRunReport
from hr_hunter.output import CSV_FIELDNAMES, candidate_to_row, write_report
from hr_hunter.sheets import append_report_to_sheet


class FakeWorksheet:
    def __init__(self, values=None):
        self.values = [list(row) for row in (values or [])]

    def get_all_values(self):
        return [list(row) for row in self.values]

    def append_row(self, row, value_input_option=None):
        self.values.append(list(row))

    def append_rows(self, rows, value_input_option=None):
        for row in rows:
            self.values.append(list(row))


class FakeSpreadsheet:
    def __init__(self, worksheet_name: str, worksheet: FakeWorksheet):
        self.worksheets = {worksheet_name: worksheet}

    def worksheet(self, worksheet_name: str):
        if worksheet_name not in self.worksheets:
            raise RuntimeError("worksheet missing")
        return self.worksheets[worksheet_name]

    def add_worksheet(self, title: str, rows: int, cols: int):
        worksheet = FakeWorksheet()
        self.worksheets[title] = worksheet
        return worksheet


class FakeClient:
    def __init__(self, spreadsheet: FakeSpreadsheet):
        self.spreadsheet = spreadsheet

    def open_by_key(self, spreadsheet_id: str):
        return self.spreadsheet


def _build_report(tmp_path: Path, run_id: str, candidates: list[CandidateProfile]) -> Path:
    report = SearchRunReport(
        run_id=run_id,
        brief_id="sheet-test",
        dry_run=False,
        generated_at="2026-03-31T00:00:00+00:00",
        provider_results=[],
        candidates=candidates,
        summary={},
    )
    json_path, _ = write_report(report, tmp_path / run_id)
    return json_path


def test_append_report_to_sheet_filters_history_and_existing_sheet_rows(tmp_path: Path, monkeypatch) -> None:
    historical_candidate = CandidateProfile(
        full_name="Historical FMCG",
        current_title="Brand Manager",
        current_company="Unilever",
        location_name="Dublin, Ireland",
        linkedin_url="https://www.linkedin.com/in/historical-fmcg",
        source="scrapingbee_google",
        source_url="https://www.linkedin.com/in/historical-fmcg",
        verification_status="verified",
        score=78.0,
    )
    existing_sheet_candidate = CandidateProfile(
        full_name="Existing Sheet FMCG",
        current_title="Category Manager",
        current_company="Beiersdorf",
        location_name="Dublin, Ireland",
        linkedin_url="https://www.linkedin.com/in/existing-sheet-fmcg",
        source="scrapingbee_google",
        source_url="https://www.linkedin.com/in/existing-sheet-fmcg",
        verification_status="review",
        score=63.0,
    )
    new_candidate = CandidateProfile(
        full_name="New Ireland FMCG",
        current_title="Senior Brand Manager",
        current_company="Colgate-Palmolive",
        location_name="Drogheda, Ireland",
        linkedin_url="https://www.linkedin.com/in/new-ireland-fmcg",
        source="scrapingbee_google",
        source_url="https://www.linkedin.com/in/new-ireland-fmcg",
        verification_status="verified",
        score=81.0,
        search_strategies=["current-target-strict", "trade-media-location-probe"],
    )

    history_report_path = _build_report(tmp_path, "history", [historical_candidate])
    current_report_path = _build_report(
        tmp_path,
        "current",
        [historical_candidate, existing_sheet_candidate, new_candidate],
    )

    existing_row = candidate_to_row(existing_sheet_candidate)
    worksheet = FakeWorksheet(
        [CSV_FIELDNAMES, [existing_row.get(header, "") for header in CSV_FIELDNAMES]]
    )
    spreadsheet = FakeSpreadsheet("Candidates", worksheet)
    monkeypatch.setattr(
        "hr_hunter.sheets._load_gspread_client",
        lambda credentials_file=None: FakeClient(spreadsheet),
    )

    append_csv_path = tmp_path / "append.csv"
    result = append_report_to_sheet(
        report_path=current_report_path,
        spreadsheet_id="spreadsheet-123",
        worksheet_name="Candidates",
        history_paths=[history_report_path],
        append_csv_path=append_csv_path,
    )

    assert result["candidate_count"] == 3
    assert result["net_new_candidate_count"] == 2
    assert result["appended_candidate_count"] == 1
    assert append_csv_path.exists()
    assert len(worksheet.values) == 3
    assert worksheet.values[-1][CSV_FIELDNAMES.index("Candidate Name")] == "New Ireland FMCG"
