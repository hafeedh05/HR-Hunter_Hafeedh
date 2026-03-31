from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from hr_hunter.config import resolve_secret
from hr_hunter.output import (
    CSV_FIELDNAMES,
    candidate_to_row,
    collect_seen_candidate_keys,
    filter_new_candidates,
    load_report,
    write_candidates_csv,
)


def _load_gspread_client(credentials_file: str | None = None):
    try:
        import gspread
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Google Sheets support is not installed. Run `pip install '.[sheets]'`."
        ) from exc

    explicit_file = str(credentials_file or "").strip()
    if explicit_file:
        return gspread.service_account(filename=explicit_file)

    secret_value = resolve_secret("GOOGLE_SERVICE_ACCOUNT_JSON")
    if secret_value:
        stripped = secret_value.strip()
        if stripped.startswith("{"):
            return gspread.service_account_from_dict(json.loads(stripped))
        return gspread.service_account(filename=stripped)

    raise RuntimeError(
        "Missing Google Sheets credentials. Set `GOOGLE_SERVICE_ACCOUNT_JSON` to a JSON blob or file path."
    )


def _find_or_create_worksheet(spreadsheet: Any, worksheet_name: str):
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except Exception:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=2000, cols=len(CSV_FIELDNAMES) + 4)
        worksheet.append_row(CSV_FIELDNAMES, value_input_option="USER_ENTERED")
        return worksheet, list(CSV_FIELDNAMES), set()

    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(CSV_FIELDNAMES, value_input_option="USER_ENTERED")
        return worksheet, list(CSV_FIELDNAMES), set()

    headers = [str(value).strip() for value in values[0]]
    identity_index = headers.index("identity_key") if "identity_key" in headers else -1
    existing_keys = set()
    if identity_index >= 0:
        for row in values[1:]:
            if len(row) > identity_index and str(row[identity_index]).strip():
                existing_keys.add(str(row[identity_index]).strip())
    return worksheet, headers, existing_keys


def append_report_to_sheet(
    *,
    report_path: Path,
    spreadsheet_id: str,
    worksheet_name: str,
    history_paths: Iterable[Path] = (),
    credentials_file: str | None = None,
    append_csv_path: Path | None = None,
) -> Dict[str, Any]:
    report = load_report(report_path)
    seen_keys = collect_seen_candidate_keys(history_paths)
    net_new_candidates = filter_new_candidates(report.candidates, seen_keys)
    if append_csv_path is not None:
        write_candidates_csv(net_new_candidates, append_csv_path)

    client = _load_gspread_client(credentials_file)
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet, headers, existing_sheet_keys = _find_or_create_worksheet(spreadsheet, worksheet_name)

    rows_to_append: List[List[Any]] = []
    observed_keys = set(existing_sheet_keys)
    appended_candidates = 0
    for candidate in net_new_candidates:
        row = candidate_to_row(candidate)
        identity_key = str(row.get("identity_key", "")).strip()
        if identity_key and identity_key in observed_keys:
            continue
        observed_keys.add(identity_key)
        rows_to_append.append([row.get(header, "") for header in headers])
        appended_candidates += 1

    if rows_to_append:
        worksheet.append_rows(rows_to_append, value_input_option="USER_ENTERED")

    return {
        "spreadsheet_id": spreadsheet_id,
        "worksheet_name": worksheet_name,
        "report_path": str(report_path),
        "candidate_count": len(report.candidates),
        "net_new_candidate_count": len(net_new_candidates),
        "appended_candidate_count": appended_candidates,
        "append_csv_path": str(append_csv_path) if append_csv_path is not None else "",
    }
