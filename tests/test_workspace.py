import json
import sqlite3
from pathlib import Path

from hr_hunter.models import CandidateProfile, SearchRunReport
from hr_hunter.output import write_report
from hr_hunter.state import enqueue_job, load_job, start_job
from hr_hunter.workspace import (
    DEFAULT_ADMIN_EMAIL,
    authenticate_user,
    create_project,
    create_user_account,
    delete_project,
    delete_project_run,
    generate_totp_code,
    get_project,
    get_user_totp_setup,
    init_workspace_db,
    list_project_runs,
    list_projects,
    list_users,
    save_project_brief,
    update_project,
)


def test_workspace_seeds_admin_and_supports_login(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)

    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    auth = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )

    assert auth["user"]["is_admin"] is True
    assert auth["user"]["email"] == DEFAULT_ADMIN_EMAIL
    assert auth["session_token"]


def test_admin_can_create_recruiter_and_assign_project(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    recruiter = create_user_account(
        email="recruiter.one@hyve",
        full_name="Recruiter One",
        team_id="analytics",
        created_by=admin["id"],
        db_path=db_path,
    )

    users = list_users(db_path=db_path)
    assert recruiter["email"] in [user["email"] for user in users]

    project = create_project(
        admin,
        name="Senior Data Analyst - UAE",
        client_name="Azadea",
        role_title="Senior Data Analyst",
        target_geography="United Arab Emirates",
        notes="Primary search mandate",
        brief_json={"role_title": "Senior Data Analyst"},
        assigned_user_ids=[recruiter["id"]],
        db_path=db_path,
    )

    assert project["name"] == "Senior Data Analyst - UAE"
    assert recruiter["id"] in [member["id"] for member in project["assigned_recruiters"]]
    assert admin["id"] in [member["id"] for member in project["assigned_recruiters"]]
    assert recruiter["totp"]["secret"]

    recruiter_auth = authenticate_user(
        "recruiter.one@hyve",
        generate_totp_code(recruiter["totp"]["secret"]),
        db_path=db_path,
    )
    recruiter_view = get_project(recruiter_auth["user"], project["id"], db_path=db_path)
    assert recruiter_view["id"] == project["id"]


def test_project_updates_and_filters_by_access(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    recruiter = create_user_account(
        email="recruiter.two@hyve",
        full_name="Recruiter Two",
        team_id="market",
        created_by=admin["id"],
        db_path=db_path,
    )
    other = create_user_account(
        email="recruiter.three@hyve",
        full_name="Recruiter Three",
        team_id="market",
        created_by=admin["id"],
        db_path=db_path,
    )

    project = create_project(
        recruiter,
        name="Growth Analyst",
        client_name="Retail Client",
        role_title="Growth Analyst",
        assigned_user_ids=[admin["id"]],
        db_path=db_path,
    )

    updated = update_project(
        recruiter,
        project_id=project["id"],
        name="Growth Analyst - GCC",
        client_name="Retail Client",
        role_title="Growth Analyst",
        target_geography="GCC",
        status="on_hold",
        notes="Paused for budget approval",
        brief_json={"titles": ["Growth Analyst"]},
        assigned_user_ids=[admin["id"], recruiter["id"]],
        db_path=db_path,
    )
    saved = save_project_brief(
        recruiter,
        project_id=project["id"],
        brief_json={"titles": ["Growth Analyst"], "countries": ["United Arab Emirates"]},
        role_title="Growth Analyst",
        target_geography="United Arab Emirates",
        db_path=db_path,
    )

    recruiter_projects = list_projects(recruiter, db_path=db_path)
    other_projects = list_projects(other, db_path=db_path)

    assert updated["status"] == "on_hold"
    assert saved["target_geography"] == "United Arab Emirates"
    assert recruiter_projects[0]["name"] == "Growth Analyst - GCC"
    assert other_projects == []


def test_delete_project_removes_project_and_related_access(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    project = create_project(
        admin,
        name="Delete Me",
        client_name="Client",
        role_title="Role",
        target_geography="UAE",
        brief_json={"role_title": "Role"},
        db_path=db_path,
    )

    deleted = delete_project(admin, project_id=project["id"], db_path=db_path)

    assert deleted["id"] == project["id"]
    assert list_projects(admin, db_path=db_path) == []


def test_delete_project_stops_running_jobs_for_that_project(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    project = create_project(
        admin,
        name="Delete With Running Job",
        client_name="Client",
        role_title="Supply Chain Manager",
        target_geography="UAE",
        brief_json={"role_title": "Supply Chain Manager"},
        db_path=db_path,
    )

    job = enqueue_job(
        "search",
        {"project_id": project["id"], "role_title": "Supply Chain Manager"},
        db_path=db_path,
    )
    start_job(job["job_id"], db_path=db_path)

    deleted = delete_project(admin, project_id=project["id"], db_path=db_path)
    stopped = load_job(job["job_id"], db_path=db_path)

    assert deleted["id"] == project["id"]
    assert job["job_id"] in deleted["stopped_job_ids"]
    assert stopped is not None
    assert stopped["status"] == "failed"
    assert "project was deleted" in stopped["error"]


def test_admin_can_delete_single_project_run_and_repoint_latest_run(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    project = create_project(
        admin,
        name="Run Cleanup",
        client_name="Client",
        role_title="Senior Product Manager",
        target_geography="UAE",
        brief_json={"role_title": "Senior Product Manager"},
        db_path=db_path,
    )

    json_path_old = tmp_path / "run-old.json"
    csv_path_old = tmp_path / "run-old.csv"
    json_path_new = tmp_path / "run-new.json"
    csv_path_new = tmp_path / "run-new.csv"
    for artifact in [json_path_old, csv_path_old, json_path_new, csv_path_new]:
        artifact.write_text("artifact", encoding="utf-8")

    with sqlite3.connect(str(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            INSERT INTO search_runs (
                id, mandate_id, brief_id, org_id, status, execution_backend, provider_order_json,
                summary_json, report_json_path, report_csv_path, dry_run, candidate_count,
                accepted_count, limit_requested, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run_old",
                project["id"],
                "brief_old",
                "local:project:" + project["id"],
                "completed",
                "local_engine",
                json.dumps(["scrapingbee_google"]),
                json.dumps({"verified_count": 1}),
                str(json_path_old),
                str(csv_path_old),
                0,
                12,
                3,
                25,
                "2026-04-07T08:00:00+00:00",
                "2026-04-07T08:00:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO search_runs (
                id, mandate_id, brief_id, org_id, status, execution_backend, provider_order_json,
                summary_json, report_json_path, report_csv_path, dry_run, candidate_count,
                accepted_count, limit_requested, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run_new",
                project["id"],
                "brief_new",
                "local:project:" + project["id"],
                "completed",
                "local_engine",
                json.dumps(["scrapingbee_google"]),
                json.dumps({"verified_count": 2}),
                str(json_path_new),
                str(csv_path_new),
                0,
                20,
                6,
                25,
                "2026-04-07T09:00:00+00:00",
                "2026-04-07T09:00:00+00:00",
            ),
        )
        connection.execute(
            "UPDATE projects SET latest_run_id = ?, latest_run_at = ? WHERE id = ?",
            ("run_new", "2026-04-07T09:00:00+00:00", project["id"]),
        )
        connection.execute(
            """
            INSERT INTO candidate_registry (
                id, org_id, identity_key, full_name, latest_candidate_json, search_ids_json,
                search_count, first_seen_at, last_seen_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "local:project:" + project["id"] + ":candidate_a",
                "local:project:" + project["id"],
                "candidate_a",
                "Candidate A",
                "{}",
                json.dumps(["run_old", "run_new"]),
                2,
                "2026-04-07T08:00:00+00:00",
                "2026-04-07T09:00:00+00:00",
                "2026-04-07T08:00:00+00:00",
                "2026-04-07T09:00:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO run_candidates (
                run_id, mandate_id, candidate_id, rank_index, score, verification_status,
                qualification_tier, feature_json, anchor_json, source, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run_old",
                project["id"],
                "candidate_a",
                1,
                82.0,
                "verified",
                "qualified",
                "{}",
                "{}",
                "scrapingbee_google",
                "2026-04-07T08:00:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO review_actions (
                mandate_id, run_id, candidate_id, reviewer_id, reviewer_name,
                owner_id, action, reason_code, note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project["id"],
                "run_old",
                "candidate_a",
                admin["id"],
                "HR Hunter Admin",
                admin["id"],
                "shortlist",
                "good_fit",
                "Strong fit",
                "2026-04-07T08:05:00+00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO candidate_reviews (
                candidate_id, mandate_id, owner_id, owner_name, latest_action,
                latest_reason_code, latest_note, latest_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "candidate_a",
                project["id"],
                admin["id"],
                "HR Hunter Admin",
                "shortlist",
                "good_fit",
                "Strong fit",
                "run_old",
                "2026-04-07T08:05:00+00:00",
            ),
        )

    deleted = delete_project_run(admin, project_id=project["id"], run_id="run_old", db_path=db_path)

    assert deleted["run_id"] == "run_old"
    assert not json_path_old.exists()
    assert not csv_path_old.exists()
    assert json_path_new.exists()
    assert csv_path_new.exists()

    remaining = get_project(admin, project["id"], db_path=db_path)
    assert remaining["latest_run_id"] == "run_new"

    with sqlite3.connect(str(db_path)) as connection:
        connection.row_factory = sqlite3.Row
        assert connection.execute("SELECT COUNT(*) FROM search_runs WHERE id = 'run_old'").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM run_candidates WHERE run_id = 'run_old'").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM review_actions WHERE run_id = 'run_old'").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM candidate_reviews WHERE latest_run_id = 'run_old'").fetchone()[0] == 0
        registry_row = connection.execute(
            "SELECT search_ids_json, search_count FROM candidate_registry WHERE identity_key = 'candidate_a'"
        ).fetchone()
        assert registry_row is not None
        assert json.loads(registry_row["search_ids_json"]) == ["run_new"]
        assert registry_row["search_count"] == 1


def test_list_project_runs_recovers_summary_from_saved_report(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    project = create_project(
        admin,
        name="Summary Recovery",
        client_name="Client",
        role_title="Senior Data Analyst",
        target_geography="UAE",
        brief_json={"role_title": "Senior Data Analyst"},
        db_path=db_path,
    )

    report = SearchRunReport(
        run_id="workspace-summary-run",
        brief_id="brief",
        dry_run=False,
        generated_at="2026-04-07T00:00:00+00:00",
        provider_results=[],
        candidates=[CandidateProfile(full_name="Verified Candidate", verification_status="verified", score=82.0)],
        summary={},
    )
    json_path, csv_path = write_report(report, tmp_path)

    with sqlite3.connect(str(db_path)) as connection:
        connection.execute(
            """
            INSERT INTO search_runs (
                id, mandate_id, brief_id, org_id, status, execution_backend, provider_order_json,
                summary_json, report_json_path, report_csv_path, dry_run, candidate_count,
                accepted_count, limit_requested, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report.run_id,
                project["id"],
                "brief",
                "local:project:" + project["id"],
                "completed",
                "local_engine",
                json.dumps(["scrapingbee_google"]),
                json.dumps({}),
                str(json_path),
                str(csv_path),
                0,
                1,
                1,
                25,
                "2026-04-07T09:00:00+00:00",
                "2026-04-07T09:00:00+00:00",
            ),
        )

    runs = list_project_runs(admin, project_id=project["id"], db_path=db_path)

    assert runs[0]["summary"]["verified_count"] == 1
    assert runs[0]["summary"]["candidate_count"] == 1


def test_rotating_user_totp_invalidates_old_code(tmp_path: Path):
    db_path = tmp_path / "workspace.db"
    init_workspace_db(db_path)
    admin_setup = get_user_totp_setup(email=DEFAULT_ADMIN_EMAIL, db_path=db_path)
    admin = authenticate_user(
        DEFAULT_ADMIN_EMAIL,
        generate_totp_code(admin_setup["totp"]["secret"]),
        db_path=db_path,
    )["user"]

    recruiter = create_user_account(
        email="recruiter.rotate@hyve",
        full_name="Rotate Recruiter",
        team_id="ops",
        created_by=admin["id"],
        db_path=db_path,
    )

    original_code = generate_totp_code(recruiter["totp"]["secret"])
    rotated = get_user_totp_setup(user_id=recruiter["id"], rotate=True, db_path=db_path)

    assert rotated["totp"]["secret"] != recruiter["totp"]["secret"]

    try:
        authenticate_user("recruiter.rotate@hyve", original_code, db_path=db_path)
    except ValueError as exc:
        assert "verification code" in str(exc).lower()
    else:
        raise AssertionError("Old TOTP code should no longer be accepted after rotation.")

    fresh_auth = authenticate_user(
        "recruiter.rotate@hyve",
        generate_totp_code(rotated["totp"]["secret"]),
        db_path=db_path,
    )

    assert fresh_auth["user"]["email"] == "recruiter.rotate@hyve"
