from hr_hunter.api import _job_actor_from_payload


def test_job_actor_from_payload_preserves_admin_flag():
    actor = _job_actor_from_payload(
        {
            "recruiter_id": "user_admin_hrhunter",
            "recruiter_name": "HR Hunter Admin",
            "team_id": "leadership",
            "recruiter_is_admin": True,
        }
    )

    assert actor == {
        "id": "user_admin_hrhunter",
        "full_name": "HR Hunter Admin",
        "team_id": "leadership",
        "is_admin": True,
    }
