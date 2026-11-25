"""The 14-day schedule report: only days with full uptime count toward lag."""

from atlas_pipelines.cli import schedule_report_text


def row(day, lag_s, waits, coverage, status="SUCCEEDED"):
    return {"partition_key": day, "status": status, "fired_at": f"{day}T11:00:00Z", "finished_at": f"{day}T11:40:00Z",
            "lag_seconds": lag_s, "not_ready_attempts": waits, "last_not_ready_at": f"{day}T11:35:00Z" if waits else None,
            "first_success_at": f"{day}T11:39:00Z", "uptime_coverage": coverage, "fully_up": coverage >= 0.99}


def test_downtime_days_are_listed_but_not_counted():
    text = schedule_report_text([row("2026-11-27", 2400, 7, 1.0), row("2026-11-28", 36000, 0, 0.3),
                                 row("2026-11-29", 600, 1, 1.0)])
    assert "11:35-11:39" in text                       # publication bracketed by the last wait and first success
    assert "no (downtime or not finished)" in text      # the 30%-uptime day
    assert "2 of 3 days counted" in text and "max 40.0" in text and "median 40.0" in text
