"""`run --partition A --to B` stops when the source is unreachable instead of timing out
on every task of every day (the owner's 2026-11-26 EIA outage: ~61 s per task, 7 BAs/day).
No network: tasks raise the same RetryableTaskError the EIA tasks raise on a timeout."""

import pytest

from atlas_pipelines import cli
from atlas_pipelines.core.pipeline import Pipeline, TaskDef
from atlas_pipelines.core.task import RetryableTaskError


def pipeline(name, fail_class, calls, ok_on=()):
    def fetch(ctx):
        calls.append(ctx.partition_key)
        if ctx.partition_key in ok_on:
            return [], []
        raise RetryableTaskError(fail_class, f"{fail_class} on {ctx.partition_key}")
    return Pipeline(name, "UTC", (TaskDef("ingest_a", fetch), TaskDef("ingest_b", fetch)))


def run(monkeypatch, lake, p, *args):
    monkeypatch.setattr(cli, "build_pipelines", lambda registry: {p.name: p})
    return cli.main(["--lake-root", str(lake), "run", p.name, *args])


def test_two_unreachable_days_stop_the_range(monkeypatch, lake, capsys):
    calls = []
    rc = run(monkeypatch, lake, pipeline("p", "network", calls), "--partition", "2026-10-27", "--to", "2026-11-24")
    assert rc == 1 and sorted(set(calls)) == ["2026-10-27", "2026-10-28"]  # 2 days, not 29
    out = capsys.readouterr().out
    assert "stopped at 2026-10-28" in out and "--partition 2026-10-28 --to 2026-11-24" in out
    assert "after Nones" not in out


def test_rate_limited_stops_at_once(monkeypatch, lake):
    calls = []
    assert run(monkeypatch, lake, pipeline("p", "rate_limited", calls), "--partition", "2026-10-27", "--to", "2026-10-30") == 1
    assert set(calls) == {"2026-10-27"}


def test_a_day_that_works_resets_the_count(monkeypatch, lake):
    calls = []
    p = pipeline("p", "server_error", calls, ok_on={"2026-10-28"})
    run(monkeypatch, lake, p, "--partition", "2026-10-27", "--to", "2026-10-30")
    assert sorted(set(calls)) == ["2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"]


@pytest.mark.parametrize("cls", ["source_not_ready", "data_check_failed"])
def test_data_problems_never_stop_the_range(monkeypatch, lake, cls):
    """Not-yet-published days or bad data are per-day facts, not an outage."""
    calls = []
    run(monkeypatch, lake, pipeline("p", cls, calls), "--partition", "2026-10-27", "--to", "2026-10-30")
    assert len(set(calls)) == 4
