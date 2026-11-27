"""Golden tests: real ENTSO-E balancing answers (17.1.B&C reserves, 17.1.G imbalance) from the owner's
`make fixture-entsoe-balancing` + the German TSO-area probe (2026-11-27; local-only, skip in CI).

Per answer: every row equals its raw point re-read independently from the ZIP (A03 carry within a
Period), every code is known, every series covers the local day (DST days: 92/100 quarter-hours,
23/25 hours), and the ES day whose final document lacks 19:30Z takes that quarter-hour from the
intermediate document, flagged. Failure cases change one thing in a real answer. No network.
"""

import io
import re
import zipfile
from datetime import date, datetime, timedelta, timezone

import pytest

from atlas_pipelines.core.markets import load_registry
from atlas_pipelines.sources.entsoe import balancing as bal
from atlas_pipelines.sources.entsoe import day_ahead as da
from conftest import FIXTURES, REPO

M = load_registry(REPO / "config" / "markets")["entsoe"]
FX = FIXTURES / "entsoe"
CASES = sorted(FX.glob("*.zip")) if FX.exists() else []
pytestmark = pytest.mark.skipif(not CASES, reason="ENTSO-E balancing fixtures are local-only")
UNITS = {"imbalance": "EUR/MWh", "reserves": "EUR/MW (period not stated in the answer)"}
FETCHED = datetime(2026, 11, 27, tzinfo=timezone.utc)


def meta(path):
    zone, rest = path.stem.split("_20", 1)
    return zone, date.fromisoformat("20" + rest[:8]), "imbalance" if "imbalance" in path.stem else "reserves"


def run(raw, zone, day, kind):
    docs = bal.parse_answer(raw)
    area = docs[0].area
    df, problems = bal.build_silver(docs, kind, M, zone, area, day, FETCHED, "fixture", UNITS)
    return df, bal.checks(df, docs, kind, M, zone, area, day, problems, "EUR")


def raw_values(raw):
    """(category, process, direction, decided, interval start) -> (price, quantity, status), re-read with regexes."""
    out = {}
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        for name in sorted(z.namelist()):
            t = z.read(name).decode()
            proc = re.search(r"<process.processType>([^<]+)", t).group(1)
            status = re.search(r"<docStatus>\s*<value>([^<]+)", t)
            decided = re.search(r"<allocationDecision_DateAndOrTime.dateTime>([^<]+)", t)
            for ts in re.findall(r"<TimeSeries>(.*?)</TimeSeries>", t, re.S):
                direction = re.search(r"<flowDirection.direction>([^<]+)", ts)
                for per in re.findall(r"<Period>(.*?)</Period>", ts, re.S):
                    p0 = datetime.fromisoformat(re.search(r"<start>([^<]+)Z</start>", per).group(1)).replace(tzinfo=timezone.utc)
                    res = int(re.search(r"<resolution>PT(\d+)M", per).group(1))
                    for pt in re.findall(r"<Point>(.*?)</Point>", per, re.S):
                        pos = int(re.search(r"<position>(\d+)", pt).group(1))
                        price = float(re.search(r"<(?:imbalance|procurement)_Price.amount>([^<]+)", pt).group(1))
                        q = re.search(r"<quantity>([^<]+)", pt)
                        cat = re.search(r"<imbalance_Price.category>([^<]+)", pt).group(1)
                        out[(cat, proc, direction.group(1) if direction else None, decided.group(1) if decided else None,
                             p0 + (pos - 1) * timedelta(minutes=res))] = (price, float(q.group(1)) if q else None,
                                                                         status.group(1) if status else None)
    return out


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
def test_real_answer_parses_completely_and_exactly(path):
    zone, day, kind = meta(path)
    raw = path.read_bytes()
    df, cs = run(raw, zone, day, kind)
    assert [c.check_name for c in cs if c.severity == "blocking" and not c.passed] == []
    assert df["series_complete"].all() and all(c.passed for c in cs if c.check_name.startswith("series_complete:"))
    start, end = M.day_bounds_utc(day)
    for _, s in df.group_by("product", "area_eic", "decided_at_utc"):
        assert s.height == (end - start) / timedelta(minutes=int(s["interval_minutes"][0]))  # DST-aware count
    # Every row equals a listed raw point, or the latest listed one before it in its series (A03 carry).
    pts = raw_values(raw)
    for r in df.iter_rows(named=True):
        listed = {k: v for k, v in pts.items() if k[4] <= r["interval_start_utc"]
                  and (k[2] is None or bal.DIRECTION[k[2]] in r["product"])
                  and (k[1] == "A16" or bal.RESERVE_PROCESS[k[1]] == r["product"].split("_")[0])
                  and (k[0] not in bal.IMBALANCE or bal.IMBALANCE[k[0]] == r["product"])
                  and (r["decided_at_utc"] is None or k[3] and datetime.fromisoformat(k[3].replace("Z", "+00:00").replace(".000", "")) == r["decided_at_utc"])
                  and (r["doc_status"] is None or bal.STATUS[v[2]] == r["doc_status"])}
        latest = max(listed, key=lambda k: k[4])
        assert (r["price"], r["quantity_mw"]) == listed[latest][:2], (path.stem, r["product"], r["interval_start_utc"])


def by(zone, day, kind):
    return next(p for p in CASES if meta(p) == (zone, date.fromisoformat(day), kind) and "probe" not in p.stem)


def test_final_document_wins_and_the_missing_quarter_hour_comes_from_the_intermediate_one():
    df, _ = run(by("ES", "2026-11-18", "imbalance").read_bytes(), "ES", date(2026, 11, 18), "imbalance")
    hole = datetime(2026, 11, 18, 19, 30, tzinfo=timezone.utc)
    inter = df.filter(df["doc_status"] == "intermediate")
    assert set(inter["interval_start_utc"]) == {hole} and inter.height == 2  # excess + insufficient, flagged
    assert df.filter(df["interval_start_utc"] != hole)["doc_status"].unique().to_list() == ["final"]


def test_two_fcr_auctions_for_one_day_are_both_kept_with_their_decision_times():
    df, _ = run(by("SE3", "2026-11-18", "reserves").read_bytes(), "SE3", date(2026, 11, 18), "reserves")
    fcr = df.filter(df["product"] == "fcr_capacity_up_down")
    assert sorted(fcr["decided_at_utc"].unique().to_list()) == [datetime(2026, 11, 17, 5, tzinfo=timezone.utc),
                                                                 datetime(2026, 11, 17, 18, tzinfo=timezone.utc)]
    assert fcr.height == 48 and set(fcr["price_type"]) == {"marginal"}


def test_german_reserves_come_per_tso_area():
    probes = [p for p in CASES if "probe" in p.stem]
    assert len(probes) == 4
    for p in probes:
        df, _ = run(p.read_bytes(), "DE_LU", date(2026, 11, 18), "reserves")
        assert set(df["product"]) == {"fcr_capacity_up_down", "afrr_capacity_up", "afrr_capacity_down",
                                      "mfrr_capacity_up", "mfrr_capacity_down"} and df["area_eic"].n_unique() == 1


def change(path, old: bytes, new: bytes) -> bytes:
    """The real ZIP with one byte-string change inside its first document."""
    src = zipfile.ZipFile(io.BytesIO(path.read_bytes()))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for i, name in enumerate(sorted(src.namelist())):
            data = src.read(name)
            if i == 0:
                assert old in data
                data = data.replace(old, new, 1)
            out.writestr(name, data)
    return buf.getvalue()


@pytest.mark.parametrize("old,new,check", [
    (b"<imbalance_Price.category>A04<", b"<imbalance_Price.category>A99<", "codes_known"),   # unknown category
    (b"<currency_Unit.name>EUR<", b"<currency_Unit.name>SEK<", "currency_matches_registry"),
    (b"<area_Domain.mRID codingScheme=\"A01\">10YES-REE------0<", b"<area_Domain.mRID codingScheme=\"A01\">10YFR-RTE------C<",
     "documents_for_area"),
])
def test_unexpected_content_blocks(old, new, check):
    raw = change(by("ES", "2026-11-18", "imbalance"), old, new)
    docs = bal.parse_answer(raw)
    df, problems = bal.build_silver(docs, "imbalance", M, "ES", "10YES-REE------0", date(2026, 11, 18), FETCHED, "x", UNITS)
    cs = bal.checks(df, docs, "imbalance", M, "ES", "10YES-REE------0", date(2026, 11, 18), problems, "EUR")
    assert check in [c.check_name for c in cs if c.severity == "blocking" and not c.passed]


def test_unknown_reserve_product_blocks_never_defaults():
    raw = change(by("SE3", "2026-11-18", "reserves"), b"<process.processType>A52<", b"<process.processType>A99<")
    df, cs = run(raw, "SE3", date(2026, 11, 18), "reserves")
    assert "codes_known" in [c.check_name for c in cs if c.severity == "blocking" and not c.passed]
    # The changed auction (decided 18:00Z) is left out, never mapped to some default product.
    assert df.filter(df["decided_at_utc"] == datetime(2026, 11, 17, 18, tzinfo=timezone.utc)).is_empty()


def test_final_value_wins_where_an_intermediate_one_overlaps():
    """One change: the real intermediate document is moved to 19:15Z, a quarter-hour the final one has."""
    path = by("ES", "2026-11-18", "imbalance")
    src = zipfile.ZipFile(io.BytesIO(path.read_bytes()))
    names = sorted(src.namelist())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as out:
        for i, name in enumerate(names):
            data = src.read(name)
            if i == 1:  # the intermediate document (docStatus A01)
                assert b"<value>A01</value>" in data
                data = data.replace(b"2026-11-18T19:30Z", b"2026-11-18T19:15Z").replace(b"2026-11-18T19:45Z", b"2026-11-18T19:30Z")
            out.writestr(name, data)
    df, cs = run(buf.getvalue(), "ES", date(2026, 11, 18), "imbalance")
    final, _ = run(path.read_bytes(), "ES", date(2026, 11, 18), "imbalance")
    t = datetime(2026, 11, 18, 19, 15, tzinfo=timezone.utc)
    at = df.filter(df["interval_start_utc"] == t).sort("product")
    assert at["doc_status"].to_list() == ["final", "final"]
    assert at["price"].to_list() == final.filter(final["interval_start_utc"] == t).sort("product")["price"].to_list()
    assert not df["series_complete"].any()  # 19:30Z is now missing in both series: missing stays missing


def test_acknowledgement_is_recognised():
    ack = bal.parse_answer((FX / "DE_LU_2026-11-18_normal_imbalance_prices.xml").read_bytes())
    assert isinstance(ack, da.Acknowledgement) and ack.no_data
