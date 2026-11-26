"""Daily data-quality report (B6, PHASE_AB_PLAN.md §3.6 "Outputs of validation").

Why it exists: warnings are only useful if someone reads them. One Markdown page per UTC day:
runs finished that day by pipeline and status, failed blocking checks, failed warnings
(outliers, revisions, gaps), retryable failures by error class. Built from the run manifests
the local runner writes (lake/_meta/runs); late assets come from Atlas (/ui/assets), which the
local manifests don't know.
Who calls it: `python -m atlas_pipelines dq-report --date YYYY-MM-DD`.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path


def collect(lake_root: str, day: str) -> dict:
    runs = []
    for f in sorted((Path(lake_root) / "_meta" / "runs").glob("*.json")):
        m = json.loads(f.read_text(encoding="utf-8"))
        if str(m.get("finished_at", "")).startswith(day):
            runs.append(m)
    status = defaultdict(Counter)
    blocking, warns, retry = Counter(), Counter(), Counter()
    examples = defaultdict(list)
    for r in runs:
        status[r["pipeline"]][r["status"]] += 1
        for t in r.get("tasks", []):
            if t.get("retryable"):
                retry[t.get("error_class") or "?"] += 1
            for c in t.get("checks", []):
                if c["passed"]:
                    continue
                key = (r["pipeline"], c["check_name"])
                (blocking if c["severity"] == "blocking" else warns)[key] += 1
                if len(examples[key]) < 3:
                    examples[key].append(f"{r['partition_key']} {t['task']}")
    return {"day": day, "runs": len(runs), "status": status, "blocking": blocking, "warns": warns,
            "retryable": retry, "examples": examples}


def markdown(s: dict) -> str:
    out = [f"# Data quality - {s['day']} (UTC)", "", f"Runs finished: {s['runs']}", "",
           "## Runs by pipeline", "", "| Pipeline | Status counts |", "|---|---|"]
    out += [f"| {p} | {', '.join(f'{k} {v}' for k, v in sorted(c.items()))} |" for p, c in sorted(s["status"].items())]
    for title, counter in (("Failed blocking checks", s["blocking"]), ("Failed warnings", s["warns"])):
        out += ["", f"## {title}", ""]
        if not counter:
            out.append("None.")
            continue
        out += ["| Pipeline | Check | Count | Examples |", "|---|---|---|---|"]
        out += [f"| {p} | {c} | {n} | {'; '.join(s['examples'][(p, c)])} |" for (p, c), n in counter.most_common()]
    out += ["", "## Retryable failures by class", ""]
    out += [f"- {k}: {v}" for k, v in s["retryable"].most_common()] or ["None."]
    out += ["", "Late assets: see Atlas `/ui/assets` (freshness is evaluated by the control plane)."]
    return "\n".join(out) + "\n"


def write(lake_root: str, day: str) -> Path:
    path = Path(lake_root) / "reports" / "dq" / f"{day}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown(collect(lake_root, day)), encoding="utf-8")
    return path
