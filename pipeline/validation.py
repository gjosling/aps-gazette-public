#!/usr/bin/env python3
"""
validation.py — build-time validation suite for the APS Gazette dataset.

A small check-runner (no framework). Each check is a function returning a list of
Finding(severity, label, detail) where severity is FAIL or WARN. FAIL blocks
publication (the caller exits 1); WARN prints a GitHub Actions ::warning:: and the
build continues.

Entry points
------------
  validate_release(df, expectations) -> list[Finding]
      Release-file checks (1a–1i, 2, 5, 6). Called from 06_build_release.py with
      the post-dedup DataFrame before writing output, and again from the end of
      07_clean_text.py (where description_clean exists, so check 6 can run).
      Callers exit 1 if any returned finding is FAIL.

  CLI  python pipeline/validation.py --raw
      Parse-log / raw-parquet reconciliation (check 3). Exits 1 on FAIL.

  CLI  python pipeline/validation.py --print-current
      Loads the release parquet and prints the observed value for every
      expectations key, as copy-pasteable JSON. This is how bounds get refreshed.

Stale-lower-bound policy (maintainer decision, recorded verbatim)
-----------------------------------------------------------------
Lower bounds derive from the committed expectations file, are updated
deliberately by a human, and are never updated by pipeline code. The
--print-current CLI exists so refreshing them is one command plus a reviewed
diff. Rationale: the review found the previous bounds (>= 50,000 rows, eSafety
>= 196) go stale as the dataset grows, which quietly widens the gap a regression
could hide in; a committed file makes every widening or tightening a visible
commit.
"""

import argparse
import importlib.util
import json
import sys
from collections import namedtuple
from pathlib import Path

import pandas as pd

# ── Paths ───────────────────────────────────────────────────────────────────────

_PIPELINE_DIR   = Path(__file__).resolve().parent
_REPO_ROOT      = _PIPELINE_DIR.parent

EXPECTATIONS    = _REPO_ROOT / "data" / "expectations.json"
LINEAGE_CSV     = _REPO_ROOT / "data" / "agency_lineage.csv"
RELEASE_PARQUET = _REPO_ROOT / "data" / "release" / "aps_gazette_vacancies.parquet"
RAW_PARQUET     = _REPO_ROOT / "data" / "gazette_vacancies_raw.parquet"
PARSE_LOG       = _REPO_ROOT / "data" / "parse_log.csv"

# ── Load ALLOWED_DIVISION_MISMATCH + CANONICAL_AGENCIES from 04_build_crosswalk.py.
# The numeric-prefixed filename is not importable normally; load via importlib,
# the same pattern 05_apply_crosswalk.py uses.
_spec = importlib.util.spec_from_file_location(
    "build_crosswalk_for_validation",
    _PIPELINE_DIR / "04_build_crosswalk.py",
)
bac = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bac)

# Prefix pool for check 2: every canonical agency name, MINUS the collapsed
# names that forward-map/remap to their current-name canonical in
# 04_build_crosswalk.py (CANONICAL_FORWARD_MAP) and 05_apply_crosswalk.py
# (CANONICAL_REMAP). These older names legitimately appear in division strings
# on correctly-collapsed rows and must not flag as mismatches. Keep this set in
# sync with those two maps.
#   - DISER  → DISR
#   - DITRDC → DITRDCSA   (via DITRDCA)
#   - DITRDCA → DITRDCSA  (Infrastructure renamed to add Sport, 2025-05-13)
_STALE_PREMOG_NAMES = {
    "Department of Industry, Science, Energy and Resources",
    "Department of Infrastructure, Transport, Regional Development and Communications",
    "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
}
_DIVISION_PREFIX_POOL = sorted(
    (c for c in bac.CANONICAL_AGENCIES if c not in _STALE_PREMOG_NAMES),
    key=len,
    reverse=True,
)

# ── Finding + reporting ──────────────────────────────────────────────────────────

FAIL = "FAIL"
WARN = "WARN"

Finding = namedtuple("Finding", ["severity", "label", "detail"])


def _print_finding(f: Finding) -> None:
    """Print a finding; emit a GitHub Actions ::warning:: annotation for WARNs."""
    if f.detail and "\n" in f.detail:
        print(f"  {f.severity}  {f.label}")
        for line in f.detail.splitlines():
            print(f"        {line}")
    else:
        suffix = f"  [{f.detail}]" if f.detail else ""
        print(f"  {f.severity}  {f.label}{suffix}")
    if f.severity == WARN:
        msg = f"{f.label}: {f.detail}" if f.detail else f.label
        print(f"::warning::{msg}")


def _report(label: str, findings: list, sink: list) -> None:
    """Print OK for a clean check, else print each finding; collect into sink."""
    if not findings:
        print(f"  OK    {label}")
    else:
        for f in findings:
            _print_finding(f)
    sink.extend(findings)


def _summarise(findings: list) -> None:
    n_fail = sum(1 for f in findings if f.severity == FAIL)
    n_warn = sum(1 for f in findings if f.severity == WARN)
    print()
    if n_fail:
        print(f"VALIDATION: {n_fail} FAIL, {n_warn} WARN")
    elif n_warn:
        print(f"VALIDATION: all checks passed ({n_warn} WARN)")
    else:
        print("VALIDATION: all checks passed")


def has_fail(findings: list) -> bool:
    return any(f.severity == FAIL for f in findings)


# ── Expectations loading ─────────────────────────────────────────────────────────

def load_expectations(path=EXPECTATIONS) -> dict:
    """Load and return the committed expectations file.

    FAILs loudly (exit 1) if the file is missing or unparsable — never falls back
    to defaults (a silent default would defeat the point of a committed bound).
    """
    path = Path(path)
    if not path.exists():
        print(f"FATAL: expectations file not found: {path}", file=sys.stderr)
        sys.exit(1)
    try:
        with open(path) as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as e:
        print(f"FATAL: could not parse expectations file {path}: {e}", file=sys.stderr)
        sys.exit(1)


# ── Release checks (1a–1i, 2, 5, 6) ──────────────────────────────────────────────

def check_release_rows(df, exp) -> list:            # 1a
    bound = exp["min_release_rows"]
    if len(df) >= bound:
        return []
    return [Finding(FAIL, "release rows", f"got {len(df):,}, expected >= {bound:,}")]


def check_agency_rows(df, exp) -> list:             # 1b
    findings = []
    for agency, bound in exp["min_agency_rows"].items():
        n = int((df["agency_canonical"] == agency).sum())
        if n < bound:
            findings.append(
                Finding(FAIL, f"agency rows: {agency}", f"got {n}, expected >= {bound}")
            )
    return findings


def check_agency_canonical_nulls(df, exp) -> list:  # 1c (WARN)
    n = int(df["agency_canonical"].isna().sum())
    bound = exp["max_agency_canonical_nulls"]
    if n <= bound:
        return []
    return [Finding(WARN, "agency_canonical nulls", f"{n} nulls, expected <= {bound}")]


def check_gazette_type(df, exp) -> list:            # 1d
    allowed = {"combined", "vacancy_only", "daily"}
    vals = set(df["gazette_type"].dropna().unique())
    unexpected = vals - allowed
    if not unexpected:
        return []
    return [Finding(FAIL, "gazette_type values", f"unexpected: {sorted(unexpected)}")]


def check_salary(df, exp) -> list:                  # 1e / check 4
    findings = []
    inverted = (
        df["salary_min"].notna()
        & df["salary_max"].notna()
        & (df["salary_min"] > df["salary_max"])
    )
    n = int(inverted.sum())
    if n:
        findings.append(Finding(FAIL, "salary_min > salary_max", f"{n} rows"))
    for col in ["salary_min", "salary_max"]:
        n = int((df[col] > 500_000).sum())
        if n:
            findings.append(Finding(FAIL, f"{col} > 500k", f"{n} rows"))
    return findings


def check_location_normalised(df, exp) -> list:     # 1f
    mismatch = df["location"].notna() & df["location_normalised"].isna()
    n = int(mismatch.sum())
    if n == 0:
        return []
    return [Finding(FAIL, "location_normalised gaps", f"{n} rows where location non-null")]


def check_division_artifacts(df, exp) -> list:      # 1g
    findings = []
    n = int(df["division"].str.contains(r"\d{4}\.pdf", na=False, regex=True).sum())
    if n:
        findings.append(Finding(FAIL, "YYYY.pdf in division", f"{n} rows"))
    n = int(df["division"].str.contains(r"Portfolio\)", na=False, regex=False).sum())
    if n:
        findings.append(Finding(FAIL, "Portfolio) in division", f"{n} rows"))
    return findings


def check_field_bleed(df, exp) -> list:             # 1h
    findings = []
    bleed_re = r"\S  [A-Z][a-z]"
    for col in ["agency", "division", "branch", "job_title", "classification", "position_number"]:
        if col not in df.columns:
            continue
        n = int(df[col].dropna().str.contains(bleed_re, regex=True).sum())
        if n:
            findings.append(Finding(FAIL, f"bleed pattern in {col}", f"{n} rows"))
    return findings


def check_empty_strings(df, exp) -> list:           # 1i
    findings = []
    str_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    for col in str_cols:
        n = int((df[col] == "").sum())
        if n:
            findings.append(Finding(FAIL, f"empty strings in {col}", f"{n} rows"))
    return findings


def _division_canonical(div: str):
    """Longest-prefix match of a raw division string against the pool (plain
    str.startswith — canonical names may contain regex metacharacters)."""
    for canon in _DIVISION_PREFIX_POOL:
        if div.startswith(canon):
            return canon
    return None


def check_division_mismatch(df, exp) -> list:       # 2
    """AIFS-class regression guard: every (agency_canonical, division_canonical)
    mismatch must be allowlisted in ALLOWED_DIVISION_MISMATCH."""
    allow = bac.ALLOWED_DIVISION_MISMATCH
    # offending[(ac, dc)] = [row_count, [example vacancy_nos]]
    offending: dict = {}
    div = df["division"]
    for pos in range(len(df)):
        d = div.iat[pos]
        if not isinstance(d, str) or not d.strip():
            continue                                 # null/empty division: skip row
        dc = _division_canonical(d)
        if dc is None:
            continue
        ac = df["agency_canonical"].iat[pos]
        if dc == ac:
            continue
        pair = (ac, dc)
        if pair in allow:
            continue
        entry = offending.setdefault(pair, [0, []])
        entry[0] += 1
        if len(entry[1]) < 3:
            entry[1].append(df["vacancy_no"].iat[pos])
    if not offending:
        return []
    lines = ["Unallowlisted division/agency_canonical mismatch(es):"]
    lines.append(f"  {'rows':>5}  {'examples':<40}  (agency_canonical, division_canonical)")
    total = 0
    for (ac, dc), (n, examples) in sorted(offending.items(), key=lambda x: -x[1][0]):
        total += n
        ex = ", ".join(str(e) for e in examples)
        lines.append(f"  {n:>5}  {ex:<40}  ({ac!r}, {dc!r})")
    detail = "\n".join(lines)
    return [Finding(FAIL, f"division-mismatch: {len(offending)} pair(s), {total} row(s)", detail)]


def check_lineage(df, exp) -> list:                 # 7
    """MoG lineage table referential integrity.

    data/agency_lineage.csv must parse; every predecessor_canonical and
    successor_canonical must be a canonical agency name; relation values must be
    a subset of {rename, merge, split}; effective_date must parse as ISO dates.
    """
    if not LINEAGE_CSV.exists():
        return [Finding(FAIL, "lineage table", f"{LINEAGE_CSV.name} not found")]
    try:
        lin = pd.read_csv(LINEAGE_CSV)
    except Exception as e:                           # pragma: no cover - defensive
        return [Finding(FAIL, "lineage table parse", str(e))]
    findings = []
    required_cols = {"predecessor_canonical", "successor_canonical",
                     "effective_date", "relation", "note"}
    missing = required_cols - set(lin.columns)
    if missing:
        return [Finding(FAIL, "lineage columns", f"missing: {sorted(missing)}")]
    canon = set(bac.CANONICAL_AGENCIES)
    for col in ("predecessor_canonical", "successor_canonical"):
        bad = sorted(set(lin[col].dropna()) - canon)
        if bad:
            findings.append(Finding(FAIL, f"lineage {col} not canonical", f"{bad}"))
    bad_rel = sorted(set(lin["relation"].dropna()) - {"rename", "merge", "split"})
    if bad_rel:
        findings.append(Finding(FAIL, "lineage relation values", f"unexpected: {bad_rel}"))
    parsed = pd.to_datetime(lin["effective_date"], format="%Y-%m-%d", errors="coerce")
    n_bad = int(parsed.isna().sum())
    if n_bad:
        findings.append(Finding(FAIL, "lineage effective_date parse",
                                f"{n_bad} value(s) not ISO YYYY-MM-DD"))
    return findings


def check_premog_dcceew(df, exp) -> list:           # 8
    """Antarctic/DEE regression guard: zero release rows attributed to DCCEEW
    with a pre-2022-07 gazette date.

    Environment/climate functions lived in DAWE before the July 2022 split, so
    DCCEEW must be from-zero at 2022-07 — no spurious pre-2022 trickle. The
    05_apply_crosswalk.py date remap enforces this; this check guards against a
    regression that would reintroduce the trickle.
    """
    DCCEEW = "Department of Climate Change, Energy, the Environment and Water"
    gd = pd.to_datetime(df["gazette_date"])
    n = int(((df["agency_canonical"] == DCCEEW) & (gd < pd.Timestamp("2022-07-01"))).sum())
    if n == 0:
        return []
    return [Finding(FAIL, "pre-2022-07 DCCEEW rows",
                    f"got {n}, expected 0 (pre-split environment functions belong to DAWE)")]


def check_am_groups(df, exp) -> list:               # 5 (activates after affirmative-measures linkage lands)
    """AM-group count bounds. Dormant while expectations.am_linkage is null.
    When non-null: FAIL if the affirmative-measures linkage columns are missing
    (build ordering violated — the linkage step in 06_build_release.py must run
    before this check has anything to bound); otherwise WARN if counts fall
    outside the configured bounds.

    AM-flag predicate: a row is AM-flagged iff is_affirmative_measure is True —
    i.e. job_title matches (?i)affirmative measure. min_am_rows bounds that count.
    (This deliberately counts AM-flagged rows, not non-null posting_group_id:
    posting_group_id counts linked rows — AM variants *and* their base postings —
    not AM-flagged rows.)
    """
    am = exp.get("am_linkage")
    if am is None:
        return []
    missing = [c for c in ("is_affirmative_measure", "posting_group_id") if c not in df.columns]
    if missing:
        return [Finding(FAIL, "am_linkage",
                        f"expectations.am_linkage is set but column(s) {missing} missing "
                        f"(affirmative-measures linkage step did not run — build ordering violated)")]
    findings = []
    grp = df["posting_group_id"]
    am_rows = int(df["is_affirmative_measure"].sum())
    n_groups = int(grp.dropna().nunique())
    min_am_rows = am.get("min_am_rows")
    min_groups = am.get("min_groups")
    max_groups = am.get("max_groups")
    if min_am_rows is not None and am_rows < min_am_rows:
        findings.append(Finding(WARN, "AM-flagged rows", f"got {am_rows}, expected >= {min_am_rows}"))
    if min_groups is not None and n_groups < min_groups:
        findings.append(Finding(WARN, "AM posting groups (low)", f"got {n_groups}, expected >= {min_groups}"))
    if max_groups is not None and n_groups > max_groups:
        findings.append(Finding(WARN, "AM posting groups (high)", f"got {n_groups}, expected <= {max_groups}"))
    return findings


def check_boilerplate_residual(df, exp) -> list:    # 6
    """Boilerplate-residual ceiling. Skipped (with an explicit line) when
    description_clean is absent — it does not exist in 06's DataFrame; the real
    run happens at the end of 07_clean_text.py."""
    cfg = exp["boilerplate_residual"]
    if "description_clean" not in df.columns:
        print("  SKIP  boilerplate residual: column description_clean absent")
        return []
    since = pd.Timestamp(cfg["since"])
    ceiling = cfg["max_pct_of_descriptions_clean"]
    phrase = cfg["phrase"]
    gd = pd.to_datetime(df["gazette_date"])
    pop = df[(gd >= since) & df["description_clean"].notna()]
    if len(pop) == 0:
        return []
    hits = int(pop["description_clean"].str.contains(phrase, case=False, regex=False).sum())
    pct = hits / len(pop) * 100
    # Handshake kept in data: FAIL when the ceiling is tightened below 1.0
    # (the v2 corpus-wide boilerplate pass drove residual to ~0, so a sub-1.0
    # ceiling is a deliberate regression gate), WARN otherwise.
    severity = FAIL if ceiling < 1.0 else WARN
    if pct <= ceiling:
        return []
    return [Finding(
        severity,
        "boilerplate residual",
        f"{pct:.1f}% of description_clean since {cfg['since']} contain "
        f"'{phrase}' ({hits}/{len(pop)}), ceiling {ceiling}%",
    )]


def validate_release(df, expectations) -> list:
    """Run every release check, print a report, and return all findings.

    The caller decides the exit code (sys.exit(1) if any finding is FAIL)."""
    print("=== RELEASE VALIDATION ===")
    findings: list = []
    _report("release rows", check_release_rows(df, expectations), findings)
    _report("per-agency rows", check_agency_rows(df, expectations), findings)
    _report("agency_canonical nulls", check_agency_canonical_nulls(df, expectations), findings)
    _report("gazette_type values", check_gazette_type(df, expectations), findings)
    _report("salary guards", check_salary(df, expectations), findings)
    _report("location_normalised coverage", check_location_normalised(df, expectations), findings)
    _report("division artifacts", check_division_artifacts(df, expectations), findings)
    _report("field-bleed pattern", check_field_bleed(df, expectations), findings)
    _report("empty strings", check_empty_strings(df, expectations), findings)
    _report("division-mismatch guard", check_division_mismatch(df, expectations), findings)
    _report("lineage table integrity", check_lineage(df, expectations), findings)
    _report("pre-2022-07 DCCEEW guard", check_premog_dcceew(df, expectations), findings)
    _report("AM-group bounds", check_am_groups(df, expectations), findings)
    # check 6 prints its own SKIP line when description_clean is absent
    bp = check_boilerplate_residual(df, expectations)
    if "description_clean" in df.columns:
        _report("boilerplate residual", bp, findings)
    else:
        findings.extend(bp)   # empty; SKIP already printed
    _summarise(findings)
    return findings


# ── Raw checks (3) ───────────────────────────────────────────────────────────────

_RAW_JOIN_COLS = ["gazette_id", "gazette_date", "gazette_type"]


def validate_raw(raw_path=RAW_PARQUET, log_path=PARSE_LOG) -> list:
    """Reconcile the parse log against the accumulated raw parquet (check 3).

    Returns [] on first-run (no log or parquet yet)."""
    print("=== RAW PARSE VALIDATION ===")
    raw_path, log_path = Path(raw_path), Path(log_path)
    if not raw_path.exists() or not log_path.exists():
        print("  nothing to check (raw parquet or parse log not present yet)")
        return []

    raw = pd.read_parquet(raw_path, columns=_RAW_JOIN_COLS)
    raw["gazette_date"] = pd.to_datetime(raw["gazette_date"]).dt.date
    log = pd.read_csv(log_path)
    log["gazette_date"] = pd.to_datetime(log["gazette_date"]).dt.date

    findings: list = []
    parsed = log[log["status"] == "parsed"]

    # (a) total: sum(record_count where parsed) == len(raw parquet)
    total_log = int(parsed["record_count"].sum())
    total_raw = len(raw)
    if total_log == total_raw:
        _report(f"parsed record_count sum == raw rows ({total_raw:,})", [], findings)
    else:
        _report(
            "parsed record_count sum vs raw rows",
            [Finding(FAIL, "record_count sum != raw rows",
                     f"log sum {total_log:,} != raw {total_raw:,}")],
            findings,
        )

    # (b) per-group reconciliation, both directions.
    raw_grp = raw.groupby(_RAW_JOIN_COLS, dropna=False).size().rename("raw_count").reset_index()
    log_grp = (parsed.groupby(_RAW_JOIN_COLS, dropna=False)["record_count"]
               .sum().rename("log_count").reset_index())
    merged = raw_grp.merge(log_grp, on=_RAW_JOIN_COLS, how="outer", indicator=True)

    group_findings = []

    # raw group with no parsed log row
    left_only = merged[merged["_merge"] == "left_only"]
    if len(left_only):
        ex = "; ".join(
            f"{r.gazette_id}/{r.gazette_date}/{r.gazette_type} ({int(r.raw_count)} rows)"
            for r in left_only.head(5).itertuples()
        )
        group_findings.append(
            Finding(FAIL, "raw group with no parsed log row", f"{len(left_only)} group(s): {ex}")
        )

    # parsed log row with record_count > 0 but no raw group
    right_only = merged[merged["_merge"] == "right_only"]
    ro_nonzero = right_only[right_only["log_count"] > 0]
    if len(ro_nonzero):
        ex = "; ".join(
            f"{r.gazette_id}/{r.gazette_date}/{r.gazette_type} ({int(r.log_count)} expected)"
            for r in ro_nonzero.head(5).itertuples()
        )
        group_findings.append(
            Finding(FAIL, "parsed log row (record_count>0) with no raw group",
                    f"{len(ro_nonzero)} row(s): {ex}")
        )

    # count mismatch where both present
    both = merged[merged["_merge"] == "both"]
    mismatch = both[both["raw_count"] != both["log_count"]]
    if len(mismatch):
        ex = "; ".join(
            f"{r.gazette_id}/{r.gazette_date}/{r.gazette_type} "
            f"(raw {int(r.raw_count)} != log {int(r.log_count)})"
            for r in mismatch.head(5).itertuples()
        )
        group_findings.append(
            Finding(FAIL, "raw-group count != parse-log record_count",
                    f"{len(mismatch)} group(s): {ex}")
        )

    # Zero-record PDFs are the expected asymmetry: parsed log rows with
    # record_count == 0 correctly have no raw group.
    n_zero = int((right_only["log_count"] == 0).sum())
    _report(
        f"per-group reconciliation (both directions; {n_zero} zero-record PDF(s) expected)",
        group_findings,
        findings,
    )

    # WARN on any log rows with status missing_pdf / error.
    bad = log[log["status"].isin(["missing_pdf", "error"])]
    if len(bad):
        by_status = bad["status"].value_counts().to_dict()
        _report(
            "parse-log status",
            [Finding(WARN, "log rows with missing_pdf/error status",
                     f"{len(bad)} row(s): {by_status}")],
            findings,
        )
    else:
        _report("no missing_pdf/error log rows", [], findings)

    _summarise(findings)
    return findings


# ── --print-current ──────────────────────────────────────────────────────────────

def print_current(expectations, release_path=RELEASE_PARQUET) -> None:
    """Print the observed value for every expectations key as copy-pasteable JSON."""
    release_path = Path(release_path)
    if not release_path.exists():
        print(f"FATAL: release parquet not found: {release_path}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_parquet(release_path)

    observed = {
        "min_release_rows": len(df),
        "min_agency_rows": {
            agency: int((df["agency_canonical"] == agency).sum())
            for agency in expectations["min_agency_rows"]
        },
        "max_agency_canonical_nulls": int(df["agency_canonical"].isna().sum()),
    }

    cfg = expectations["boilerplate_residual"]
    boiler = {"phrase": cfg["phrase"], "since": cfg["since"]}
    if "description_clean" in df.columns:
        since = pd.Timestamp(cfg["since"])
        gd = pd.to_datetime(df["gazette_date"])
        pop = df[(gd >= since) & df["description_clean"].notna()]
        if len(pop):
            hits = int(pop["description_clean"].str.contains(cfg["phrase"], case=False, regex=False).sum())
            boiler["max_pct_of_descriptions_clean"] = round(hits / len(pop) * 100, 1)
        else:
            boiler["max_pct_of_descriptions_clean"] = None
    else:
        boiler["max_pct_of_descriptions_clean"] = None
    observed["boilerplate_residual"] = boiler

    if "posting_group_id" in df.columns and "is_affirmative_measure" in df.columns:
        grp = df["posting_group_id"]
        n_groups = int(grp.dropna().nunique())
        observed["am_linkage"] = {
            "min_am_rows": int(df["is_affirmative_measure"].sum()),
            "min_groups": n_groups,
            "max_groups": n_groups,
        }
    else:
        observed["am_linkage"] = None

    print(json.dumps(observed, indent=2))


# ── CLI ──────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="APS Gazette build-time validation suite.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--raw", action="store_true",
                       help="reconcile the parse log against the raw parquet (check 3)")
    group.add_argument("--print-current", action="store_true",
                       help="print observed values for every expectations key as JSON")
    args = parser.parse_args()

    if args.raw:
        findings = validate_raw()
        sys.exit(1 if has_fail(findings) else 0)

    if args.print_current:
        print_current(load_expectations())
        sys.exit(0)


if __name__ == "__main__":
    main()
