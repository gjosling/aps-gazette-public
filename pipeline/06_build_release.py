"""
Build public release files from gazette_vacancies_crosswalk.parquet.

Applies column renames and gazette_type value renames for external clarity,
deduplicates daily notices against weekly versions, runs validation checks,
ensures duties_text follows description, then writes:
  data/release/aps_gazette_vacancies.parquet
  data/release/aps_gazette_vacancies.csv.gz
"""

import datetime
import hashlib
import os
import re
import sys
from pathlib import Path

import pandas as pd

# validation.py is a legal module name; make it importable by adding the
# pipeline/ dir to sys.path (the numerically-prefixed pipeline scripts can't be).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import validation
import release_io

# ── Load ──────────────────────────────────────────────────────────────────────

SRC = 'data/gazette_vacancies_crosswalk.parquet'

# ── Affirmative-measures linkage ──────────────────────────────────────────────
#
# APS agencies gazette the same role 2–3 times under Affirmative Measures
# variants (Disability / Aboriginal and Torres Strait Islander) with distinct
# vacancy_nos. is_affirmative_measure flags the AM-titled rows; posting_group_id
# links AM variants of one posting to each other and their base posting, so that
# role-level counts can collapse the AM duplicate excess. Both columns are pure
# functions of existing columns, recomputed on every rebuild (no state file).
#
# Linkage is hash-only: position_number is NOT used — measured this session, it
# under-links (~85% of true AM groups mint a separate requisition number per
# variant) and over-merges (58% of reused position numbers span distinct roles).
# The group key is (agency_canonical, AM-stripped normalised title,
# classification_code as printed, closing_date). The classification component
# trades rare over-merges for ~1% under-merges — under-merging is the accepted
# direction. See docs/data_dictionary.md for the counting guidance.

AM_TITLE_RE = re.compile(r'(?i)affirmative\s+measure')


def strip_am(title: str) -> str:
    t = title
    # parenthesised variants: "(Affirmative Measures - Disability)", "(Affirmative Measure)", ...
    t = re.sub(r'\(\s*[^()]*affirmative\s+measures?[^()]*\)', ' ', t, flags=re.I)
    # dash/comma-attached variants incl. target group:
    # "- Affirmative Measures Indigenous", "– Affirmative Measure-Disability",
    # ", Affirmative Measures - Aboriginal and Torres Strait Islander"
    t = re.sub(r'[-–—,:]?\s*affirmative\s+measures?\b[\s–—:-]*'
               r'(?:disability|indigenous|first\s+nations?|'
               r'aboriginal(?:\s+and\s+torres\s+strait\s+islander)?|'
               r'torres\s+strait\s+islander)?s?\b',
               ' ', t, flags=re.I)
    t = re.sub(r'affirmative\s+measures?', ' ', t, flags=re.I)   # residual
    return t


def norm_title(title) -> str:
    if not isinstance(title, str):
        return ""
    t = strip_am(title).lower()
    t = re.sub(r'[^\w\s]', ' ', t)          # kills trailing "- " separators, dashes, parens
    return re.sub(r'\s+', ' ', t).strip()


def _str_or_empty(v) -> str:
    """agency_canonical / classification_code coerced to str; nulls (None, pd.NA)
    → "". Avoids `pd.NA or ""`, whose truthiness raises."""
    return v if isinstance(v, str) else ""


def _iso_or_empty(v) -> str:
    """closing_date (datetime.date) → ISO string; nulls (None/NaT/NaN) → ""."""
    return v.isoformat() if isinstance(v, datetime.date) else ""


def add_am_linkage(df: pd.DataFrame) -> pd.DataFrame:
    """Add is_affirmative_measure and posting_group_id, placed immediately after
    job_title. posting_group_id is populated only for rows in a key-group that
    contains >= 1 AM-flagged row (incl. a singleton AM row); null everywhere else."""
    is_am = df["job_title"].apply(
        lambda t: bool(AM_TITLE_RE.search(t)) if isinstance(t, str) else False
    )

    key = (
        df["agency_canonical"].map(_str_or_empty) + "\x1f"
        + df["job_title"].map(norm_title) + "\x1f"
        + df["classification_code"].map(_str_or_empty) + "\x1f"
        + df["closing_date"].map(_iso_or_empty)
    )
    am_keys = set(key[is_am])
    group_id = key.where(key.isin(am_keys)).map(
        lambda k: hashlib.sha1(k.encode()).hexdigest()[:12] if isinstance(k, str) else None
    )

    df = df.copy()
    df["is_affirmative_measure"] = is_am.to_numpy(dtype=bool)
    df["posting_group_id"] = group_id.to_numpy()

    # Placement: both columns immediately after job_title, in this order.
    cols = [c for c in df.columns if c not in ("is_affirmative_measure", "posting_group_id")]
    jt = cols.index("job_title")
    cols = cols[:jt + 1] + ["is_affirmative_measure", "posting_group_id"] + cols[jt + 1:]
    df = df[cols]

    n_am = int(is_am.sum())
    n_linked = int(df["posting_group_id"].notna().sum())
    n_groups = int(df["posting_group_id"].dropna().nunique())
    print(f"AM linkage: {n_am:,} AM rows; {n_linked:,} rows linked into "
          f"{n_groups:,} posting groups")
    return df


def run():
    df = pd.read_parquet(SRC)
    print(f"Loaded {SRC}: {len(df):,} rows, {len(df.columns)} cols")

    # Drop internal QA columns if present
    for col in ['duties_cat', 'raw_vn_count']:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
            print(f"{col}: dropped")

    # Ensure duties_text sits immediately after description
    if 'duties_text' in df.columns and 'description' in df.columns:
        cols = [c for c in df.columns if c != 'duties_text']
        cols.insert(cols.index('description') + 1, 'duties_text')
        df = df[cols]
        n_dt = df['duties_text'].notna().sum()
        print(f"duties_text: {n_dt:,} non-null ({n_dt / len(df) * 100:.1f}%)")

    # Move agency_canonical, agency_group and ps_act_employer to immediately after
    # branch (ps_act_employer sits immediately after agency_group).
    _agency_block = [c for c in ('agency_canonical', 'agency_group', 'ps_act_employer')
                     if c in df.columns]
    if 'branch' in df.columns and _agency_block:
        cols = [c for c in df.columns if c not in _agency_block]
        branch_idx = cols.index('branch')
        cols = cols[:branch_idx + 1] + _agency_block + cols[branch_idx + 1:]
        df = df[cols]
        print(f"{'/'.join(_agency_block)}: moved to after branch")

    # ── Column renames ────────────────────────────────────────────────────────────

    df.rename(columns={
        'classification_clean': 'classification_code',
    }, inplace=True)
    print("Renamed: classification_clean → classification_code")

    # ── gazette_type value renames ─────────────────────────────────────────────────

    df['gazette_type'] = df['gazette_type'].replace({
        'weekly_old':     'combined',
        'weekly_vacancy': 'vacancy_only',
    })
    print(f"gazette_type: {df['gazette_type'].value_counts().to_dict()}")

    # ── Filter: exclude test notices ──────────────────────────────────────────────
    # "Department of Testing" rows are test notices that appear in the gazette data
    # and should not be included in the release.
    test_mask = df['agency'] == 'Department of Testing'
    n_test = test_mask.sum()
    if n_test:
        df = df[~test_mask].copy()
        print(f"Filtered {n_test} 'Department of Testing' test notices")

    # ── Dedup: drop daily notices where a weekly version exists ───────────────────
    #
    # Daily gazettes repost the same vacancy notices as weekly gazettes but the
    # PDF layout produces worse field separation (division/branch concatenated,
    # placeholder position numbers). When a vacancy_no appears in both a daily
    # and a weekly gazette (vacancy_only or combined), keep the weekly version.
    # Vacancy_no values that appear only in daily gazettes are retained as-is.

    WEEKLY_TYPES = {'vacancy_only', 'combined'}
    n_before_dedup = len(df)

    is_weekly = df['gazette_type'].isin(WEEKLY_TYPES)
    weekly_vns = set(df.loc[is_weekly, 'vacancy_no'].dropna())

    # Drop daily rows that have a weekly counterpart
    keep = is_weekly | ~df['vacancy_no'].isin(weekly_vns)
    df = df[keep].copy()

    # Deduplicate within weekly: keep earliest per vacancy_no (handles 3+ appearances).
    # subset=['vacancy_no'] is intentional — PS52/PS2 year-boundary re-publications are
    # genuinely the same notice and should be collapsed to one row, not kept as two.
    weekly_mask = df['gazette_type'].isin(WEEKLY_TYPES)
    df_weekly = df[weekly_mask].sort_values('gazette_date').drop_duplicates(subset=['vacancy_no'], keep='first')
    df_daily = df[~weekly_mask]
    df = pd.concat([df_weekly, df_daily], ignore_index=True)

    n_dropped = n_before_dedup - len(df)
    print(f"Dedup: dropped {n_dropped:,} daily duplicates, {len(df):,} rows remain")

    # ── gazette_date: strip time component ───────────────────────────────────────

    df['gazette_date'] = pd.to_datetime(df['gazette_date']).dt.date
    print("gazette_date: converted to date")

    # ── Affirmative-measures linkage ──────────────────────────────────────────────

    df = add_am_linkage(df)

    # ── Validation ────────────────────────────────────────────────────────────────
    #
    # Checks live in pipeline/validation.py and their bounds in the committed
    # data/expectations.json. A FAIL blocks publication: we exit 1 here so CI
    # stops before 07/08/push. Check 6 (boilerplate residual) is skipped here —
    # description_clean does not exist yet — and runs at the end of 07_clean_text.py.

    print()
    expectations = validation.load_expectations()
    findings = validation.validate_release(df, expectations)
    if validation.has_fail(findings):
        print("\nRelease BLOCKED: validation FAILed. Not publishing; CI stops before 07/08/push.")
        sys.exit(1)

    # ── Column summary ────────────────────────────────────────────────────────────

    print(f"\n=== COLUMNS ({len(df.columns)}) ===")
    for col in df.columns:
        n_null = df[col].isna().sum()
        pct = n_null / len(df) * 100
        tag = f"  {n_null:,} null ({pct:.1f}%)" if n_null else ""
        print(f"  {col}{tag}")

    # ── Write output ───────────────────────────────────────────────────────────────

    os.makedirs('data/release', exist_ok=True)

    PARQUET_OUT = 'data/release/aps_gazette_vacancies.parquet'
    CSV_OUT     = 'data/release/aps_gazette_vacancies.csv.gz'

    release_io.write_release(df, "06_build_release")

    p_mb = os.path.getsize(PARQUET_OUT) / 1e6
    c_mb = os.path.getsize(CSV_OUT) / 1e6

    print(f"\n=== OUTPUT ===")
    print(f"  {PARQUET_OUT}  ({p_mb:.1f} MB)")
    print(f"  {CSV_OUT}  ({c_mb:.1f} MB)")
    print(f"\nDone. {len(df):,} rows, {len(df.columns)} columns.")


if __name__ == '__main__':
    run()
