"""
Build public release files from gazette_vacancies_crosswalk.parquet.

Applies column renames and gazette_type value renames for external clarity,
deduplicates daily notices against weekly versions, runs validation checks,
ensures duties_text follows description, then writes:
  data/release/aps_gazette_vacancies.parquet
  data/release/aps_gazette_vacancies.csv.gz
"""

import os
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

    # Move agency_canonical and agency_group to immediately after branch
    if all(c in df.columns for c in ('agency_canonical', 'agency_group', 'branch')):
        cols = [c for c in df.columns if c not in ('agency_canonical', 'agency_group')]
        branch_idx = cols.index('branch')
        cols = cols[:branch_idx + 1] + ['agency_canonical', 'agency_group'] + cols[branch_idx + 1:]
        df = df[cols]
        print("agency_canonical/agency_group: moved to after branch")

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
