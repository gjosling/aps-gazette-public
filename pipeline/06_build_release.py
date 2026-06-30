"""
Build public release files from gazette_vacancies_crosswalk.parquet.

Applies column renames and gazette_type value renames for external clarity,
deduplicates daily notices against weekly versions, runs validation checks,
ensures duties_text follows description, then writes:
  data/release/aps_gazette_vacancies.parquet
  data/release/aps_gazette_vacancies.csv.gz
"""

import os
import re
import pandas as pd

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

    ERRORS = []

    def ok(label):
        print(f"  OK    {label}")

    def fail(label, detail=''):
        msg = f"FAIL  {label}" + (f"  [{detail}]" if detail else '')
        ERRORS.append(msg)
        print(f"  {msg}")

    print("\n=== VALIDATION ===")

    # Row count — sanity lower bound only (dedup reduces count variably)
    # Lower bound based on dataset as of Jan 2026; bump as the dataset grows.
    if len(df) >= 50_000:
        ok(f"row count = {len(df):,}")
    else:
        fail("row count", f"got {len(df):,}, expected >= 50,000")

    # agency_canonical nulls — crosswalk gaps are expected occasionally; reported as
    # warnings (not failures) by 09_check_coverage.py at the end of the pipeline.
    n = df['agency_canonical'].isna().sum()
    ok(f"agency_canonical nulls = {n}")

    # eSafety row count — lower bound (grows as new notices are ingested)
    # Lower bound based on dataset as of Jan 2026; bump as the dataset grows.
    n = (df['agency_canonical'] == 'Office of the eSafety Commissioner').sum()
    if n >= 196:
        ok(f"eSafety rows = {n}")
    else:
        fail("eSafety rows", f"got {n}, expected >= 196")

    # gazette_type values — allow 'daily' in addition to renamed weekly types
    ALLOWED_TYPES = {'combined', 'vacancy_only', 'daily'}
    gt_vals = set(df['gazette_type'].dropna().unique())
    unexpected = gt_vals - ALLOWED_TYPES
    if not unexpected:
        ok(f"gazette_type values = {sorted(gt_vals)}")
    else:
        fail("gazette_type values", f"unexpected: {unexpected}")

    # No salary_min > salary_max
    inverted = df['salary_min'].notna() & df['salary_max'].notna() & (df['salary_min'] > df['salary_max'])
    n = inverted.sum()
    if n == 0:
        ok("no salary_min > salary_max")
    else:
        fail("salary_min > salary_max", f"{n} rows")

    # No salary > 500k
    for col in ['salary_min', 'salary_max']:
        n = (df[col] > 500_000).sum()
        if n == 0:
            ok(f"no {col} > 500k")
        else:
            fail(f"{col} > 500k", f"{n} rows")

    # location_normalised populated where location is non-null
    mismatch = df['location'].notna() & df['location_normalised'].isna()
    n = mismatch.sum()
    if n == 0:
        ok("location_normalised populated where location is non-null")
    else:
        fail("location_normalised gaps", f"{n} rows")

    # No YYYY.pdf in division
    n = df['division'].str.contains(r'\d{4}\.pdf', na=False, regex=True).sum()
    if n == 0:
        ok("no YYYY.pdf in division")
    else:
        fail("YYYY.pdf in division", f"{n} rows")

    # No Portfolio) in division
    n = df['division'].str.contains(r'Portfolio\)', na=False, regex=False).sum()
    if n == 0:
        ok("no Portfolio) in division")
    else:
        fail("Portfolio) in division", f"{n} rows")

    # No field-bleed double-space pattern in structured fields
    bleed_re = r'\S  [A-Z][a-z]'
    for col in ['agency', 'division', 'branch', 'job_title', 'classification', 'position_number']:
        if col not in df.columns:
            continue
        n = df[col].dropna().str.contains(bleed_re, regex=True).sum()
        if n == 0:
            ok(f"no bleed pattern in {col}")
        else:
            fail(f"bleed pattern in {col}", f"{n} rows")

    # No empty strings in any string column
    str_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    total_empty = sum((df[col] == '').sum() for col in str_cols)
    if total_empty == 0:
        ok(f"no empty strings across {len(str_cols)} string columns")
    else:
        for col in str_cols:
            n = (df[col] == '').sum()
            if n:
                fail(f"empty strings in {col}", f"{n} rows")

    # Summary
    print()
    if ERRORS:
        print(f"VALIDATION: {len(ERRORS)} failure(s)")
    else:
        print("VALIDATION: all checks passed")

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

    df.to_parquet(PARQUET_OUT, index=False)
    df.to_csv(CSV_OUT, index=False, compression='gzip')

    p_mb = os.path.getsize(PARQUET_OUT) / 1e6
    c_mb = os.path.getsize(CSV_OUT) / 1e6

    print(f"\n=== OUTPUT ===")
    print(f"  {PARQUET_OUT}  ({p_mb:.1f} MB)")
    print(f"  {CSV_OUT}  ({c_mb:.1f} MB)")
    print(f"\nDone. {len(df):,} rows, {len(df.columns)} columns.")


if __name__ == '__main__':
    run()
