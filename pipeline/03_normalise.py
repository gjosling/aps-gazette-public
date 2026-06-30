#!/usr/bin/env python3
"""
03_normalise.py — Derive structured columns from raw gazette parse output.

Reads data/gazette_vacancies_raw.parquet (output of 02_parse.py) and adds:

  salary_min, salary_max    parsed from the raw salary string
  closing_date              parsed from closing_date_raw to ISO YYYY-MM-DD
  location_normalised       comma-parts sorted alphabetically
  classification_clean      long-form grade names contracted to codes
                            (APS Level 6 → APS6, Executive Level 1 → EL1, …)
  duties_text               job responsibilities section extracted from description,
                            bounded by Duties/The Role … Eligibility markers

Also applies salary guards 1–5 and closing-date guard 6.
Ported from field normalisers and guards in 02_gazette_parse.R.

Input:  data/gazette_vacancies_raw.parquet
Output: data/gazette_vacancies_normalised.parquet

Usage:
    python pipeline/03_normalise.py
    python pipeline/03_normalise.py --dry-run
"""

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

RAW_PATH = Path("data/gazette_vacancies_raw.parquet")
OUT_PATH = Path("data/gazette_vacancies_normalised.parquet")

def _parse_salary_min(s) -> float | None:
    """First $X amount in salary string; $1 is skipped by the regex — see Guard 5."""
    if not s or pd.isna(s):
        return None
    m = re.search(r'\$(\d[\d,]+)', str(s))
    if not m:
        return None
    try:
        return float(m.group(1).replace(',', ''))
    except ValueError:
        return None


def _parse_salary_max(s) -> float | None:
    """Second $X in a min–max range; None for single-figure salary strings."""
    if not s or pd.isna(s):
        return None
    m = re.search(r'\$(\d[\d,]+)\s*[-–]\s*\$(\d[\d,]+)', str(s))
    if not m:
        return None
    try:
        return float(m.group(2).replace(',', ''))
    except ValueError:
        return None


def _parse_closing_date(raw) -> date | None:
    """Extract 'D Month YYYY' from a free-text closing date field.

    Handles varied formats: "Friday 23 May 2025", "23 May 2025 11:59pm AEST".
    """
    if not raw or pd.isna(raw):
        return None
    m = re.search(r'(\d{1,2}) (\w+) (\d{4})', str(raw))
    if not m:
        return None
    try:
        d = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y")
        return d.date()
    except ValueError:
        return None


def _normalise_location(s) -> str | None:
    """Sort comma-delimited location parts alphabetically; null in → null out."""
    if not isinstance(s, str):
        return None
    parts = [p.strip() for p in s.split(',')]
    return ', '.join(sorted(parts))


# Gazette descriptions are space-joined (newlines collapsed by the parser).
# Section markers appear as capitalised standalone words in that flow.
# Survey of 500 descriptions (across 2021–2026):
#   97.8% have "Duties" as the duties-section marker
#   16.9% of those have "Duties The Role" as a combined double-header (Defence mostly)
#   99.6% have "Eligibility" as the end-of-duties boundary
#
# "The Role" fallback requires it is NOT preceded by a word character + space
# (lookbehind excludes mid-sentence matches like "About The Role" or "months The Role").
# "About [Tt]he [Rr]ole" is listed explicitly so that compound heading style
# still resolves rather than falling through to null.
_DUTIES_START_RE = re.compile(
    r'\bDuties\s+The Role\s+'       # combined double-header (~17%): skip both labels
    r'|\bDuties\s+'                  # solo Duties marker (~80%): skip label
    r'|\bAbout [Tt]he [Rr]ole\s+'   # "About The/the Role" compound heading
    r'|(?<![a-zA-Z] )The Role\s+'   # standalone The Role — not preceded by word+space
)
_ELIGIBILITY_RE = re.compile(r'\bEligibility\b')


def _extract_duties_text(description) -> str | None:
    """Extract the job responsibilities section from a gazette description.

    Bounded by the first Duties/The Role marker and the Eligibility marker.
    Section labels themselves are stripped from the output.
    Returns null if no duties-type marker is present.
    """
    if not description or pd.isna(description):
        return None
    s = str(description)
    start = _DUTIES_START_RE.search(s)
    if not start:
        return None
    text = s[start.end():]
    end = _ELIGIBILITY_RE.search(text)
    if end:
        text = text[:end.start()]
    return text.strip() or None


def _normalise_classification(s) -> str | None:
    """Contract long-form classification names to compact codes (APS Level 6 → APS6).

    Applied as in-place substitutions — semicolons work automatically:
    'APS Level 5;APS Level 6' → 'APS5;APS6'. Unrecognised strings pass through.
    """
    if not s or pd.isna(s):
        return None
    s = str(s)
    s = re.sub(r'Executive Level (\d)', r'EL\1', s)
    s = re.sub(r'APS Level (\d)',       r'APS\1', s)
    s = re.sub(r'Senior Executive Service Band (\d)', r'SES\1', s)
    s = s.replace('Graduate APS', 'Graduate')
    return s.strip() or None


def run(dry_run: bool = False) -> None:
    if not RAW_PATH.exists():
        print(f"Input not found: {RAW_PATH}", file=sys.stderr)
        print("Run '02_parse.py batch' first.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(RAW_PATH)
    df['gazette_date'] = pd.to_datetime(df['gazette_date'])
    print(f"Loaded {RAW_PATH}: {len(df):,} rows, {len(df.columns)} cols\n")

    # ── salary_min / salary_max ───────────────────────────────────────────

    df['salary_min'] = df['salary'].apply(_parse_salary_min)
    df['salary_max'] = df['salary'].apply(_parse_salary_max)
    print(f"salary_min: {df['salary_min'].notna().sum():,} non-null")
    print(f"salary_max: {df['salary_max'].notna().sum():,} non-null")

    # ── salary guards ─────────────────────────────────────────────────────

    # Guard 1: salary_max ≈10× true value — PDF rendering artifact where a
    # comma or digit is duplicated during text extraction.
    n = (df['salary_max'] > 500_000).sum()
    print(f"Guard 1 (salary_max >500k):         {n:3d} rows → salary_max = None")
    df.loc[df['salary_max'] > 500_000, 'salary_max'] = None

    # Guard 2: same artifact on salary_min.
    n = (df['salary_min'] > 500_000).sum()
    print(f"Guard 2 (salary_min >500k):         {n:3d} rows → salary_min = None")
    df.loc[df['salary_min'] > 500_000, 'salary_min'] = None

    # Guard 3: inverted range — salary_min > salary_max after guards 1–2.
    # Preserve min (more reliable); null the unreliable max.
    inverted = (
        df['salary_min'].notna() & df['salary_max'].notna() &
        (df['salary_min'] > df['salary_max'])
    )
    print(f"Guard 3 (salary_min > salary_max):  {inverted.sum():3d} rows → salary_max = None")
    df.loc[inverted, 'salary_max'] = None

    # Guard 4: mixed hourly/annual format (e.g. "$90 - $101,031").
    # The hourly rate lands in salary_min; the annual figure is salary_max.
    mixed = (
        df['salary_min'].notna() & df['salary_max'].notna() &
        (df['salary_min'] < 1_000) & (df['salary_max'] > 30_000)
    )
    print(f"Guard 4 (mixed hourly/annual):      {mixed.sum():3d} rows → salary_min = None")
    df.loc[mixed, 'salary_min'] = None

    # Guard 5: single-digit dollar start ("$1 - $100").
    # The regex skips $1 (only one digit) and picks up $100 as salary_min.
    # Detected by checking the raw salary string directly.
    single_digit = df['salary'].str.match(r'^\$[1-9]\s*[-–]', na=False)
    print(f"Guard 5 ($[1-9] start pattern):     {single_digit.sum():3d} rows → salary_min = None")
    df.loc[single_digit, 'salary_min'] = None

    # ── closing_date ──────────────────────────────────────────────────────

    df['closing_date'] = df['closing_date_raw'].apply(_parse_closing_date)
    print(f"\nclosing_date: {df['closing_date'].notna().sum():,} non-null")

    # Guard 6: closing_date >1 year before gazette_date.
    # Caused by "Closing Date:" references in description text of re-posted
    # notices picking up a historical date. See parser comment on VN-0733046.
    cd_ts = pd.to_datetime(df['closing_date'], errors='coerce')
    stale = (
        cd_ts.notna() & df['gazette_date'].notna() &
        ((df['gazette_date'] - cd_ts).dt.days > 365)
    )
    print(f"Guard 6 (stale closing_date >1yr):  {stale.sum():3d} rows → closing_date = None")
    df.loc[stale, 'closing_date'] = None

    # ── location_normalised ───────────────────────────────────────────────

    df['location_normalised'] = df['location'].apply(_normalise_location)
    print(f"\nlocation_normalised: {df['location_normalised'].notna().sum():,} non-null")

    # ── classification_clean ──────────────────────────────────────────────

    df['classification_clean'] = df['classification'].apply(_normalise_classification)
    print(f"classification_clean: {df['classification_clean'].notna().sum():,} non-null")

    # ── duties_text ───────────────────────────────────────────────────────

    df['duties_text'] = df['description'].apply(_extract_duties_text)
    n_dt = df['duties_text'].notna().sum()
    print(f"duties_text: {n_dt:,} non-null ({n_dt / len(df) * 100:.1f}%)")

    # ── Column ordering (matches data dictionary) ─────────────────────────

    col_order = [
        # Provenance
        'gazette_id', 'gazette_date', 'gazette_type', 'vacancy_no', 'raw_vn_count',
        # Agency (canonical columns added by 05_apply_crosswalk.py)
        'agency', 'division', 'branch',
        # Role
        'job_title', 'job_type',
        'location', 'location_normalised',
        'salary', 'salary_min', 'salary_max',
        'classification', 'classification_clean',
        'position_number',
        'closing_date_raw', 'closing_date',
        # Office arrangement
        'office_arrangement', 'office_arrangement_details',
        # Links and text
        'agency_website',
        'description',
        'duties_text',
    ]
    extra = [c for c in df.columns if c not in col_order]
    if extra:
        print(f"\nUnexpected columns (appended): {extra}")
    df = df[col_order + extra]

    # ── Summary ───────────────────────────────────────────────────────────

    print(f"\n=== OUTPUT: {len(df):,} rows, {len(df.columns)} cols ===")
    new_cols = ['salary_min', 'salary_max', 'closing_date', 'location_normalised', 'classification_clean', 'duties_text']
    for col in new_cols:
        n_null = df[col].isna().sum()
        pct    = n_null / len(df) * 100
        print(f"  {col:25s}  {df[col].notna().sum():7,} non-null  "
              f"({n_null:,} null, {pct:.1f}%)")

    if dry_run:
        print("\n[dry-run] not writing output")
        return

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"\nSaved → {OUT_PATH}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Normalise raw gazette parse output — add salary, date, location and classification columns.'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='compute and report without writing output file',
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
