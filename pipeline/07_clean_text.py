#!/usr/bin/env python3
"""
07_clean_text.py — Strip agency boilerplate and redact contact details from
gazette vacancy descriptions.

Reads data/release/aps_gazette_vacancies.parquet, adds a description_clean column,
and writes the updated file back (both parquet and csv.gz).

Boilerplate method
------------------
1. Split each description into sentences (paragraph → newline → sentence-boundary).
2. Strip section-label prefixes ("Duties", "Eligibility", "Notes") and drop sentences
   that begin with "About the/our/us" (agency mission section header).
3. Normalise each sentence (lowercase, replace punctuation with spaces, collapse whitespace).
4. Bin ads by agency_canonical × half-year period.
5. Count how many distinct ads in each bin contain each normalised sentence.
6. For sentences exceeding THRESHOLD in any qualifying bin (≥ MIN_BIN ads), also count
   distinct normalised job titles in that highest-fraction bin.
7. Flag the sentence only if it also appears across ≥ MIN_TITLES distinct job titles
   in that bin — this prevents bulk-recruitment templates (same role posted to many
   locations) from being mistaken for cross-role boilerplate.
8. Reconstruct description_clean by joining the non-flagged sentences.

Ads with null description get null description_clean.

PII redaction (applied after boilerplate stripping)
---------------------------------------------------
Email addresses (something@domain.tld) are replaced with [email redacted].
Australian phone numbers in all common formats are replaced with [phone redacted].
Contact officer names are replaced with [name redacted] when a title-case name
(2–4 words, each capitalised) follows "please contact", "contact officer", or
"for enquiries". Names not in title case are not caught.
Redaction is applied to description, description_clean, and duties_text.
Redaction is idempotent: running the step twice produces the same result.

Usage
-----
    python pipeline/07_clean_text.py [--threshold 0.30] [--min-titles 3] [--dry-run]

Outputs
-------
    data/release/aps_gazette_vacancies.parquet      (updated in place, skipped with --dry-run)
    data/release/aps_gazette_vacancies.csv.gz       (updated in place, skipped with --dry-run)
    data/diagnostics/boilerplate_sentences.csv      (always written — audit trail)
"""

import argparse
import collections
import os
import re
import sys
from pathlib import Path

import pandas as pd

PARQUET_PATH  = Path("data/release/aps_gazette_vacancies.parquet")
CSV_PATH      = Path("data/release/aps_gazette_vacancies.csv.gz")
AUDIT_CSV     = Path("data/diagnostics/boilerplate_sentences.csv")

DEFAULT_THRESHOLD  = 0.30
DEFAULT_MIN_TITLES = 3
MIN_BIN            = 10      # bins smaller than this are never used for flagging
BIN_PERIOD         = "2Q"    # half-year (change to "Q" for quarterly)


# ── Text processing ────────────────────────────────────────────────────────────

_LABEL_PREFIX_RE = re.compile(
    r'^(?:(?:Duties(?:\s+The\s+Role)?|Eligibility|Notes)\s+)+',
    re.IGNORECASE,
)
_ABOUT_HEADER_RE = re.compile(r'^About\s+(?:the|our|us)\b', re.IGNORECASE)
# Splits on sentence-ending punctuation followed by a capital — deliberately
# does not handle abbreviations (e.g. "Dr.", "e.g.") to keep the regex simple.
_SENT_RE         = re.compile(r'(?<=[.!?])\s+(?=[A-Z])')

# Strip trailing location suffix ("- Melbourne CBD, Victoria") and year prefix ("2023 - ")
_TITLE_LOCATION_RE = re.compile(r'\s*-\s*[a-z][a-z ,]+(?:,\s*[a-z ]+)?$')
_TITLE_YEAR_RE     = re.compile(r'^\d{4}\s*-\s*')


def _strip_inline_header(sentence: str) -> str | None:
    """Strip section-label prefix, or return None if the sentence IS a header."""
    if _ABOUT_HEADER_RE.match(sentence):
        return None
    s = _LABEL_PREFIX_RE.sub("", sentence).strip()
    return s if s else None


def split_sentences(text: str) -> list[str]:
    """Split description text into sentences after inline-header stripping."""
    if not isinstance(text, str):
        return []
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    out = []
    for para in re.split(r"\n{2,}", text):
        for line in para.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            for part in _SENT_RE.split(line):
                s = _strip_inline_header(part.strip())
                if s:
                    out.append(s)
    return out


def normalise(sentence: str) -> str:
    """Lowercase, replace punctuation with spaces, collapse whitespace — for frequency counting."""
    s = sentence.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalise_title(title: str) -> str:
    """Normalise job title for diversity counting: strip location suffixes and year prefixes."""
    if not isinstance(title, str):
        return ""
    t = title.lower()
    t = _TITLE_LOCATION_RE.sub("", t)
    t = _TITLE_YEAR_RE.sub("", t)
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


# ── PII redaction ─────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

_PHONE_RE = re.compile(
    r'(?:'
    r'\+61[\s\-.]?4\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3}'    # +61 4XX XXX XXX
    r'|04\d{2}[\s\-.]?\d{3}[\s\-.]?\d{3}'               # 04XX XXX XXX
    r'|\+61[\s\-.]?\d[\s\-.]?\d{4}[\s\-.]?\d{4}'        # +61 X XXXX XXXX
    r'|\(\s*0\d\s*\)[\s\-.]?\d{4}[\s\-.]?\d{4}'         # (0X) XXXX XXXX
    r'|0\d[\s\-.]?\d{4}[\s\-.]?\d{4}'                   # 0X XXXX XXXX
    r'|1[38]00[\s\-.]?\d{3}[\s\-.]?\d{3}'               # 1800/1300 XXX XXX
    r'|13[\s\-.]?\d{2}[\s\-.]?\d{2}'                    # 13 XX XX
    r')'
)

_CONTACT_NAME_RE = re.compile(
    r'(?:please\s+contact|contact\s+officer|for\s+enquiries)'
    r'[\s:,\-–]*'
    r'(?!officer\b)'
    r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
    re.IGNORECASE,
)

_PII_COLS = ["description", "description_clean", "duties_text"]


def _redact_contact_name(m: re.Match) -> str:
    name = m.group(1)
    if not all(w[0].isupper() for w in name.split()):
        return m.group(0)
    return m.group(0)[: -len(name)] + "[name redacted]"


def redact_pii(text: str | None) -> str | None:
    if not isinstance(text, str):
        return text
    text = _EMAIL_RE.sub("[email redacted]", text)
    text = _PHONE_RE.sub("[phone redacted]", text)
    text = _CONTACT_NAME_RE.sub(_redact_contact_name, text)
    return text


# ── Boilerplate detection ──────────────────────────────────────────────────────

def build_boilerplate_sets(
    df: pd.DataFrame,
    threshold: float,
    min_bin: int,
    bin_period: str,
    min_title_diversity: int,
) -> tuple[dict[str, frozenset[str]], list[dict]]:
    """
    For each agency, return the set of normalised sentences that:
      - appear in ≥ threshold fraction of ads in at least one qualifying bin, AND
      - appear across ≥ min_title_diversity distinct normalised job titles in that
        same highest-fraction bin.

    Also returns audit_rows: one row per (agency, sentence) that exceeded the
    frequency threshold, including those rescued by the title-diversity guard.
    """
    df = df[df["description"].notna()].copy()
    df["_period"]     = pd.to_datetime(df["gazette_date"]).dt.to_period(bin_period)
    df["_title_norm"] = df["job_title"].apply(normalise_title)

    agency_bp:  dict[str, frozenset[str]] = {}
    audit_rows: list[dict]                = []
    agencies = sorted(df["agency_canonical"].dropna().unique())

    for agency in agencies:
        sub = df[df["agency_canonical"] == agency]

        # per sentence: track best (highest-fraction) bin stats
        # sent_best[ns] = (period_str, frac, n_ads, bin_size, n_titles)
        sent_best:     dict[str, tuple] = {}
        sent_original: dict[str, str]   = {}

        for period, grp in sub.groupby("_period"):
            if len(grp) < min_bin:
                continue
            bin_size = len(grp)

            sent_ads:    collections.defaultdict[str, set] = collections.defaultdict(set)
            sent_titles: collections.defaultdict[str, set] = collections.defaultdict(set)

            for _, row in grp.iterrows():
                sents_in_ad: set[str] = set()
                for s in split_sentences(row["description"]):
                    ns = normalise(s)
                    if ns:
                        sents_in_ad.add(ns)
                        if ns not in sent_original:
                            sent_original[ns] = s
                for ns in sents_in_ad:
                    sent_ads[ns].add(row["vacancy_no"])
                    sent_titles[ns].add(row["_title_norm"])

            for ns, ads in sent_ads.items():
                frac     = len(ads) / bin_size
                n_titles = len(sent_titles[ns])
                if ns not in sent_best or frac > sent_best[ns][1]:
                    sent_best[ns] = (str(period), frac, len(ads), bin_size, n_titles)

        # Flag: must exceed both frequency threshold AND title diversity
        flagged: set[str] = set()
        for ns, (period_str, frac, n_ads, bin_size, n_titles) in sent_best.items():
            if frac < threshold:
                continue
            is_flagged = n_titles >= min_title_diversity
            if is_flagged:
                flagged.add(ns)
            audit_rows.append({
                "agency_canonical":    agency,
                "half_year":           period_str,
                "sentence_normalised": ns,
                "n_ads":               n_ads,
                "bin_size":            bin_size,
                "pct_of_bin":          round(frac * 100, 1),
                "n_titles":            n_titles,
                "flagged":             is_flagged,
                "sentence_original":   sent_original.get(ns, ""),
            })

        agency_bp[agency] = frozenset(flagged)

    return agency_bp, audit_rows


# ── Cleaning ───────────────────────────────────────────────────────────────────

def clean_description(
    description: str | None,
    boilerplate: frozenset[str],
) -> str | None:
    if not isinstance(description, str):
        return None
    sents = split_sentences(description)
    kept  = [s for s in sents if normalise(s) not in boilerplate]
    return " ".join(kept) if kept else None


# ── Main ───────────────────────────────────────────────────────────────────────

def run(threshold: float, min_title_diversity: int, dry_run: bool) -> None:
    if not PARQUET_PATH.exists():
        print(f"Input not found: {PARQUET_PATH}", file=sys.stderr)
        print("Run '06_build_release.py' first.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(PARQUET_PATH)
    print(
        f"Loaded {PARQUET_PATH}: {len(df):,} rows, {len(df.columns)} cols\n"
        f"Threshold: {threshold:.0%}  |  min_titles: {min_title_diversity}  "
        f"|  min_bin: {MIN_BIN}  |  period: {BIN_PERIOD}\n"
    )

    # ── Build boilerplate sets ─────────────────────────────────────────────────

    print("Building boilerplate sets...")
    agency_bp, audit_rows = build_boilerplate_sets(
        df, threshold, MIN_BIN, BIN_PERIOD, min_title_diversity
    )

    n_agencies_with_bp = sum(1 for bp in agency_bp.values() if bp)
    total_flagged      = sum(len(bp) for bp in agency_bp.values())
    total_freq_thresh  = len(audit_rows)
    rescued            = total_freq_thresh - total_flagged

    print(f"  {len(agency_bp)} agencies processed")
    print(f"  {n_agencies_with_bp} agencies have ≥1 flagged sentence")
    print(f"  {total_freq_thresh:,} sentences exceeded frequency threshold")
    print(f"  {rescued:,} rescued by title-diversity guard (< {min_title_diversity} distinct titles)")
    print(f"  {total_flagged:,} (agency, sentence) pairs flagged\n")

    top = sorted(agency_bp.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    print("  Top 10 agencies by flagged-sentence count:")
    for ag, bp in top:
        if bp:
            print(f"    {len(bp):3d}  {ag}")
    print()

    # ── Apply per-row ──────────────────────────────────────────────────────────

    print("Applying boilerplate filter...")

    def _clean(row) -> str | None:
        bp = agency_bp.get(row["agency_canonical"], frozenset())
        return clean_description(row["description"], bp)

    df["description_clean"] = df.apply(_clean, axis=1)

    # ── Statistics ─────────────────────────────────────────────────────────────

    has_desc  = df["description"].notna()
    has_clean = df["description_clean"].notna()

    orig_chars  = df.loc[has_desc,  "description"].str.len().sum()
    clean_chars = df.loc[has_clean, "description_clean"].str.len().sum()
    retained_pct = clean_chars / orig_chars * 100 if orig_chars else 0

    n_fully_stripped = (has_desc & ~has_clean).sum()

    df_cmp = df[has_desc].copy()
    df_cmp["_orig_c"]  = df_cmp["description"].str.len()
    df_cmp["_clean_c"] = df_cmp["description_clean"].str.len().fillna(0)
    df_cmp["_ret"]     = df_cmp["_clean_c"] / df_cmp["_orig_c"]
    q = df_cmp["_ret"].quantile([0.25, 0.50, 0.75])

    print(f"  Ads with description:             {has_desc.sum():,}")
    print(f"  Ads with description_clean:       {has_clean.sum():,}")
    print(f"  Fully stripped (all boilerplate): {n_fully_stripped}")
    print(f"  Characters retained:              {retained_pct:.1f}%")
    print(f"    ({clean_chars:,} / {orig_chars:,} chars)")
    print(f"  Per-ad retention — median: {q[0.50]:.1%}  Q25: {q[0.25]:.1%}  Q75: {q[0.75]:.1%}\n")

    # ── Audit CSV (always written) ─────────────────────────────────────────────

    AUDIT_CSV.parent.mkdir(parents=True, exist_ok=True)
    audit_df = (
        pd.DataFrame(audit_rows)
        .sort_values(["agency_canonical", "pct_of_bin"], ascending=[True, False])
    )
    audit_df.to_csv(AUDIT_CSV, index=False)
    print(f"Audit CSV → {AUDIT_CSV}  ({len(audit_df):,} rows)")
    print(f"  {audit_df['flagged'].sum():,} flagged  |  {(~audit_df['flagged']).sum():,} rescued by title-diversity guard\n")

    # ── PII redaction ─────────────────────────────────────────────────────────

    print("Redacting PII...")
    rows_before = {col: df[col].copy() for col in _PII_COLS if col in df.columns}
    for col in _PII_COLS:
        if col in df.columns:
            df[col] = df[col].map(redact_pii)

    for col, before in rows_before.items():
        # NaN != NaN is True in pandas, so this slightly overstates changes in null-heavy columns.
        n_changed = (df[col] != before).sum()
        print(f"  {col}: {n_changed:,} rows changed")
    print()

    # ── Column placement: description_clean immediately after description ───────

    cols = list(df.columns)
    if "description_clean" in cols:
        cols.remove("description_clean")
    desc_idx = cols.index("description")
    cols.insert(desc_idx + 1, "description_clean")
    df = df[cols]

    # ── Write ──────────────────────────────────────────────────────────────────

    if dry_run:
        print("[dry-run] not writing parquet/csv output")
        return

    df.to_parquet(PARQUET_PATH, index=False)
    df.to_csv(CSV_PATH, index=False, compression="gzip")

    p_mb = os.path.getsize(PARQUET_PATH) / 1e6
    c_mb = os.path.getsize(CSV_PATH)     / 1e6
    print(f"Saved → {PARQUET_PATH}  ({p_mb:.1f} MB)")
    print(f"Saved → {CSV_PATH}  ({c_mb:.1f} MB)")
    print(f"\nDone. {len(df):,} rows, {len(df.columns)} columns.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Strip agency boilerplate from gazette vacancy descriptions."
    )
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_THRESHOLD, metavar="FRAC",
        help=f"Fraction of ads in a bin that must contain a sentence to flag it "
             f"(default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--min-titles", type=int, default=DEFAULT_MIN_TITLES, metavar="N",
        help=f"Minimum distinct job titles a sentence must span in its highest-frequency "
             f"bin to be flagged — prevents bulk-recruitment templates from being "
             f"mistaken for boilerplate (default: {DEFAULT_MIN_TITLES})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="compute and report without writing parquet/csv output files "
             "(audit CSV is still written)",
    )
    args = parser.parse_args()

    if not 0 < args.threshold < 1:
        print("--threshold must be between 0 and 1 (exclusive)", file=sys.stderr)
        sys.exit(1)
    if args.min_titles < 1:
        print("--min-titles must be ≥ 1", file=sys.stderr)
        sys.exit(1)

    run(threshold=args.threshold, min_title_diversity=args.min_titles, dry_run=args.dry_run)
