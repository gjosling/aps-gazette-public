#!/usr/bin/env python3
"""
07_clean_text.py — Strip agency boilerplate and redact contact details from
gazette vacancy descriptions.

Reads data/release/aps_gazette_vacancies.parquet, adds a description_clean column,
and writes the updated file back (both parquet and csv.gz).

Boilerplate method (two passes; effective set = per-agency ∪ global)
-------------------------------------------------------------------
Per-agency pass:
1. Split each description into sentences (paragraph → newline → sentence-boundary).
   An inline "About the/our/us <section>" header fused to the following sentence
   by the absence of punctuation (e.g. "About the Role We have...") is split from
   it here, so the header can be dropped without deleting the body (see
   _ABOUT_HEADER_SPLIT_RE) — before this fix the whole fused unit was discarded.
2. Strip section-label prefixes ("Duties", "Eligibility", "Notes") and drop sentences
   that begin with "About the/our/us" (agency mission section header) — now only
   ever a genuine standalone header unit, per (1).
3. Normalise each sentence (lowercase, replace punctuation with spaces, collapse whitespace).
4. Bin ads by agency_canonical × quarter period.
5. Count how many distinct ads in each bin contain each normalised sentence.
6. For sentences exceeding THRESHOLD in any qualifying bin (≥ MIN_BIN ads), also count
   distinct normalised job titles in that highest-fraction bin.
7. Flag the sentence only if it also appears across ≥ MIN_TITLES distinct job titles
   in that bin — this prevents bulk-recruitment templates (same role posted to many
   locations) from being mistaken for cross-role boilerplate.

Global pass (review finding F4): the per-agency method can never learn
gazette-wide template text for small agencies (their bins fall below MIN_BIN). A
corpus-level pass bins ALL ads by quarter and flags a normalised sentence when,
in any bin (≥ GLOBAL_MIN_BIN_ADS ads), it appears in ≥ GLOBAL_THRESHOLD of the
bin AND spans ≥ GLOBAL_MIN_TITLES distinct job titles. The global set applies to
every agency, including sub-MIN_BIN ones. It catches the RecruitAbility standard
passage and the May-2025 gazette eligibility passage. Global-pass audit rows use
agency_canonical = "__GLOBAL__".

8. Reconstruct description_clean by joining the sentences flagged by neither pass
   and not matching PROTECTED_RE (multi-position, pool/register, intake-structure
   or eligibility markers) — a protected sentence is never stripped even if
   frequency-flagged; it is still listed in the audit CSV with protected=True.

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
    python pipeline/07_clean_text.py [--threshold 0.30] [--min-titles 3]
        [--global-threshold 0.40] [--global-min-titles 30] [--dry-run]

Outputs
-------
    data/release/aps_gazette_vacancies.parquet      (updated in place, skipped with --dry-run)
    data/release/aps_gazette_vacancies.csv.gz       (updated in place, skipped with --dry-run)
    data/diagnostics/boilerplate_sentences.csv                        (stable name — always written)
    data/diagnostics/boilerplate_sentences-<version>-<YYYYMMDD>.csv   (dated archive — always written)
"""

import argparse
import collections
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# validation.py is a legal module name; make it importable by adding the
# pipeline/ dir to sys.path (the numerically-prefixed pipeline scripts can't be).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import validation
import release_io

PARQUET_PATH  = Path("data/release/aps_gazette_vacancies.parquet")
CSV_PATH      = Path("data/release/aps_gazette_vacancies.csv.gz")
AUDIT_DIR     = Path("data/diagnostics")
AUDIT_CSV     = AUDIT_DIR / "boilerplate_sentences.csv"   # stable name (diff workflows)

# Version of the boilerplate-stripping method, stamped into release metadata by
# release_io.build_metadata (parquet key aps_gazette:boilerplate_method_version and
# the .meta.json sidecar) and used in the dated audit-CSV filename. BUMP THIS
# constant on ANY change to the thresholds, sentence splitting/normalisation, or
# pass structure — it is the only marker that description_clean was recomputed by a
# different method. v2 added the global corpus-wide pass (bumped from v1). v3
# (spec 09 Phase B) fixed the fused About-header segmenter bug and added the
# PROTECTED_RE guard — see CHANGELOG.
BOILERPLATE_METHOD_VERSION = "2026-07-v3"

DEFAULT_THRESHOLD  = 0.30
DEFAULT_MIN_TITLES = 3
MIN_BIN            = 10      # bins smaller than this are never used for flagging
BIN_PERIOD         = "Q"     # quarterly (pd.to_period freqstr)

# Global (corpus-wide) pass. Bins ALL ads by quarter; no MIN_BIN needed
# (every quarterly bin has thousands of ads), but degenerate partial periods are
# skipped via GLOBAL_MIN_BIN_ADS.
DEFAULT_GLOBAL_THRESHOLD  = 0.40   # ≥40% of a corpus bin; below this, one dominant
                                   # agency's own blurb can cross the bar (see spec)
DEFAULT_GLOBAL_MIN_TITLES = 30     # distinct normalised job titles in that bin
GLOBAL_MIN_BIN_ADS        = 500    # skip corpus bins smaller than this (defensive)
GLOBAL_AGENCY_SENTINEL    = "__GLOBAL__"   # agency_canonical value for global audit rows


# ── Text processing ────────────────────────────────────────────────────────────

_LABEL_PREFIX_RE = re.compile(
    r'^(?:(?:Duties(?:\s+The\s+Role)?|Eligibility|Notes)\s+)+',
    re.IGNORECASE,
)
_ABOUT_HEADER_RE = re.compile(r'^About\s+(?:the|our|us)\b', re.IGNORECASE)
# Splits on sentence-ending punctuation followed by a capital — deliberately
# does not handle abbreviations (e.g. "Dr.", "e.g.") to keep the regex simple.
_SENT_RE         = re.compile(r'(?<=[.!?])\s+(?=[A-Z])')

# 09A diagnosis (Step 4): when an inline "About the/our/us <section>" header has
# no sentence-ending punctuation before the sentence that follows it (e.g. "About
# the Role We have a number of roles available…"), _SENT_RE can't split them, so
# _ABOUT_HEADER_RE (below) used to drop the whole fused unit — header AND body —
# unconditionally, deleting substantive, non-boilerplate content. This regex
# peels a bounded header noun-phrase off the front of a fused unit so the body
# becomes its own unit and reaches the frequency pass like any other sentence.
# It only fires on an enumerated set of section-header nouns (a genuine
# standalone header, e.g. "About the Australian Antarctic Division", has no
# trailing content and so never matches — it still falls through to
# _ABOUT_HEADER_RE's whole-unit drop, which remains correct for that case).
_ABOUT_HEAD_NOUN = (
    r'(?:(?:team|role|position|department|division|branch|group|directorate|unit|'
    r'agency|organisation|organization|section|office|program|programme|'
    r'authority|commission|bureau|area|tribunal|person)s?'
    r'|opportunit(?:y|ies))'
)
# Words that start a new sentence rather than continue an "of <name>" phrase
# (e.g. "About the Department of Parliamentary Services The Department…" must
# stop the header at "Services", not swallow "The Department").
_SENT_START_WORD = r'(?:the|this|that|these|those|it|they|we|you|our|your|a|an)'
_ABOUT_HEADER_SPLIT_RE = re.compile(
    rf'^(About\s+(?:the|our|us)\s+(?:[A-Za-z][\w\'&-]*\s+){{0,3}}?{_ABOUT_HEAD_NOUN}'
    rf'(?:\s+of\s+(?:(?!{_SENT_START_WORD}\b)[A-Za-z][\w\'&-]*)'
    rf'(?:\s+(?!{_SENT_START_WORD}\b)[A-Za-z][\w\'&-]*){{0,4}})?)'
    rf'\s*:?\s+(?=\S)',
    re.IGNORECASE,
)

# Strip trailing location suffix ("- Melbourne CBD, Victoria") and year prefix ("2023 - ")
_TITLE_LOCATION_RE = re.compile(r'\s*-\s*[a-z][a-z ,]+(?:,\s*[a-z ]+)?$')
_TITLE_YEAR_RE     = re.compile(r'^\d{4}\s*-\s*')


def _strip_inline_header(sentence: str) -> str | None:
    """Strip section-label prefix, or return None if the sentence IS a header."""
    if _ABOUT_HEADER_RE.match(sentence):
        return None
    s = _LABEL_PREFIX_RE.sub("", sentence).strip()
    return s if s else None


def _split_fused_header(part: str) -> list[str]:
    """Peel a fused inline About-header off the front of `part` (see
    _ABOUT_HEADER_SPLIT_RE) so the following sentence becomes its own unit.
    Returns [part] unchanged if there's no recognised fused header, or if the
    header has no trailing body (a genuine standalone header)."""
    m = _ABOUT_HEADER_SPLIT_RE.match(part)
    if not m:
        return [part]
    head = m.group(1).strip()
    rest = part[m.end():].strip()
    return [head, rest] if rest else [part]


def split_sentences(text: str, stats: dict | None = None) -> list[str]:
    """Split description text into sentences after inline-header stripping.

    `stats`, if given, is incremented in place with counts of units dropped
    by the About-header whole-unit rule (`about_header`) and by the
    empty-after-label-strip rule (`label_empty`) — used to log the
    otherwise-invisible header removal path (see 09A diagnosis Step 3/B.3)."""
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
                part = part.strip()
                if not part:
                    continue
                for unit in _split_fused_header(part):
                    s = _strip_inline_header(unit)
                    if s is None:
                        if stats is not None:
                            key = "about_header" if _ABOUT_HEADER_RE.match(unit) else "label_empty"
                            stats[key] = stats.get(key, 0) + 1
                        continue
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


# ── Protected-phrase guard (spec 09 B.1) ────────────────────────────────────────
#
# Some sentences are simultaneously template (frequency-flagged) AND substantive
# (assert a fact about the posting — multi-position, pool/register, intake
# structure, eligibility). A sentence matching PROTECTED_RE is never stripped,
# regardless of frequency; it stays listed in the audit record (protected=True)
# so the record remains informative about template frequency, but the stripping
# is suppressed. See 09A diagnosis Step 5(c) for the size of this carve-out.
#
# The merit-pool alternation ("a merit pool may be created…") is deliberately
# EXCLUDED: it is near-universal legal-adjacent boilerplate that is only weakly
# informative on its own (spec 08 already declines to treat it as a bulk-posting
# marker), and protecting it would visibly inflate clean-text length across ~35
# heavily-templated entries for little analytical value — a bad trade against
# the ~25 flagged entries the rest of the pattern protects.
PROTECTED_RE = re.compile(
    r'(?i)'
    r'multiple\s+(?:positions?|vacancies|roles?)'
    r'|(?:a\s+)?number\s+of\s+(?:positions?|vacancies|roles)'
    r'|various\s+(?:positions?|vacancies|roles)'
    r'|bulk\s+recruitment'
    r'|current\s+and\s+anticipated\s+vacanc'
    r'|(?:employment|talent)\s+register'
    r'|expressions?\s+of\s+interest'
    r'|graduate\s+program|apprenticeship|cadetship|traineeship|school\s+leaver'
    r'|identified\s+position'
)


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
            original = sent_original.get(ns, "")
            audit_rows.append({
                "agency_canonical":    agency,
                "quarter":             period_str,
                "sentence_normalised": ns,
                "n_ads":               n_ads,
                "bin_size":            bin_size,
                "pct_of_bin":          round(frac * 100, 1),
                "n_titles":            n_titles,
                "flagged":             is_flagged,
                "protected":           bool(PROTECTED_RE.search(original)),
                "sentence_original":   original,
            })

        agency_bp[agency] = frozenset(flagged)

    return agency_bp, audit_rows


def build_global_boilerplate_set(
    df: pd.DataFrame,
    global_threshold: float,
    global_min_titles: int,
    bin_period: str,
    min_bin_ads: int = GLOBAL_MIN_BIN_ADS,
) -> tuple[frozenset[str], list[dict]]:
    """
    Corpus-level boilerplate pass (review finding F4). Bins ALL ads (no agency
    split) by quarter and returns the set of normalised sentences that, in any
    qualifying bin (≥ min_bin_ads ads):
      - appear in ≥ global_threshold fraction of that bin's ads, AND
      - span ≥ global_min_titles distinct normalised job titles in that
        highest-fraction bin.

    The global set catches gazette-wide template text (the RecruitAbility standard
    passage; the May-2025 eligibility passage) that small agencies' per-agency bins
    can never learn. It is unioned with each agency's set, applying to every agency
    including sub-MIN_BIN ones — that is the fix.

    Uses the same best-(highest-fraction)-bin machinery as build_boilerplate_sets,
    so the flagged set is the conservative subset (a genuine corpus-wide template
    dominant in ≥40% of thousands of ads spans hundreds of titles, so best-bin and
    any-bin agree on the real targets).

    Also returns audit_rows: one row per sentence that exceeded global_threshold
    (flagged or rescued by the title guard), each with agency_canonical =
    GLOBAL_AGENCY_SENTINEL and the same columns as the per-agency audit rows.
    """
    df = df[df["description"].notna()].copy()
    df["_period"]     = pd.to_datetime(df["gazette_date"]).dt.to_period(bin_period)
    df["_title_norm"] = df["job_title"].apply(normalise_title)

    # per sentence: track best (highest-fraction) bin stats across all bins
    sent_best:     dict[str, tuple] = {}
    sent_original: dict[str, str]   = {}

    for period, grp in df.groupby("_period"):
        if len(grp) < min_bin_ads:
            continue                                 # degenerate partial period
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

    flagged: set[str]      = set()
    audit_rows: list[dict] = []
    for ns, (period_str, frac, n_ads, bin_size, n_titles) in sent_best.items():
        if frac < global_threshold:
            continue
        is_flagged = n_titles >= global_min_titles
        if is_flagged:
            flagged.add(ns)
        original = sent_original.get(ns, "")
        audit_rows.append({
            "agency_canonical":    GLOBAL_AGENCY_SENTINEL,
            "quarter":             period_str,
            "sentence_normalised": ns,
            "n_ads":               n_ads,
            "bin_size":            bin_size,
            "pct_of_bin":          round(frac * 100, 1),
            "n_titles":            n_titles,
            "flagged":             is_flagged,
            "protected":           bool(PROTECTED_RE.search(original)),
            "sentence_original":   original,
        })

    return frozenset(flagged), audit_rows


# ── Cleaning ───────────────────────────────────────────────────────────────────

def clean_description(
    description: str | None,
    boilerplate: frozenset[str],
    stats: dict | None = None,
) -> str | None:
    if not isinstance(description, str):
        return None
    sents = split_sentences(description, stats=stats)
    # Protected sentences are never stripped, even if frequency-flagged — see
    # PROTECTED_RE above (spec 09 B.1).
    kept = [s for s in sents if PROTECTED_RE.search(s) or normalise(s) not in boilerplate]
    return " ".join(kept) if kept else None


# ── Main ───────────────────────────────────────────────────────────────────────

def run(
    threshold: float,
    min_title_diversity: int,
    global_threshold: float,
    global_min_titles: int,
    dry_run: bool,
) -> None:
    if not PARQUET_PATH.exists():
        print(f"Input not found: {PARQUET_PATH}", file=sys.stderr)
        print("Run '06_build_release.py' first.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(PARQUET_PATH)
    print(
        f"Loaded {PARQUET_PATH}: {len(df):,} rows, {len(df.columns)} cols\n"
        f"Method version: {BOILERPLATE_METHOD_VERSION}\n"
        f"Per-agency — threshold: {threshold:.0%}  |  min_titles: {min_title_diversity}  "
        f"|  min_bin: {MIN_BIN}  |  period: {BIN_PERIOD}\n"
        f"Global     — threshold: {global_threshold:.0%}  |  min_titles: {global_min_titles}  "
        f"|  min_bin_ads: {GLOBAL_MIN_BIN_ADS}  |  period: {BIN_PERIOD}\n"
    )

    # ── Build per-agency boilerplate sets ──────────────────────────────────────

    print("Building per-agency boilerplate sets...")
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

    # ── Build global (corpus-wide) boilerplate set ─────────────────────────────

    print("Building global boilerplate set...")
    global_bp, global_audit_rows = build_global_boilerplate_set(
        df, global_threshold, global_min_titles, BIN_PERIOD
    )
    g_freq_thresh = len(global_audit_rows)
    g_rescued     = g_freq_thresh - len(global_bp)
    print(f"  {g_freq_thresh:,} sentences exceeded the {global_threshold:.0%} global frequency threshold")
    print(f"  {g_rescued:,} rescued by title-diversity guard (< {global_min_titles} distinct titles)")
    print(f"  {len(global_bp):,} sentences flagged globally (apply to EVERY agency)\n")

    # Guard 2: print the full __GLOBAL__ flagged-sentence list for human
    # review. Anything role-specific here means GLOBAL_THRESHOLD is too low.
    flagged_global_audit = sorted(
        (r for r in global_audit_rows if r["flagged"]),
        key=lambda r: r["pct_of_bin"],
        reverse=True,
    )
    print(f"  === __GLOBAL__ flagged sentences ({len(flagged_global_audit)}) ===")
    for r in flagged_global_audit:
        print(f"    [{r['pct_of_bin']:>5.1f}% of {r['bin_size']:,} ads, "
              f"{r['n_titles']} titles, {r['quarter']}]  {r['sentence_original']}")
    print()

    audit_rows = audit_rows + global_audit_rows

    # ── Apply per-row (per-agency set ∪ global set) ────────────────────────────

    print("Applying boilerplate filter (per-agency ∪ global)...")

    # Header-path removals (op 2, _ABOUT_HEADER_RE whole-unit drop) leave no
    # trace anywhere else in the record — this counter is what makes that
    # removal path visible (09A diagnosis Step 3/B.3; per-row logging would be
    # more complete but isn't cheap at this row count, so this is aggregate).
    header_stats: dict[str, int] = {}

    def _clean(row) -> str | None:
        bp = agency_bp.get(row["agency_canonical"], frozenset()) | global_bp
        return clean_description(row["description"], bp, stats=header_stats)

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
    print(f"  Per-ad retention — median: {q[0.50]:.1%}  Q25: {q[0.25]:.1%}  Q75: {q[0.75]:.1%}")
    print(f"  Header-path removals (About-family whole-unit drops): {header_stats.get('about_header', 0):,} units")
    print(f"  Label-only empty-after-strip removals:                {header_stats.get('label_empty', 0):,} units\n")

    # ── Audit CSV (always written: stable name + dated archive) ────────────────
    #
    # Both files carry the same content (per-agency + __GLOBAL__ rows). The stable
    # name keeps diff workflows working; the dated archive (method version + build
    # date, UTC) preserves the audit for each method version. Local files only —
    # no R2 push (the audit is deterministic and regenerable, so it is not published).

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    audit_df = (
        pd.DataFrame(audit_rows)
        .sort_values(["agency_canonical", "pct_of_bin"], ascending=[True, False])
    )
    build_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    dated_audit = AUDIT_DIR / f"boilerplate_sentences-{BOILERPLATE_METHOD_VERSION}-{build_date}.csv"
    audit_df.to_csv(AUDIT_CSV, index=False)
    audit_df.to_csv(dated_audit, index=False)
    print(f"Audit CSV → {AUDIT_CSV}  ({len(audit_df):,} rows)")
    print(f"Audit CSV → {dated_audit}  ({len(audit_df):,} rows)")
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

    release_io.write_release(df, "07_clean_text")

    p_mb = os.path.getsize(PARQUET_PATH) / 1e6
    c_mb = os.path.getsize(CSV_PATH)     / 1e6
    print(f"Saved → {PARQUET_PATH}  ({p_mb:.1f} MB)")
    print(f"Saved → {CSV_PATH}  ({c_mb:.1f} MB)")
    print(f"\nDone. {len(df):,} rows, {len(df.columns)} columns.")

    # ── Validation ──────────────────────────────────────────────────────────────
    #
    # description_clean now exists, so this is where the boilerplate-residual check
    # (check 6) actually runs. A FAIL blocks publication: exit 1 so CI stops before
    # 08/push. The locally rewritten release files are already on disk, which is
    # acceptable — publication is what the gate protects.
    print()
    expectations = validation.load_expectations()
    findings = validation.validate_release(df, expectations)
    if validation.has_fail(findings):
        print("\nRelease BLOCKED: validation FAILed. Not publishing; CI stops before 08/push.")
        sys.exit(1)


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
        "--global-threshold", type=float, default=DEFAULT_GLOBAL_THRESHOLD, metavar="FRAC",
        help=f"Corpus-wide pass: fraction of ALL ads in a quarterly bin that must "
             f"contain a sentence to flag it as gazette-wide boilerplate "
             f"(default: {DEFAULT_GLOBAL_THRESHOLD})",
    )
    parser.add_argument(
        "--global-min-titles", type=int, default=DEFAULT_GLOBAL_MIN_TITLES, metavar="N",
        help=f"Corpus-wide pass: minimum distinct job titles a sentence must span in "
             f"its highest-frequency bin to be flagged globally "
             f"(default: {DEFAULT_GLOBAL_MIN_TITLES})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="compute and report without writing parquet/csv output files "
             "(audit CSVs are still written)",
    )
    args = parser.parse_args()

    if not 0 < args.threshold < 1:
        print("--threshold must be between 0 and 1 (exclusive)", file=sys.stderr)
        sys.exit(1)
    if args.min_titles < 1:
        print("--min-titles must be ≥ 1", file=sys.stderr)
        sys.exit(1)
    if not 0 < args.global_threshold < 1:
        print("--global-threshold must be between 0 and 1 (exclusive)", file=sys.stderr)
        sys.exit(1)
    if args.global_min_titles < 1:
        print("--global-min-titles must be ≥ 1", file=sys.stderr)
        sys.exit(1)

    run(
        threshold=args.threshold,
        min_title_diversity=args.min_titles,
        global_threshold=args.global_threshold,
        global_min_titles=args.global_min_titles,
        dry_run=args.dry_run,
    )
