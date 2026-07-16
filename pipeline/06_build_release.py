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


# ── posting_kind (spec 08) ──────────────────────────────────────────────
# Flags posting STRUCTURE, not seniority. Precedence: register >
# entry_program > bulk_round. Null = standard posting as far as the
# markers can tell — not a guarantee. Segmentation flag, NOT a count fix.

# ---------- register ----------
REGISTER_RE = re.compile(
    r'(?i)\bregisters?\b'
    r'|talent\s+(?:pool|pipeline)s?\b'
    r'|\beoi\b|expressions?\s+of\s+interest'
    r'|temporary\s+employment(?:\s+registers?|\s+\d{4}|\s*$)'   # "Temporary Employment 2022 - 2023"
    r'|merit\s+pool\s*$'
)
# Named registers as subject matter (roles about a register, not pool postings).
# Observed allowlist — audited row-by-row this session; re-harvest when extending markers.
REGISTER_EXCLUDE_RE = re.compile(
    r'(?i)federal\s+register\s+of\s+legislation'
    r'|national\s+cancer\s+screening\s+register'
    r'|business\s+register'                 # ABS Business Register Unit/Analyst
    r'|national\s+firearms\s+register'
    r'|self.?exclusion\s+register'
    r'|address\s+register'                  # ABS Address Register
    r'|security\s+register\s+assessor'      # IRAP assessor
    r'|register\s+development'
    r'|register\s+of\s+environmental\s+organisations'  # briefed FP class; 0 rows observed, guard kept
    r'|deputy\s+register\b'                 # "Deputy Register Human Source Capability" (Registrar typo)
)

# ---------- entry_program ----------
EP_SHIP_RE = re.compile(r'(?i)\b(?:apprenticeship|cadetship|traineeship)s?\b')
EP_ROLEWORD_RE = re.compile(r'(?i)\b(?:apprentice|cadet|trainee)s?\b')
EP_GRAD_STRUCT_RE = re.compile(
    r'(?i)\bgraduates?\b[\w\s,&/()\'\u2019\u2013\u2014-]*?\b(?:program(?:me)?s?|intakes?|pathways?|streams?|recruitment|cohorts?)\b'
    r'|\b(?:program(?:me)?s?|intakes?|pathways?|streams?)\b[\w\s,&/()\'\u2019\u2013\u2014-]*?\bgraduates?\b'
    r'|\bAGGP\b'
)
EP_OTHER_STRUCT_RE = re.compile(
    r'(?i)entry.?level\s+professional\s+program'    # ABARES ELPP; bare "entry level program(s)" is a team name
    r'|\bintern(?:ship)?s?\b'
    r'|school\s+leaver'
    # "development program" only when NOT immediately governed by an admin-role noun
    r'|development\s+program(?:me)?s?\b(?!\s*(?:\([\w &]*\)\s*)?(?:managers?|co-?ordinators?|administrators?|directors?|liaison|support\s+officers?|officers?|advis[eo]rs?|leads?)\b)'
    r'|\brecruits?\b'                               # BFORT family, Lateral Recruits ("recruitment" not matched)
    # named diversity/identified intakes — observed allowlist, re-harvest when new programs appear
    r'|first\s+nations\s+pathway\s+program'
    r'|directions\s+program'                        # AFP Directions Program
    r'|veterans?[\u2019\']?s?\s+employment\s+pathway'
)
# Admin-word set is deliberately narrow: adding officer/support/executive was tested and
# rejected (kills "Border Force Officer Recruit Trainee", "Trainee Administration Officer").
EP_ADMIN_WORDS  = r'(?:co-?ordinators?|managers?|mentors?|team\s+lead(?:er)?s?|(?:assistant\s+)?directors?)'
EP_MARKER_WORDS = r'(?:graduates?|apprentice(?:ship)?s?|cadet(?:ship)?s?|trainee(?:ship)?s?|intern(?:ship)?s?)'
EP_EXCLUDE_RE = re.compile(
    r'(?i)\b' + EP_ADMIN_WORDS + r'\b[\w\s,&/()\'\u2019\u2013\u2014|-]*\b' + EP_MARKER_WORDS + r'\b'
    r'|\b' + EP_MARKER_WORDS + r'\b[\w\s,&/()\'\u2019\u2013\u2014|-]*\b' + EP_ADMIN_WORDS + r'\b'
    r'|(?:ADF|Defence\s+Force|Army|Navy|Air\s+Force)\s+Cadets?\b'       # ADF Cadets youth organisation
    r'|Cadets?\s+(?:Directorate|Brigade|Branch|Governance|Engagement|Support)'
    r'|Apprenticeships\s+(?:Policy|Team)'                                # DEWR Australian Apprenticeships policy area
    r'|Recruit\s+Administration'                                         # Army 1 RTB admin role
    r'|Work\s+Experience\s+(?:Liaison|Co-?ordinator|Manager|Administration)'
    r'|^\s*(?:advis[eo]r|assistant\s+director|director|manager|co-?ordinator)s?\s*[,:\u2013\u2014-]\s*(?=.*development\s+program)'
)
CC_TRAINING_RE = re.compile(r'(?i)graduate|cadet\s+aps|trainee\s+aps|apprentice\s+aps|aboriginal\s+cadet')

def bare_graduate(title: str) -> bool:
    """True when the title's residue is just 'graduate(s)' after stripping decoration —
    asserting intake structure, not a role (e.g. 'NDIA APS4 Graduate (AM - First Nations)')."""
    if not isinstance(title, str) or not re.search(r'(?i)\bgraduates?\b', title):
        return False
    t = title
    t = re.sub(r'\([^)]*\)', ' ', t)
    t = re.sub(r'(?i)[-\u2013\u2014,:]\s*affirmative\s+measures?\b.*$', ' ', t)
    t = re.sub(r'(?i)[-\u2013\u2014,:]\s*(?:identified|indigenous\s+targeted).*$', ' ', t)
    t = re.sub(r'(?i)\b(?:aps|el)\s*\d(?:\s*[/;-]\s*(?:aps|el)?\s*\d)*\b', ' ', t)
    t = re.sub(r'(?i)\b(?:first\s+nations|indigenous|aboriginal(?:\s+and\s+torres\s+strait\s+islander)?)\b', ' ', t)
    t = re.sub(r'\b(?:19|20)\d{2}(?:\s*[-/]\s*\d{2,4})?\b|\b0\d\d\b', ' ', t)   # years incl. OCR-mangled ("026 First Nations Graduate")
    t = re.sub(r'\b[A-Z]{2,6}\b', ' ', t)     # ALL-CAPS acronyms only — NOT case-insensitive (a (?i) here strips ordinary words)
    t = re.sub(r'[^A-Za-z]+', ' ', t).strip().lower()
    return t in ("graduate", "graduates", "graduate aps", "aps graduate", "aps graduates")

# ---------- bulk_round ----------
BULK_TITLE_RE = re.compile(
    r'(?i)bulk\s+(?:round|recruitment|positions?|vacancies|intake)'
    r'|\(\s*bulk(?:\s+round)?\s*\)|[-\u2013\u2014]\s*bulk\s*$'
    r'|recruitment\s+round'
    r'|\b(?:multiple|several|various|numerous)\s+(?:[\w./&-]+\s+){0,3}(?:positions?|vacancies|roles?|opportunit(?:y|ies)|classifications?|levels?|jobs?)\b'
    r'|\(\s*(?:multiple|several|various)\s*\)|\b(?:multiple|several|various)\s*$'
    r'|^\s*(?:multiple|several|various)\s*(?:[-\u2013\u2014:]|$)'
    r'|\b\d+\s*x\s+\w|\bx\s*\d+\b|\(\s*\d+\s*x'                     # "2 x Anthropologists", "(x2)", "X3"
)
BULK_TITLE_EXCLUDE_RE = re.compile(
    r'(?i)bulk\s+(?:print|material|billing|cargo|fuel)'             # "Passport Bulk Print", "Bulk Material Analysis"
    r'|multiple\s+sclerosis'
)
BULK_DESC_RE = re.compile(   # applied to raw `description` — see spec text
    r'(?i)(?:seeking\s+to\s+fill|filling|we\s+have|there\s+are|recruit(?:ing)?(?:\s+for)?|now\s+recruiting)\s+multiple\s+(?:positions?|vacancies|roles?)'
    r'|multiple\s+(?:positions?|vacancies|roles?)\s+(?:are\s+)?(?:currently\s+)?available'
    # anchored: bare "bulk recruitment" also appears as past-reference boilerplate and in
    # recruitment-team duty statements; "through/via" anchors tested and removed (matched
    # "previously applied through the bulk recruitment process")
    r'|(?:conducting|undertaking|undergoing|running|commenced|part\s+of|this\s+is)\s+(?:a\s+|the\s+)?bulk\s+recruitment'
    r'|(?:fill\s+)?a\s+number\s+of\s+(?:positions?|vacancies|roles)\s+(?:are\s+available|available|at\s|across|within|in\b)'
    r'|fill\s+a\s+number\s+of\s+(?:positions?|vacancies|roles)'
    r'|various\s+(?:positions?|vacancies|roles)\s+(?:are\s+)?(?:currently\s+)?available'
    r'|(?:seeking\s+to\s+fill|fill(?:ing)?)\s+various\s+(?:positions?|vacancies|roles)'
)

def derive_posting_kind(df: pd.DataFrame) -> pd.Series:
    # .astype(object): pandas' default pyarrow-backed string dtype routes .str.contains
    # through RE2, which rejects the \uXXXX escapes used below (and lacks lookahead
    # support); object dtype restores the Python `re` engine. No regex logic changes.
    t  = df["job_title"].fillna("").astype(object)
    cc = df["classification_code"].fillna("").astype(object)
    d  = df["description"].fillna("").astype(object)          # RAW text, deliberately (stage 06; see spec)

    is_register = t.str.contains(REGISTER_RE) & ~t.str.contains(REGISTER_EXCLUDE_RE)

    ep_title = (t.str.contains(EP_SHIP_RE) | t.str.contains(EP_ROLEWORD_RE)
                | t.str.contains(EP_GRAD_STRUCT_RE) | t.str.contains(EP_OTHER_STRUCT_RE)
                | t.map(bare_graduate))
    # cc training classification is authoritative: promotes ambiguous titles AND overrides
    # the title-level admin excluder ("Apprentice Turf Manager", cc="Apprentice APS (Trades)").
    is_ep = (ep_title & ~t.str.contains(EP_EXCLUDE_RE)) | cc.str.contains(CC_TRAINING_RE)

    is_bulk = (t.str.contains(BULK_TITLE_RE) & ~t.str.contains(BULK_TITLE_EXCLUDE_RE)) | d.str.contains(BULK_DESC_RE)

    out = pd.Series(pd.NA, index=df.index, dtype="object")
    out[is_bulk]     = "bulk_round"
    out[is_ep]       = "entry_program"   # entry_program > bulk_round
    out[is_register] = "register"        # register > entry_program
    return out


def add_posting_kind(df: pd.DataFrame) -> pd.DataFrame:
    """Add posting_kind, placed immediately after posting_group_id."""
    df = df.copy()
    df["posting_kind"] = derive_posting_kind(df)

    cols = [c for c in df.columns if c != "posting_kind"]
    pgi = cols.index("posting_group_id")
    cols = cols[:pgi + 1] + ["posting_kind"] + cols[pgi + 1:]
    df = df[cols]

    counts = df["posting_kind"].value_counts(dropna=True)
    n_flagged = int(df["posting_kind"].notna().sum())
    print(f"posting_kind: {dict(counts)} ({n_flagged / len(df) * 100:.2f}% flagged)")
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

    # ── Posting-structure flag ──────────────────────────────────────────────────

    df = add_posting_kind(df)

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
