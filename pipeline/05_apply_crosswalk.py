"""
Apply agency crosswalk to produce gazette_vacancies_crosswalk.parquet.

Adds two columns to the normalised vacancy data:
  agency_canonical  — entity name as of the gazette date (date-aware MoG)
  agency_group      — stable grouping label for time-series analysis

Inputs:
  data/gazette_vacancies_normalised.parquet   (output of 03_normalise.py)
  data/agency_crosswalk.csv                   (committed reference file)
  04_build_crosswalk module                   (for live matching fallback on new strings)

Output:
  data/gazette_vacancies_crosswalk.parquet

Salary guards and location_normalised are computed upstream in 03_normalise.py and
are already present in the input file — this script does not recompute them.
"""

import sys
import re
import importlib.util
from pathlib import Path
import pandas as pd

_spec = importlib.util.spec_from_file_location(
    "build_crosswalk",
    Path(__file__).parent / "04_build_crosswalk.py",
)
bac = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bac)

# ── Helper functions ─────────────────────────────────────────────────────────

MOG_DATE = pd.Timestamp('2022-07-01')

# Broader than the same-named set in 04_build_crosswalk.py: includes single-entity
# portfolio strings (Social Services, Defence, …) where the division field may
# identify a specific sub-entity (e.g. NDIA inside Social Services portfolio).
DIVISION_REQUIRED = {
    # Multi-entity portfolio names: real entity is always in division or branch.
    'Agriculture, Water and the Environment',
    'Communications and the Arts',
    'Education, Skills and Employment',
    'Education',
    'Health',
    'Infrastructure, Transport, Regional',
    'Infrastructure, Transport, Regional Development and Communications',
    'Department of Infrastructure, Transport, Regional Development and Communications',
    'Climate Change, Energy, the',
    'Department of Infrastructure, Transport, Regional',
    # Single-entity portfolio names where division identifies the sub-entity.
    # These previously resolved via portfolio_only (short-circuiting division field).
    'Social Services',
    'Prime Minister and Cabinet',
    "Attorney-General's",
    'Treasury',
    'Finance',
    'Home Affairs',
    'Defence',
    'Foreign Affairs and Trade',
    'Employment and Workplace Relations',
    'Industry, Science, Energy and Resources',
    'Industry, Science and Resources',
    'Industry, Science, Energy and',           # truncated form (PDF column wrap)
    'Industry, Innovation and Science',        # pre-2020 portfolio name
    'Climate Change, Energy, the Environment and Water',
    'Infrastructure, Transport, Regional Development, Communications and the Arts',
    # Name-continuation: agency name wraps across a line, suffix is in division
    'Office of the Official Secretary to the',
    'National Commission for Aboriginal and Torres Strait',   # div = "Islander Children and Young People"
    'Veterans’ Affairs (part of the Defence',
    # SA portfolio prefix: gazette groups AIFS vacancies under the SA portfolio header;
    # division field identifies the real sub-entity (e.g. "Australian Institute of Family Studies ...").
    'Services Australia (part of the Social Services Portfolio)',
    'Services Australia (part of the Social',                  # truncated form (PDF column wrap)
}

# Fallback canonical for single-entity portfolio strings when division-field
# resolution finds no specific sub-entity (e.g. division = "Various").
PORTFOLIO_FALLBACK = {
    'Social Services':       'Department of Social Services',
    'Prime Minister and Cabinet': 'Department of the Prime Minister and Cabinet',
    "Attorney-General's":   "Attorney-General's Department",
    'Treasury':              'Department of the Treasury',
    'Finance':               'Department of Finance',
    'Home Affairs':          'Department of Home Affairs',
    'Defence':               'Department of Defence',
    'Foreign Affairs and Trade': 'Department of Foreign Affairs and Trade',
    'Employment and Workplace Relations': 'Department of Employment and Workplace Relations',
    'Industry, Science, Energy and Resources': 'Department of Industry, Science and Resources',
    'Industry, Science and Resources': 'Department of Industry, Science and Resources',
    'Industry, Science, Energy and': 'Department of Industry, Science and Resources',
    'Industry, Innovation and Science': 'Department of Industry, Science and Resources',
    'Climate Change, Energy, the Environment and Water':
        'Department of Climate Change, Energy, the Environment and Water',
    'Infrastructure, Transport, Regional Development, Communications and the Arts':
        'Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts',
    'Infrastructure, Transport, Regional Development and Communications':
        'Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts',
    'Services Australia (part of the Social Services Portfolio)': 'Services Australia',
    'Services Australia (part of the Social':                     'Services Australia',
}


def cw_lookup(raw_str, cw_dict):
    """Exact crosswalk dict lookup, then live match_canonical() fallback."""
    if not raw_str or not str(raw_str).strip():
        return (None, None)
    key = str(raw_str).strip()
    result = cw_dict.get(key)
    if result:
        return result
    canon, _ = bac.match_canonical(key)
    if canon:
        return (canon, bac.get_agency_group(canon))
    return (None, None)


def resolve_pdf_row(row, cw_dict):
    """Reconstruct agency from division+branch for .pdf-agency rows."""
    div = str(row['division']).strip() if pd.notna(row['division']) else ''
    bra = str(row['branch']).strip()   if pd.notna(row['branch'])   else ''
    recon = (div + ' ' + bra).strip() if bra else div
    c, g = cw_lookup(recon, cw_dict)
    if c is None and div:
        c, g = cw_lookup(div, cw_dict)
    return (c, g)


def resolve_div_required_row(row, cw_dict):
    """Agency is a multi-entity portfolio; real entity is in division or branch."""
    agency_str = str(row['agency']).strip() if pd.notna(row['agency']) else ''
    div = str(row['division']).strip() if pd.notna(row['division']) else ''
    bra = str(row['branch']).strip()   if pd.notna(row['branch'])   else ''
    recon = (div + ' ' + bra).strip() if bra else div
    c, g = cw_lookup(recon, cw_dict)
    if c is None and div:
        c, g = cw_lookup(div, cw_dict)
    if c is None and bra:
        c, g = cw_lookup(bra, cw_dict)                  # branch alone (Climate, CASA)
    if c is None and bra and div:
        c, g = cw_lookup(bra + ' ' + div, cw_dict)      # branch+div ("Dept Infra... Dev Comms...")
    if c is None and div:
        c, g = cw_lookup(agency_str + ' ' + div, cw_dict)  # name-continuation ("Office of Sec GG")
    # Branch-override: when branch alone resolves to a known sub-entity of the
    # agency identified by division, prefer branch. Fixes clusters where div
    # identifies the hosting body (e.g. APSC) but branch names the actual
    # hiring body (e.g. Parliamentary Workplace Support Service, IHACPA, DFSVC).
    #
    # Uses an explicit (hosting-agency → valid sub-entities) whitelist rather than
    # a general "branch wins" rule, to avoid false positives where generic internal
    # unit labels (e.g. "ICT Services" → DoD, "Communications Infrastructure" →
    # DITRDCA) appear in branch and override a correctly-resolved division canonical.
    BRANCH_OVERRIDE_PAIRS = {
        'Australian Public Service Commission': {
            'Parliamentary Workplace Support Service',
        },
        'Department of Health, Disability and Ageing': {
            'Independent Health and Aged Care Pricing Authority',
            'National Mental Health Commission',
            'Australian Centre for Disease Control',
            'Independent Hospital Pricing Authority',
        },
        'Department of Health and Aged Care': {
            'Independent Health and Aged Care Pricing Authority',
            'National Mental Health Commission',
            'Australian Centre for Disease Control',
            'Independent Hospital Pricing Authority',
        },
        'Department of Social Services': {
            'Domestic, Family and Sexual Violence Commission',
        },
        'Australian Communications and Media Authority': {
            'Office of the eSafety Commissioner',
        },
        'Department of Defence': {
            'Australian Naval Nuclear Power Safety Regulator',
            'Australian Submarine Agency',
        },
        'Department of the Prime Minister and Cabinet': {
            'Parliamentary Workplace Support Service',
        },
    }
    if bra:
        b_canon, b_method = bac.match_canonical(bra)
        if (b_canon is not None
                and b_method not in {'portfolio_only', 'uncertain', 'division_required'}
                and b_canon != c):
            if c is None or (c in BRANCH_OVERRIDE_PAIRS
                             and b_canon in BRANCH_OVERRIDE_PAIRS[c]):
                c, g = b_canon, bac.get_agency_group(b_canon)
    # Date-aware fallback for pre-MoG DAWE rows with no sub-entity in division
    if c is None and row['agency'] == 'Agriculture, Water and the Environment':
        if row['gazette_date'] < MOG_DATE:
            c = 'Department of Agriculture, Water and the Environment'
            g = bac.get_agency_group(c)
    # Generic fallback for single-entity portfolio strings where division is
    # generic (e.g. "Various") and identifies no specific sub-entity
    if c is None and agency_str in PORTFOLIO_FALLBACK:
        c = PORTFOLIO_FALLBACK[agency_str]
        g = bac.get_agency_group(c)
    return (c, g)


def run():
    # ── Load data ────────────────────────────────────────────────────────────────

    df = pd.read_parquet("data/gazette_vacancies_normalised.parquet")
    df['gazette_date'] = pd.to_datetime(df['gazette_date'])

    cw = pd.read_csv("data/agency_crosswalk.csv")
    cw_dict = {
        row['agency_raw']: (
            row['agency_canonical'] if pd.notna(row['agency_canonical']) else None,
            row['agency_group']     if pd.notna(row['agency_group'])     else None,
        )
        for _, row in cw.iterrows()
    }

    # Raw agency strings that resolved via portfolio_only in the crosswalk.
    # Used in Pass 1 to trigger division-field fallback: when "Social Services"
    # resolves to DSS but division says "National Disability Insurance Agency",
    # the sub-entity wins.
    PORTFOLIO_ONLY_RAW = {
        row['agency_raw']
        for _, row in cw.iterrows()
        if pd.notna(row.get('match_method', '')) and row['match_method'] == 'portfolio_only'
    }

    # ── Apply ────────────────────────────────────────────────────────────────────

    IS_PDF      = df['agency'].str.contains(r'\.pdf$', na=False, regex=True)
    IS_DIV_REQ  = df['agency'].isin(DIVISION_REQUIRED)
    IS_NORMAL   = ~IS_PDF & ~IS_DIV_REQ

    print(f"Rows:  {len(df):,}")
    print(f"  normal:          {IS_NORMAL.sum():,}")
    print(f"  .pdf-agency:     {IS_PDF.sum():,}")
    print(f"  division-req:    {IS_DIV_REQ.sum():,}")

    canonical  = [None] * len(df)
    group      = [None] * len(df)

    # Pass 1 — normal rows: direct crosswalk lookup with portfolio-parent refinement.
    # When the agency field resolves to a portfolio-level entry (via portfolio_only
    # match in the crosswalk) AND division is non-null, try resolve_div_required_row()
    # to identify the specific sub-entity (e.g. "Social Services" → DSS refines to
    # NDIA when division contains "National Disability Insurance Agency").
    for idx, row in df[IS_NORMAL].iterrows():
        pos = df.index.get_loc(idx)
        agency_val = row['agency']
        c, g = cw_lookup(agency_val, cw_dict)
        if (c is not None
                and str(agency_val).strip() in PORTFOLIO_ONLY_RAW
                and pd.notna(row['division'])
                and str(row['division']).strip()):
            c2, g2 = resolve_div_required_row(row, cw_dict)
            if c2 is not None and c2 != c:
                c, g = c2, g2
        canonical[pos] = c
        group[pos]     = g

    # Pass 2 — .pdf rows: reconstruct from division+branch
    for idx, row in df[IS_PDF].iterrows():
        pos = df.index.get_loc(idx)
        c, g = resolve_pdf_row(row, cw_dict)
        canonical[pos] = c
        group[pos]     = g

    # Pass 3 — division-required rows
    for idx, row in df[IS_DIV_REQ].iterrows():
        pos = df.index.get_loc(idx)
        c, g = resolve_div_required_row(row, cw_dict)
        canonical[pos] = c
        group[pos]     = g

    df['agency_canonical'] = canonical
    df['agency_group']     = group

    # ── Canonical remap: collapse stale pre-MoG names to current canonical ────────
    #
    # Prefix-matching inside match_canonical() resolves "Department of Industry,
    # Science, Energy and Resources <division>" strings to DISER, and older
    # Infrastructure names to their own canonical — because those names remain in
    # CANONICAL_AGENCIES for correct sub-entity prefix stripping.  A
    # post-resolution remap here consolidates them to the current canonical
    # without disrupting the resolution logic. Belt-and-braces with
    # CANONICAL_FORWARD_MAP in 04 (manual/portfolio matches bypass that map, so
    # this apply-time remap is the safety net that catches them).
    CANONICAL_REMAP = {
        'Department of Industry, Science, Energy and Resources':
            'Department of Industry, Science and Resources',
        'Department of Infrastructure, Transport, Regional Development and Communications':
            'Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts',
        'Department of Infrastructure, Transport, Regional Development, Communications and the Arts':
            'Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts',
    }
    remapped = df['agency_canonical'].replace(CANONICAL_REMAP)
    n_remapped = (df['agency_canonical'] != remapped).sum()
    if n_remapped:
        print(f"Canonical remap: {n_remapped:,} rows updated (DISER→DISR / DITRDC→DITRDCSA / DITRDCA→DITRDCSA)")
    df['agency_canonical'] = remapped
    df['agency_group'] = df['agency_canonical'].map(
        lambda c: bac.get_agency_group(c) if pd.notna(c) else None
    )

    # ── Row-level reattribution: single-vacancy misattributions ───────────────────
    #
    # Two rows are misattributed under the documented conventions and cannot be
    # fixed via MANUAL_OVERRIDES (their raw agency strings are the generic keys
    # for thousands of correctly-attributed rows). They are corrected here by
    # vacancy_no:
    #   VN-0704814 — raw agency APSC, division "Parliamentary Workplace Support
    #     Service"; names PWSS in division rather than branch, so no pass reaches
    #     it. Belongs to PWSS under the documented sub-entity exception.
    #   VN-0700097 — raw agency printed as TGA in the source PDF, division
    #     "Independent Hospital Pricing Authority"; a gazette-source error (fields
    #     parse cleanly). The role is IHPA's.
    #   VN-0771081 — raw agency "Australian Submarine Agency", division/branch
    #     "Australian Naval Nuclear Power Safety Regulator" / "Director General",
    #     job "ANNPSR Graduate Program" (2 rows). An ANNPSR posting advertised via
    #     ASA while ANNPSR stands up. ANNPSR is a first-class canonical agency
    #     here, so the role is attributed to it rather than left under ASA.
    # Correcting these removes the mismatched pairs from the division-mismatch
    # scan, which is why the corresponding ALLOWED_DIVISION_MISMATCH entries were
    # deleted (never added, for VN-0771081). These change agency_canonical/
    # agency_group only, not vacancy identity.
    VACANCY_NO_OVERRIDES = {
        "VN-0704814": "Parliamentary Workplace Support Service",
        "VN-0700097": "Independent Hospital Pricing Authority",
        "VN-0771081": "Australian Naval Nuclear Power Safety Regulator",
    }
    for vn, canon in VACANCY_NO_OVERRIDES.items():
        mask = df['vacancy_no'] == vn
        n = int(mask.sum())
        if n:
            df.loc[mask, 'agency_canonical'] = canon
            df.loc[mask, 'agency_group']     = bac.get_agency_group(canon)
            print(f"Row override: {vn} → {canon} ({n} row(s))")

    # ── Antarctic + Environment-and-Energy pre-MoG date remap ─────────────────────
    #
    # MANUAL_OVERRIDES statically maps the Australian Antarctic Division strings
    # (and the legacy "Department of the Environment and Energy" string) to DCCEEW
    # regardless of date. But environment/climate functions lived in DAWE before
    # the 2022-07 split; DCCEEW should be from-zero at 2022-07, with no spurious
    # pre-2022 trickle. So for gazette_date < MOG_DATE we remap these rows to
    # DAWE / "Agriculture department". Post-2022-07 rows keep DCCEEW (the static
    # override is correct for them). This is the single date-aware point, matching
    # the pre-MoG DAWE fallback pattern in resolve_div_required_row() above; the
    # MANUAL_OVERRIDES entries themselves are left unchanged.
    #
    # Keying: the Antarctic rows key on the raw `agency` string (the three
    # MANUAL_OVERRIDES keys). The DEE rows do NOT — in the surviving (deduped)
    # printings the raw agency is "Environment and Energy" and the string
    # "Department of the Environment and Energy" sits in `division`, so those rows
    # are matched on the division string as well. (The remap keys on both columns
    # for that reason.)
    #
    # The `== DCCEEW` guard on the DEE branch is essential and deliberate: one
    # DEE-division row (VN-0672901) legitimately resolves to Clean Energy Regulator
    # — a distinct agency whose division text merely starts with the DEE string —
    # and must NOT be dragged into DAWE. The guard also keeps the remap targeted at
    # the exact strings this fix concerns rather than "every pre-2022 DCCEEW row",
    # so the downstream validation guard (zero pre-2022 DCCEEW) stays a real
    # regression check, not a tautology that this remap could trivially satisfy.
    DCCEEW_NAME = 'Department of Climate Change, Energy, the Environment and Water'
    DAWE_NAME   = 'Department of Agriculture, Water and the Environment'
    ANTARCTIC_RAW = {
        'Australian Antarctic',
        'Australian Antarctic Division',
        'Australian Antarctic Division Australian Antarctic Division Various Various',
    }
    DEE_RAW = 'Department of the Environment and Energy'

    def _strip(s):
        return str(s).strip() if pd.notna(s) else ''

    agency_key = df['agency'].map(_strip)
    div_key    = df['division'].map(_strip)
    pre_mog    = df['gazette_date'] < MOG_DATE
    is_antarctic = agency_key.isin(ANTARCTIC_RAW)
    is_dee = ((agency_key == DEE_RAW) | (div_key == DEE_RAW)) & (df['agency_canonical'] == DCCEEW_NAME)
    remap_mask = pre_mog & (is_antarctic | is_dee)

    n_remap = int(remap_mask.sum())
    if n_remap:
        df.loc[remap_mask, 'agency_canonical'] = DAWE_NAME
        df.loc[remap_mask, 'agency_group']     = bac.get_agency_group(DAWE_NAME)
        print(f"Antarctic/DEE pre-MoG remap: {n_remap:,} rows → "
              f"DAWE / Agriculture department "
              f"({int((pre_mog & is_antarctic).sum()):,} Antarctic + "
              f"{int((pre_mog & is_dee).sum()):,} Environment-and-Energy)")

    # ── ps_act_employer flag ──────────────────────────────────────────────────────
    #
    # True where the employing entity engages staff under the Public Service Act
    # 1999; False for own-Act employers, Commonwealth companies and Parliamentary
    # Service Act departments (bac.NON_PS_ACT_EMPLOYERS). Nullable boolean: null
    # only where agency_canonical is null. Derived last, so it reflects every
    # canonical/group correction above.
    df['ps_act_employer'] = df['agency_canonical'].map(
        lambda c: None if pd.isna(c) else c not in bac.NON_PS_ACT_EMPLOYERS
    ).astype('boolean')
    n_false = int((df['ps_act_employer'] == False).sum())
    n_null_flag = int(df['ps_act_employer'].isna().sum())
    print(f"ps_act_employer: {n_false:,} False (non-PS-Act), "
          f"{n_null_flag:,} null (agency_canonical null)")

    # ── Coverage report ──────────────────────────────────────────────────────────

    n_total   = len(df)
    n_mapped  = df['agency_canonical'].notna().sum()
    n_null    = n_total - n_mapped

    print(f"\n=== COVERAGE ===")
    print(f"Mapped:   {n_mapped:,} / {n_total:,}  ({n_mapped/n_total*100:.1f}%)")
    print(f"NULL:     {n_null}")

    if n_null > 0:
        print("\nNULL rows:")
        print(df[df['agency_canonical'].isna()][
            ['gazette_date','agency','division','branch','job_title']
        ].to_string())

    # ── Top 30 ───────────────────────────────────────────────────────────────────

    print("\n=== TOP 30 CANONICAL AGENCIES ===")
    vc = df['agency_canonical'].value_counts()
    for rank, (agency, count) in enumerate(vc.head(30).items(), 1):
        pct = count / n_total * 100
        print(f"  #{rank:2d}  {count:6,}  ({pct:4.1f}%)  {agency}")

    # ── Agencies with <5 rows ─────────────────────────────────────────────────────

    rare = vc[vc < 5]
    print(f"\n=== CANONICAL AGENCIES WITH <5 ROWS ({len(rare)}) ===")
    for agency, cnt in rare.sort_values().items():
        print(f"  {cnt}  {agency}")

    print(f"\nDistinct canonical agencies: {df['agency_canonical'].nunique()}")

    # ── Post-resolution cleanup ───────────────────────────────────────────────────

    # m1: salary text in job_title (PDF extraction artifact — salary string parsed as title)
    salary_in_title = df['job_title'].str.match(r'^\$\d', na=False)
    n = salary_in_title.sum()
    print(f"\nm1 (salary text in job_title): {n} rows → job_title = null")
    df.loc[salary_in_title, 'job_title'] = None

    # m4: description = "To Apply" (incomplete PDF extraction)
    to_apply = df['description'].str.strip() == 'To Apply'
    n = to_apply.sum()
    print(f"m4 (description = 'To Apply'): {n} rows → description = null")
    df.loc[to_apply, 'description'] = None

    # Null consistency: placeholder strings → null across 6 columns
    PLACEHOLDERS = {'-', 'N/A', 'n/a', '.', ''}
    for col in ['division', 'branch', 'salary', 'position_number', 'office_arrangement_details', 'closing_date_raw']:
        mask = df[col].isin(PLACEHOLDERS)
        n = mask.sum()
        if n:
            print(f"Placeholder nulling ({col}): {n} rows")
        df.loc[mask, col] = None

    # Classification field-bleed artifacts: singleton reference-number values that
    # slipped through the C3 bleed guard (position numbers misread as grade codes)
    CLASSIFICATION_ARTIFACTS = {
        '1270, 1670', '21-HERDIV-11473', '21-LADIV-8110', '22-LADIV-13772',
        '23-RLSDIV-18270', '24-LDIV-26971', 'CA - 1134', 'CS - 1122', 'CS - 1178',
        'CS - 1179', 'CS - 2044', 'CS - 2054', 'CS - 2081', 'CS - 2141',
        'DSTG/00288/21', 'NSW', 'OPS - 1622', 'OPS - 2220', 'RP&S - 1188',
        'RP&S - 2123', 'RPS - 1252', 'RPS - 1588', 'SC - 1255',
    }
    artifact_mask = df['classification_clean'].isin(CLASSIFICATION_ARTIFACTS)
    n = artifact_mask.sum()
    print(f"Classification artifacts: {n} rows → classification_clean = null, classification = null")
    df.loc[artifact_mask, 'classification_clean'] = None
    df.loc[artifact_mask, 'classification'] = None

    # ── Save ──────────────────────────────────────────────────────────────────────

    OUTPUT = "data/gazette_vacancies_crosswalk.parquet"
    df.to_parquet(OUTPUT, index=False)
    print(f"\nSaved → {OUTPUT}")
    print(f"  rows: {len(df):,}  |  cols: {len(df.columns)}")


if __name__ == '__main__':
    run()
