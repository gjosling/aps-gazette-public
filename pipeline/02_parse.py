#!/usr/bin/env python3
"""
02_parse.py — APS PS Gazette PDF parser with batch mode.

Extracts structured fields from gazette PDFs and writes results to
data/gazette_vacancies_raw.parquet.

Usage:
    # Parse all pending PDFs from data/manifest.csv (typical CI run):
    python pipeline/02_parse.py batch

    # Limit to first N unprocessed PDFs (incremental / memory-bounded):
    python pipeline/02_parse.py batch --batch-size 50

    # Parse a single PDF to CSV (original interface):
    python pipeline/02_parse.py single <input.pdf> <gazette_id> <date YYYY-MM-DD> <output.csv>

Batch mode reads data/manifest.csv (produced by 01_download.py), skips files
whose latest parse-log entry is `parsed`, and appends new records to
data/gazette_vacancies_raw.parquet. Files logged `missing_pdf` or `error` are
retried on every run (last-status-wins in the append-only log), so a transient
failure is no longer a permanent skip.

Safe to interrupt: the raw parquet and parse_log.csv are flushed together every
100 successfully parsed PDFs (FLUSH_EVERY) and once at end of run. The parquet
is written first (atomically, via a .tmp file + os.replace), and the batch's
pending log rows are appended only after to_parquet returns. An interrupted run
therefore loses at most the last unflushed batch of both artefacts together, and
the next run redoes exactly those PDFs.

Pass --full to ignore the parse log and reparse every PDF in data/pdfs/,
replacing any existing parquet. Use this after parser changes to rebuild from
the archived PDFs (see r2_sync.py pull-pdfs).

Fields extracted (raw — no salary parsing or normalisation):
    gazette_id, gazette_date, gazette_type, vacancy_no, agency, division, branch,
    closing_date_raw, job_title, job_type, location, salary, classification,
    position_number, office_arrangement, office_arrangement_details,
    agency_website, description, raw_vn_count

Known limitations:
    - Agency names that wrap across two lines with no blank-line separation
      from the division will be split: agency gets line 1, division gets line 2.
      Resolved downstream by the agency crosswalk (04_build_crosswalk.py /
      05_apply_crosswalk.py).
    - ~1% of notices link to external recruitment systems with no description
      text in the PDF. These will have description = None.
    - description contains the full free-text body including boilerplate
      ("About the agency", "Notes", etc.). The Position Contact section
      (officer name, phone, email) is excluded — the parser stops at the
      "Position Contact" sentinel.
"""

import argparse
import csv
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PARSE_TYPES  = {"weekly_vacancy", "weekly_old", "daily"}
MANIFEST_PATH  = Path("data/manifest.csv")
PARSE_LOG_PATH = Path("data/parse_log.csv")
PARQUET_PATH   = Path("data/gazette_vacancies_raw.parquet")
PDF_DIR        = Path("data/pdfs")

# Flush the raw parquet and parse log together every this-many successfully
# parsed PDFs (see parse_gazette_batch). Bounds the crash-loss window.
FLUSH_EVERY = 100

PARSE_LOG_FIELDS = [
    "filename", "gazette_date", "gazette_id", "gazette_type",
    "record_count", "vn_count", "status",
]

def strip_page_headers(text):
    """
    Remove gazette page headers while preserving surrounding content.

    The page header pattern is always:
        \\f
        Australian Public Service Gazette
        PS{N} Weekly Gazette ... .pdf
        [blank line OR content line]
        Page N of N
        [blank line]

    Two-pass approach handles all observed variants.
    """
    # Pass 1a: two-line header (title on own line, PS line on next).
    # Modern PDFs have a .pdf suffix; older PDFs (~2021) do not.
    text = re.sub(
        r'\x0cAustralian Public Service Gazette\nPS\d+ [^\n]+\n',
        '',
        text
    )
    # Pass 1b: one-line header (older PDFs where title + PS number are on the same line)
    text = re.sub(
        r'\x0cAustralian Public Service Gazette PS\d+ [^\n]+\n',
        '',
        text
    )
    # Pass 2: remove "Page N of N" line (may appear with or without leading blank)
    text = re.sub(r'^\nPage \d+ of \d+\n', '\n', text, flags=re.MULTILINE)
    text = re.sub(r'^Page \d+ of \d+\n', '', text, flags=re.MULTILINE)
    # Remove any stray form feeds
    text = text.replace('\x0c', '')
    # Strip orphaned "YYYY.pdf" lines left when a long filename wraps mid-line
    text = re.sub(r'^\d{4}\.pdf\n', '', text, flags=re.MULTILINE)
    # Safety net: bare gazette title lines that survived (no \x0c present).
    # Two-line variant (title on own line, PS on next) — common in 2021 PDFs.
    text = re.sub(
        r'^Australian Public Service Gazette\nPS\d+ [^\n]+\n',
        '',
        text, flags=re.MULTILINE
    )
    # One-line variant (title + PS number on same line).
    text = re.sub(
        r'^Australian Public Service Gazette PS\d+ [^\n]+\n',
        '',
        text, flags=re.MULTILINE
    )

    # Fix "Job Description" label split by a page break:
    # "Job\n<url>\nDescription\n" -> "Job Description\n\n<url>\n\n"
    text = re.sub(
        r'\bJob\n(https?://\S+)\nDescription\n',
        r'Job Description\n\n\1\n\n',
        text
    )

    # Normalise all split field labels — these appear in the daily gazette
    # format where the narrow label column causes multi-word labels to wrap.
    # Must be done after page header stripping so we don't accidentally join
    # content lines.
    SPLIT_LABELS = [
        (r'\bJob\nTitle\b',                      'Job Title'),
        (r'\bJob\nDescription\b',                'Job Description'),
        (r'\bFuture\nMerit\nLocations\b',        'Future Merit\nLocations'),
        (r'\bOffice\nArrangement\nDetails\b',     'Office Arrangement\nDetails'),
        (r'\bOffice\nArrangement\b',             'Office Arrangement'),
        (r'\bPosition\nNumber\b',                'Position Number'),
        (r'\bAgency\nWebsite\b',                 'Agency Website'),
    ]
    for pattern, replacement in SPLIT_LABELS:
        text = re.sub(pattern, replacement, text)

    return text

def split_notices(text):
    """
    Split full gazette text into individual notice blocks.

    In daily gazettes, the agency/division/branch block for a notice appears
    BEFORE the 'Vacancy VN-' line (at the end of the preceding block). We
    prepend that trailing context to each notice so the header parser can
    find it in the expected position.
    """
    BOILERPLATE_TAIL = (
        'applicants found suitable may be offered similar employment '
        'opportunities by other Australian Public Service agencies'
    )

    parts = re.split(r'(?=^Vacancy VN-)', text, flags=re.MULTILINE)

    notices = []
    for i, part in enumerate(parts):
        if not part.strip().startswith('Vacancy VN-'):
            continue

        # Check if this notice has an agency in its own header
        lines = part.strip().split('\n')
        cd_idx = next((j for j, l in enumerate(lines) if l.startswith('Closing Date:')), None)
        has_agency_in_header = cd_idx is not None and cd_idx > 1 and any(
            l.strip() and not l.strip().startswith('http')
            for l in lines[1:cd_idx]
        )

        if not has_agency_in_header and i > 0:
            # Look at the tail of the preceding block for agency context.
            # Agency lines appear after the boilerplate footer.
            prev = parts[i - 1]
            bp_idx = prev.rfind(BOILERPLATE_TAIL)
            if bp_idx >= 0:
                tail = prev[bp_idx + len(BOILERPLATE_TAIL):].strip()
                tail = re.sub(r'^\d{4}\.pdf\n', '', tail, flags=re.MULTILINE).strip()
                if tail:
                    first_line = lines[0]
                    part = part.replace(
                        first_line + '\n',
                        first_line + '\n' + tail + '\n',
                        1
                    )

        notices.append(part)

    return notices

FIELD_LABELS = [
    'Job Title', 'Job Type', 'Location', 'Salary',
    'Future Merit\nLocations', 'Office Arrangement', 'Office Arrangement\nDetails',
    'Classification', 'Position Number', 'Agency Website', 'Job Description'
]

def extract_field(text, label):
    """
    Extract the value of a labelled field.
    Fields appear as: LABEL\\n\\nVALUE\\n\\n<next label>
    Multi-line values are joined with spaces.
    """
    next_labels = '|'.join(re.escape(l) for l in FIELD_LABELS)
    pattern = rf'^{re.escape(label)}\n\n(.*?)(?=\n\n(?:{next_labels})\n|\nThis notice is part|\nClosing Date:|\Z)'
    m = re.search(pattern, text, re.MULTILINE | re.DOTALL)
    if not m:
        return None
    val = m.group(1).strip()
    return ' '.join(val.split('\n')).strip() or None

# URL or email on its own line — marks the end of structured fields
# and start of description text.
# Bare-domain URLs (e.g. moadoph.gov.au/path, dese.nga.net.au/cp) are included
# because some agencies omit the https:// scheme in their gazette submissions.
LINK_RE = re.compile(
    r'^(?:https?://|www\.|\S+@\S+\.\S+|[a-zA-Z0-9][\w.-]*\.[a-z]{2,}/)\S*\n',
    re.MULTILINE,
)

# Detects field-bleed: a field value that starts with a known label followed by
# two spaces (the PDF column separator), indicating the label was misread as part
# of the preceding field's value.
_BLEED_RE = re.compile(
    r'^(?:' + '|'.join(re.escape(l.replace('\n', ' ')) for l in FIELD_LABELS) + r')  '
)

def extract_description(text):
    """
    Extract free-text job description.

    The description follows the Job Description label and a link (URL or email).
    Between the label and the link there may be arbitrary section headings
    (e.g. "Duties", "About X", "The Role") — these are skipped.

    The Position Contact section (officer name, phone, email) is excluded:
    the parser stops at the "Position Contact" sentinel.
    """
    jd_match = re.search(r'^Job Description\n', text, re.MULTILINE)
    if not jd_match:
        return None

    after_label = text[jd_match.end():]
    link_match = LINK_RE.search(after_label)
    if link_match:
        after_link = after_label[link_match.end():].lstrip('\n')
    else:
        # No URL anchor — description starts immediately (e.g. AHL format).
        after_link = after_label.lstrip('\n')

    for sentinel in [
        '\nTo Apply\n', '\nPosition Contact\n',
        '\nAgency Recruitment Site\n', '\nThis notice is part'
    ]:
        idx = after_link.find(sentinel)
        if idx >= 0:
            after_link = after_link[:idx]

    description = after_link.strip()
    return ' '.join(description.split()) if description else None

FOOTER_BOILERPLATE = (
    r'applicants found suitable may be offered similar employment opportunities '
    r'by other Australian Public Service agencies\n\n'
)

def extract_agency_from_footer(text):
    """
    Fallback: extract agency name from the notice footer.

    When the agency header lands on the previous PDF page, the agency name
    reliably appears in the footer after the standard boilerplate text.
    """
    m = re.search(FOOTER_BOILERPLATE + r'(.+?)(?:\n\n|$)', text, re.DOTALL)
    if not m:
        return None, None
    block = m.group(1).strip()
    lines = [l.strip() for l in block.split('\n') if l.strip()]
    return (lines[0] if lines else None), (lines[1] if len(lines) > 1 else None)

def parse_header(text):
    """
    Parse agency, division, branch from the header block before Job Title.

    The header block contains consecutive lines of agency/division/branch.
    If blank lines separate the lines, each blank-separated segment is a
    distinct field. If no blank lines, treat lines positionally:
        line 1 = agency, line 2 = division, line 3 = branch.

    Note: agency names that wrap to two lines are indistinguishable from
    agency + division when there are no blank separators. These will appear
    split. Use an agency crosswalk table to normalise downstream.
    """
    jt_idx = text.find('\nJob Title\n')
    header_block = text[:jt_idx] if jt_idx > 0 else text[:500]

    header_block = re.sub(r'^Vacancy VN-\d+\n', '', header_block)
    header_block = re.sub(r'^Closing Date:.*\n', '', header_block, flags=re.MULTILINE)

    # Remove URL/email lines
    header_block = re.sub(
        r'^(?:https?://|www\.|\S+@\S+\.\S+)\S*\n', '', header_block, flags=re.MULTILINE
    )

    segments = []
    current = []
    for line in header_block.split('\n'):
        stripped = line.strip()
        if stripped == '':
            if current:
                segments.append(current)
                current = []
        else:
            current.append(stripped)
    if current:
        segments.append(current)

    if not segments:
        return None, None, None

    if len(segments) == 1:
        lines = segments[0]
        agency   = lines[0] if len(lines) >= 1 else None
        division = lines[1] if len(lines) >= 2 else None
        branch   = lines[2] if len(lines) >= 3 else None
    else:
        agency   = ' '.join(segments[0])
        division = ' '.join(segments[1]) if len(segments) >= 2 else None
        branch   = ' '.join(segments[2]) if len(segments) >= 3 else None

    if agency and division and agency == division:
        division = branch
        branch   = None

    if division and re.search(r'Portfolio\)$', division.strip()):
        division = branch
        branch   = None

    return agency, division, branch

def parse_notice(text, gazette_id, gazette_date):
    """Parse a single notice block into a dict of fields."""
    lines = text.strip().split('\n')

    vn_match = re.match(r'Vacancy (VN-\d+)', lines[0])
    vacancy_no = vn_match.group(1) if vn_match else None

    cd_line = next((l for l in lines if l.startswith('Closing Date:')), None)
    closing_date_raw = cd_line.replace('Closing Date:', '').strip() if cd_line else None

    agency, division, branch = parse_header(text)
    if not agency:
        agency, division = extract_agency_from_footer(text)
        branch = None

    text_clean = re.sub(
        r'\nThis notice is part of the electronic.*$', '', text, flags=re.DOTALL
    ).strip()

    # Option B: agency URL and "Job Description" label on the same line.
    # pdftotext collapses the two-column row when there is no blank-line separator.
    # "https://asic.gov.au/  Job Description\n" → "https://asic.gov.au/\n\nJob Description\n"
    text_clean = re.sub(
        r'(https?://\S+)([ \t]{2,})(Job Description\n)',
        r'\1\n\n\3',
        text_clean,
    )

    # Mode C (symmetric to Option B): "Job Description" label and ATS URL on the same line.
    # "Job Description https://ats.example.com/\n" → "Job Description\n\nhttps://ats.example.com/\n"
    text_clean = re.sub(
        r'^(Job Description)([ \t]+)(https?://\S+\n)',
        r'\1\n\n\3',
        text_clean,
        flags=re.MULTILINE,
    )

    result = {
        'gazette_id':         gazette_id,
        'gazette_date':       gazette_date,
        'vacancy_no':         vacancy_no,
        'agency':             agency,
        'division':           division,
        'branch':             branch,
        'closing_date_raw':   closing_date_raw,
        'job_title':          extract_field(text_clean, 'Job Title'),
        'job_type':           extract_field(text_clean, 'Job Type'),
        'location':           extract_field(text_clean, 'Location'),
        'salary':             extract_field(text_clean, 'Salary'),
        'classification':     extract_field(text_clean, 'Classification'),
        'position_number':    extract_field(text_clean, 'Position Number'),
        'office_arrangement':         extract_field(text_clean, 'Office Arrangement'),
        'office_arrangement_details': extract_field(text_clean, 'Office Arrangement\nDetails'),
        'agency_website':             extract_field(text_clean, 'Agency Website'),
        'description':        extract_description(text_clean),
    }

    LABEL_NAMES = {l.replace('\n', ' ') for l in FIELD_LABELS}
    for key in ('classification', 'position_number', 'office_arrangement',
                'office_arrangement_details', 'agency_website'):
        val = result.get(key)
        if val and val in LABEL_NAMES:
            result[key] = None

    if result.get('classification') and re.match(r'^\d+$', result['classification']):
        result['classification'] = None

    for _key in ('location', 'classification', 'position_number',
                 'office_arrangement', 'office_arrangement_details', 'agency_website'):
        _val = result.get(_key)
        if _val and _BLEED_RE.match(_val):
            result[_key] = None

    if result.get('job_type') and not re.search(
        r'(?i)\b(?:ongoing|non.?ongoing|irregular|intermittent)\b',
        result['job_type']
    ):
        result['job_type'] = None

    if result.get('office_arrangement') and re.match(
        r'^(?:APS|EL|SES)\s*\d|^Executive Level|^Senior Executive Service',
        result['office_arrangement']
    ):
        result['office_arrangement'] = None

    if result.get('position_number') and re.match(r'https?://', result['position_number']):
        result['position_number'] = None

    if result.get('agency_website') and not re.match(r'https?://', result['agency_website']):
        result['agency_website'] = None

    return result

def parse_gazette(pdf_path, gazette_id, gazette_date):
    """Parse a gazette PDF and return (records, raw_vn_count).

    raw_vn_count is the number of 'Vacancy VN-' markers in the stripped text.
    Comparing it to len(records) detects notices silently dropped by the parser.
    """
    result = subprocess.run(
        ['pdftotext', pdf_path, '-'],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext failed: {result.stderr}")

    text = strip_page_headers(result.stdout)
    raw_vn_count = len(re.findall(r'^Vacancy VN-', text, re.MULTILINE))
    notices = split_notices(text)
    records = [parse_notice(n, gazette_id, gazette_date) for n in notices]
    return records, raw_vn_count

def write_csv(records, output_path):
    """Write records to CSV with proper quoting."""
    if not records:
        return
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=list(records[0].keys()),
            quoting=csv.QUOTE_ALL,
            lineterminator='\r\n'
        )
        writer.writeheader()
        writer.writerows(records)

def _load_parse_log() -> set[str]:
    """Return the set of filenames whose *latest* parse-log entry is `parsed`.

    The log is append-only, so a filename can appear more than once (e.g. a
    `missing_pdf` row followed later by a `parsed` row after retry). Reading in
    order and keeping the last status per filename makes retries recordable:
    only files that most recently succeeded are treated as done and skipped.
    `missing_pdf`/`error` files fall through and are retried on the next run.
    """
    if not PARSE_LOG_PATH.exists():
        return set()
    latest: dict[str, str] = {}
    with open(PARSE_LOG_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            latest[row['filename']] = row['status']
    return {fn for fn, status in latest.items() if status == 'parsed'}


def _append_parse_log(row: dict) -> None:
    """Append one row to parse_log.csv, creating headers if needed."""
    write_header = not PARSE_LOG_PATH.exists()
    PARSE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PARSE_LOG_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=PARSE_LOG_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _rewrite_parse_log_deduped() -> None:
    """Rewrite parse_log.csv keeping only the last entry per filename.

    Ordered by (gazette_date, filename). Bounds log growth from repeatedly
    retried failures and drops rows superseded by a later status. Called only
    at end of run, immediately after the final parquet write, so the full
    rewrite can never leave the log ahead of persisted data.
    """
    if not PARSE_LOG_PATH.exists():
        return
    latest: dict[str, dict] = {}
    with open(PARSE_LOG_PATH, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            latest[row['filename']] = row
    rows = sorted(latest.values(), key=lambda r: (r['gazette_date'], r['filename']))
    tmp = Path(str(PARSE_LOG_PATH) + '.tmp')
    with open(tmp, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=PARSE_LOG_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, PARSE_LOG_PATH)

def parse_gazette_batch(batch_size: int | None = None, full: bool = False) -> None:
    """
    Parse gazette PDFs and write records to gazette_vacancies_raw.parquet.

    Incremental (default): skips files whose latest parse-log entry is `parsed`;
    files logged `missing_pdf`/`error` are retried. Parquet and parse_log are
    flushed together every FLUSH_EVERY PDFs (see module docstring).
    Full (--full): ignores parse_log, reparsing every PDF in data/pdfs/ and
    replacing the existing parquet. Use after parser changes.
    """
    if not MANIFEST_PATH.exists():
        print(f"Manifest not found: {MANIFEST_PATH}", file=sys.stderr)
        print("Run 01_download.py first.", file=sys.stderr)
        sys.exit(1)

    manifest = pd.read_csv(MANIFEST_PATH, dtype=str)
    downloadable = manifest[
        (manifest['status'] == 'downloaded') &
        (manifest['gazette_type'].isin(PARSE_TYPES))
    ].copy()

    if full:
        already_parsed: set[str] = set()
        if PARSE_LOG_PATH.exists():
            PARSE_LOG_PATH.unlink()
            print("Full reparse: cleared parse_log.csv")
    else:
        already_parsed = _load_parse_log()

    pending = downloadable[~downloadable['filename'].isin(already_parsed)]
    pending = pending.sort_values('gazette_date').reset_index(drop=True)

    if batch_size is not None:
        pending = pending.head(batch_size)

    total_pending = len(pending)
    print(f"Downloaded (parseable types): {len(downloadable)}")
    if not full:
        print(f"Already parsed:               {len(already_parsed)}")
    print(f"Pending this run:             {total_pending}"
          + (f"  (batch_size={batch_size})" if batch_size else "")
          + ("  [full reparse]" if full else ""))

    if total_pending == 0:
        print("Nothing to do.")
        return

    existing_df = None
    if not full and PARQUET_PATH.exists():
        existing_df = pd.read_parquet(PARQUET_PATH)
        existing_df['gazette_date'] = pd.to_datetime(existing_df['gazette_date'])
        print(f"Existing parquet:             {len(existing_df):,} rows")
    elif full and PARQUET_PATH.exists():
        print(f"Full reparse: replacing {PARQUET_PATH}")

    # `new_records` accumulates across the whole run; each flush rewrites the
    # full combined parquet (existing + all new records so far). `pending_log`
    # holds log rows not yet written; they are appended only after the parquet
    # write, so the log can never run ahead of persisted data.
    new_records: list[dict] = []
    pending_log: list[dict] = []
    n_parsed  = 0
    n_errors  = 0
    n_since_flush = 0

    def flush(final: bool) -> None:
        """Persist parquet (if there are new records), then the pending log rows.

        Parquet first (atomic .tmp + os.replace), log after — never the reverse.
        The end-of-run flush additionally rewrites the log deduplicated to the
        last status per filename.
        """
        if new_records:
            new_df = pd.DataFrame(new_records)
            new_df['gazette_date'] = pd.to_datetime(new_df['gazette_date'])
            if existing_df is not None:
                combined = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined = new_df
            combined = combined.sort_values(
                ['gazette_date', 'vacancy_no'], na_position='last'
            ).reset_index(drop=True)
            PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = Path(str(PARQUET_PATH) + '.tmp')
            combined.to_parquet(tmp, index=False)
            os.replace(tmp, PARQUET_PATH)

        # Only after the parquet is safely on disk do we touch the log.
        for log_row in pending_log:
            _append_parse_log(log_row)
        pending_log.clear()

        if final:
            _rewrite_parse_log_deduped()

    for _, row in pending.iterrows():
        filename    = row['filename']
        gazette_date = row['gazette_date']
        gazette_id   = row['gazette_id']
        gazette_type = row['gazette_type']
        pdf_path     = PDF_DIR / filename

        if not pdf_path.exists():
            print(f"  MISSING PDF: {filename}", file=sys.stderr)
            pending_log.append({
                'filename':     filename,
                'gazette_date': gazette_date,
                'gazette_id':   gazette_id,
                'gazette_type': gazette_type,
                'record_count': 0,
                'vn_count':     0,
                'status':       'missing_pdf',
            })
            n_errors += 1
            continue

        try:
            records, vn_count = parse_gazette(str(pdf_path), gazette_id, gazette_date)
        except Exception as e:
            print(f"  ERROR {filename}: {e}", file=sys.stderr)
            pending_log.append({
                'filename':     filename,
                'gazette_date': gazette_date,
                'gazette_id':   gazette_id,
                'gazette_type': gazette_type,
                'record_count': 0,
                'vn_count':     0,
                'status':       'error',
            })
            n_errors += 1
            continue

        for r in records:
            r['raw_vn_count'] = vn_count
            r['gazette_type'] = gazette_type

        new_records.extend(records)
        n_parsed += 1
        n_since_flush += 1

        drop_note = ''
        if len(records) < vn_count:
            drop_note = f'  ← {vn_count - len(records)} VN markers unparsed'
        print(f"  {gazette_date}  {gazette_id}  {gazette_type:15s}  "
              f"{len(records):3d}/{vn_count} notices{drop_note}")

        pending_log.append({
            'filename':     filename,
            'gazette_date': gazette_date,
            'gazette_id':   gazette_id,
            'gazette_type': gazette_type,
            'record_count': len(records),
            'vn_count':     vn_count,
            'status':       'parsed',
        })

        if n_since_flush >= FLUSH_EVERY:
            flush(final=False)
            n_since_flush = 0

    flush(final=True)

    total_rows = (len(existing_df) if existing_df is not None else 0) + len(new_records)
    print(f"\nParsed {n_parsed} PDFs  +{len(new_records):,} records")
    if new_records:
        print(f"Total in parquet: {total_rows:,} rows → {PARQUET_PATH}")
    if n_errors:
        print(f"Errors: {n_errors}", file=sys.stderr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='APS PS Gazette PDF parser.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'examples:\n'
            '  python pipeline/02_parse.py batch\n'
            '  python pipeline/02_parse.py batch --batch-size 50\n'
            '  python pipeline/02_parse.py single gazette.pdf PS16 2026-04-23 out.csv'
        ),
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_batch = sub.add_parser('batch', help='parse all pending PDFs from manifest')
    p_batch.add_argument(
        '--batch-size', type=int, default=None, metavar='N',
        help='stop after parsing N PDFs (default: all pending)',
    )
    p_batch.add_argument(
        '--full', action='store_true',
        help='ignore parse log and reparse all PDFs, replacing existing parquet',
    )

    p_single = sub.add_parser('single', help='parse one PDF to CSV (original interface)')
    p_single.add_argument('pdf_path',     help='path to gazette PDF')
    p_single.add_argument('gazette_id',   help='gazette identifier, e.g. PS16')
    p_single.add_argument('gazette_date', help='gazette date YYYY-MM-DD')
    p_single.add_argument('output_csv',   help='output CSV path')

    args = parser.parse_args()

    if args.cmd == 'batch':
        parse_gazette_batch(batch_size=args.batch_size, full=args.full)

    elif args.cmd == 'single':
        records, vn_count = parse_gazette(args.pdf_path, args.gazette_id, args.gazette_date)
        write_csv(records, args.output_csv)
        print(f"Parsed {len(records)} of {vn_count} notices → {args.output_csv}")
