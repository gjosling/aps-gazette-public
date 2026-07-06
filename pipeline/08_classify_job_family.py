#!/usr/bin/env python3
"""
08_classify_job_family.py — Classify APS vacancies into APSC 2025 Job Families.

Uses the synchronous Messages API for small runs (<1000 rows) and the Batch API
for bulk runs. Override with --sync or --batch.

Source of truth for classification data is data/job_family_classifications.parquet,
which is persisted to private R2 across CI runs. The release parquet receives only
the three public columns (job_family, job_family_confidence, job_family_secondary).
Audit columns (model, prompt version, raw response, timestamp) stay in the
classifications file only.

Usage:
    uv run python pipeline/08_classify_job_family.py [--dry-run] [--reclassify] [--sample N]
    uv run python pipeline/08_classify_job_family.py --agency "Australian Taxation Office" --year 2025
    uv run python pipeline/08_classify_job_family.py --batch   # force Batch API
    uv run python pipeline/08_classify_job_family.py --sync    # force synchronous API
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

# release_io is a legal module name; make it importable by adding the pipeline/
# dir to sys.path (the numerically-prefixed pipeline scripts can't be).
sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_io

PARQUET_PATH          = Path("data/release/aps_gazette_vacancies.parquet")
CSV_PATH              = Path("data/release/aps_gazette_vacancies.csv.gz")
CLASSIFICATIONS_PATH  = Path("data/job_family_classifications.parquet")
OVERRIDES_PATH        = Path("data/job_family_overrides.csv")
PROMPT_PATH           = Path("prompts/job_family_system.txt")
BATCH_IDS_PATH        = Path("data/classify_batch_ids.json")

VALID_JOB_FAMILIES = {
    "ACCOUNTING_AND_FINANCE",
    "BUSINESS_AND_ORGANISATIONAL_MANAGEMENT",
    "COMMUNICATIONS_AND_ENGAGEMENT",
    "COMPLIANCE_AND_REGULATION",
    "DATA_AND_RESEARCH",
    "ENGINEERING_AND_TECHNICAL",
    "HEALTH",
    "HUMAN_RESOURCES",
    "ICT_AND_DIGITAL",
    "INTELLIGENCE_AND_INFORMATION_MANAGEMENT",
    "LEGAL_AND_PARLIAMENTARY",
    "POLICY",
    "PORTFOLIO_PROGRAM_AND_PROJECT_MANAGEMENT",
    "SCIENCE",
    "SERVICE_DELIVERY",
    "TRADES_AND_LABOUR",
}

MODEL            = "claude-sonnet-4-6"
MAX_DESC         = 3000
MAX_TOKENS       = 600
BATCH_SIZE       = 10_000  # default; overridable with --batch-size
POLL_INTERVAL    = 60
TIMEOUT          = 7_200
PROMPT_VERSION   = "2025-v2"
SYNC_THRESHOLD   = 1000   # use sync API when classifying fewer than this many rows

# All columns stored in the classifications file (internal / private R2)
ALL_CLASSIFICATION_COLS = [
    "job_family",
    "job_family_confidence",
    "job_family_secondary",
    "job_family_model",
    "job_family_prompt_version",
    "job_family_raw_response",
    "job_family_classified_at",
]

# Subset written into the public release parquet
PUBLIC_COLS = [
    "job_family",
    "job_family_confidence",
    "job_family_secondary",
]

_BRANCH_SKIP = {"", "nan", "none", "various", "na", "n/a"}


# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------

def build_user_prompt(row: pd.Series) -> str:
    parts = [
        f"Job Title: {row['job_title']}",
        f"Classification Level: {row['classification_code']}",
        f"Organisational Unit: {row['division']}",
    ]

    branch = row.get("branch")
    if pd.notna(branch) and str(branch).strip().lower() not in _BRANCH_SKIP:
        parts.append(f"Branch: {branch}")

    desc_clean = row.get("description_clean")
    duties     = row.get("duties_text")
    description_text = ""
    if pd.notna(desc_clean) and str(desc_clean).strip():
        description_text = str(desc_clean)
    elif pd.notna(duties) and str(duties).strip():
        description_text = str(duties)
    parts.append(f"Description:\n{description_text[:MAX_DESC]}")

    return "\n".join(parts)


def make_custom_id(gazette_id: str, vacancy_no: str) -> str:
    raw = f"{gazette_id}_{vacancy_no}"
    return re.sub(r"[^a-zA-Z0-9_-]", "_", str(raw).strip())[:64]


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_response(text: str) -> tuple[str | None, str | None, str | None]:
    cleaned = re.sub(r"^```(?:json)?\s*", "", text.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())
    # Try full string first, then every {...} object found in the text.
    # The model sometimes self-corrects by emitting a second JSON block after
    # chain-of-thought, so we scan all candidates and return the LAST one that
    # contains a valid job_family (earlier blocks may have hallucinated names).
    candidates = [cleaned] + [m.group(0) for m in re.finditer(r"\{[^{}]+\}", cleaned, re.DOTALL)]
    best: tuple[str | None, str | None, str | None] | None = None
    for s in candidates:
        try:
            data = json.loads(s)
            family = data.get("job_family") or None
            if family is not None and family not in VALID_JOB_FAMILIES:
                family = None
            result = (
                family,
                data.get("confidence") or None,
                data.get("secondary_family") or None,
            )
            # Always keep the latest successfully-parsed result; prefer one with a valid family.
            if best is None or family is not None:
                best = result
        except json.JSONDecodeError:
            continue
    return best if best is not None else (None, None, None)


# ---------------------------------------------------------------------------
# Batch API
# ---------------------------------------------------------------------------

def submit_batch_with_retry(
    client: anthropic.Anthropic,
    requests: list,
    label: str,
    max_attempts: int = 3,
) -> str:
    delays = [5, 10, 20]
    for attempt in range(max_attempts):
        try:
            batch = client.messages.batches.create(requests=requests)
            print(f"  Submitted {label}: {batch.id} ({len(requests)} requests)")
            return batch.id
        except Exception as exc:
            if attempt < max_attempts - 1:
                wait = delays[attempt]
                print(f"  Batch submit failed ({exc}); retrying in {wait}s ...")
                time.sleep(wait)
            else:
                print(f"  Batch submit failed after {max_attempts} attempts: {exc}", file=sys.stderr)
                raise


def sync_classify(
    client: anthropic.Anthropic,
    to_classify: pd.DataFrame,
    system_prompt: str,
) -> dict[tuple[str, str], dict]:
    """Classify rows one at a time using the synchronous Messages API."""
    results: dict[tuple[str, str], dict] = {}
    total = len(to_classify)
    n_ok = n_skip = 0

    for i, (_, row) in enumerate(to_classify.iterrows()):
        if i % 50 == 0:
            print(f"  {i}/{total} processed ({n_ok} ok, {n_skip} skipped) ...")

        gid = str(row["gazette_id"])
        vno = str(row["vacancy_no"])

        raw = None
        for attempt in range(3):
            try:
                msg = client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    temperature=0,
                    system=[{
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": build_user_prompt(row)}],
                )
                raw = msg.content[0].text.strip() if msg.content else ""
                break
            except Exception as exc:
                wait = 5 * (2 ** attempt)
                if attempt < 2:
                    print(f"  API error ({exc}); retrying in {wait}s ...")
                    time.sleep(wait)
                else:
                    print(f"  Skipping {gid}/{vno} after 3 failed attempts: {exc}")
                    n_skip += 1

        if raw is None:
            continue

        family, confidence, secondary = parse_response(raw)
        n_ok += 1
        results[(gid, vno)] = {
            "job_family":                family,
            "job_family_confidence":     confidence,
            "job_family_secondary":      secondary,
            "job_family_model":          MODEL,
            "job_family_prompt_version": PROMPT_VERSION,
            "job_family_raw_response":   raw,
            "job_family_classified_at":  datetime.now(timezone.utc).isoformat(),
        }

    print(f"  {total}/{total} processed ({n_ok} ok, {n_skip} skipped)")
    return results


def poll_batches(client: anthropic.Anthropic, batch_ids: list[str]) -> None:
    start   = time.time()
    pending = set(batch_ids)

    while pending:
        if time.time() - start > TIMEOUT:
            print(f"\nTimeout ({TIMEOUT}s) reached with {len(pending)} batches still pending.",
                  file=sys.stderr)
            sys.exit(1)

        still_pending: set[str] = set()
        for bid in sorted(pending):
            try:
                b = client.messages.batches.retrieve(bid)
                c = b.request_counts
                print(f"  {bid}: {b.processing_status} "
                      f"({c.processing} processing / {c.succeeded} done / {c.errored} errors)")
                if b.processing_status != "ended":
                    still_pending.add(bid)
            except Exception as exc:
                print(f"  {bid}: transient error ({exc}) — will retry next poll")
                still_pending.add(bid)

        pending = still_pending
        if pending:
            print(f"  Waiting {POLL_INTERVAL}s ...")
            time.sleep(POLL_INTERVAL)


def retrieve_batch_results(
    client: anthropic.Anthropic,
    batch_ids: list[str],
    id_map: dict[str, tuple[str, str]],
) -> dict[tuple[str, str], dict]:
    """Return {(gazette_id, vacancy_no): full classification fields}."""
    results: dict[tuple[str, str], dict] = {}
    classified_at = datetime.now(timezone.utc).isoformat()

    for bid in batch_ids:
        n_ok = n_err = n_parse_fail = 0
        for result in client.messages.batches.results(bid):
            key = id_map.get(result.custom_id)
            if key is None:
                continue
            if result.result.type == "succeeded":
                content = result.result.message.content
                raw = content[0].text.strip() if content else ""
                family, confidence, secondary = parse_response(raw)
                n_ok += 1
                if family is None:
                    n_parse_fail += 1
                results[key] = {
                    "job_family":                family,
                    "job_family_confidence":     confidence,
                    "job_family_secondary":      secondary,
                    "job_family_model":          MODEL,
                    "job_family_prompt_version": PROMPT_VERSION,
                    "job_family_raw_response":   raw,
                    "job_family_classified_at":  classified_at,
                }
            else:
                n_err += 1
                # Don't write to classifications file — absent key means next run retries
        print(f"  {bid}: {n_ok} ok, {n_err} API errors, {n_parse_fail} parse failures")

    return results


# ---------------------------------------------------------------------------
# Release parquet helpers
# ---------------------------------------------------------------------------

def insert_public_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Place PUBLIC_COLS immediately after duties_text (or description_clean)."""
    base = [c for c in df.columns if c not in PUBLIC_COLS]
    for anchor in ("duties_text", "description_clean"):
        if anchor in base:
            pos = base.index(anchor) + 1
            break
    else:
        pos = len(base)
    ordered = base[:pos] + PUBLIC_COLS + base[pos:]
    return df[ordered]


def join_and_write_release(df: pd.DataFrame, clf: pd.DataFrame) -> None:
    """Join PUBLIC_COLS from clf onto df and write the release parquet + csv.gz."""
    for col in PUBLIC_COLS:
        if col in df.columns:
            df = df.drop(columns=[col])

    clf_public = (
        clf[["gazette_id", "vacancy_no"] + PUBLIC_COLS]
        .copy()
        .assign(
            gazette_id=clf["gazette_id"].astype(str),
            vacancy_no=clf["vacancy_no"].astype(str),
        )
    )

    df = df.copy()
    df["gazette_id"] = df["gazette_id"].astype(str)
    df["vacancy_no"] = df["vacancy_no"].astype(str)

    df = df.merge(clf_public, on=["gazette_id", "vacancy_no"], how="left")
    df = insert_public_cols(df)

    print("\n=== WRITE RELEASE ===")
    n_classified = df["job_family"].notna().sum()
    print(f"  job_family populated: {n_classified:,} / {len(df):,} rows")
    release_io.write_release(df, "08_classify_job_family")
    print(f"  Written: {PARQUET_PATH}")
    print(f"  Written: {CSV_PATH}")


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run(
    dry_run: bool = False,
    reclassify: bool = False,
    sample: int | None = None,
    agency: str | None = None,
    year: int | None = None,
    mode: str = "auto",
    batch_size: int = BATCH_SIZE,
) -> None:
    print("=== 08 CLASSIFY JOB FAMILY ===")

    # --- Load release parquet ---
    if not PARQUET_PATH.exists():
        print(f"ERROR: {PARQUET_PATH} not found", file=sys.stderr)
        sys.exit(1)
    df = pd.read_parquet(PARQUET_PATH)
    print(f"Loaded {len(df):,} rows from {PARQUET_PATH}")

    # --- Load or create classifications file ---
    if CLASSIFICATIONS_PATH.exists():
        clf = pd.read_parquet(CLASSIFICATIONS_PATH)
        print(f"Loaded {len(clf):,} existing classifications from {CLASSIFICATIONS_PATH}")
    else:
        clf = pd.DataFrame(columns=["gazette_id", "vacancy_no"] + ALL_CLASSIFICATION_COLS)
        print("No existing classifications file — starting fresh.")

    # --- Handle --reclassify ---
    if reclassify:
        if agency is None and year is None:
            n_existing = len(clf)
            clf = pd.DataFrame(columns=clf.columns)
            print(f"--reclassify: cleared all {n_existing:,} existing classifications")
        else:
            # Build the set of keys to remove using the release parquet as the filter source
            filter_df = df.copy()
            if agency is not None:
                filter_df = filter_df[
                    filter_df["agency_canonical"].str.contains(agency, case=False, na=False)
                ]
            if year is not None:
                filter_df = filter_df[
                    pd.to_datetime(filter_df["gazette_date"], errors="coerce").dt.year == year
                ]
            keys_to_remove = set(
                filter_df["gazette_id"].astype(str) + "||" + filter_df["vacancy_no"].astype(str)
            )
            clf_keys = clf["gazette_id"].astype(str) + "||" + clf["vacancy_no"].astype(str)
            n_existing = clf_keys.isin(keys_to_remove).sum()
            clf = clf[~clf_keys.isin(keys_to_remove)].copy()
            print(f"--reclassify: cleared {n_existing:,} classifications matching filters")

    # --- Determine unclassified rows ---
    clf_keys = set(clf["gazette_id"].astype(str) + "||" + clf["vacancy_no"].astype(str))
    df_keys  = df["gazette_id"].astype(str) + "||" + df["vacancy_no"].astype(str)
    to_classify = df[~df_keys.isin(clf_keys)].copy()
    print(f"Rows needing classification: {len(to_classify):,}  ({len(df) - len(to_classify):,} already classified)")

    # --- Apply optional filters ---
    if agency is not None:
        mask = to_classify["agency_canonical"].str.contains(agency, case=False, na=False)
        to_classify = to_classify[mask]
        print(f"--agency filter '{agency}': {len(to_classify):,} rows")

    if year is not None:
        mask = pd.to_datetime(to_classify["gazette_date"], errors="coerce").dt.year == year
        to_classify = to_classify[mask]
        print(f"--year filter {year}: {len(to_classify):,} rows")

    if sample is not None:
        to_classify = to_classify.head(sample)
        print(f"--sample {sample}: classifying first {len(to_classify)} rows")

    # --- Decide API mode ---
    if mode == "auto":
        use_sync = len(to_classify) < SYNC_THRESHOLD
    else:
        use_sync = (mode == "sync")

    n_batches = (len(to_classify) + batch_size - 1) // batch_size if not to_classify.empty else 0
    if to_classify.empty:
        print("Nothing to classify.")
    elif use_sync:
        print(f"Mode: sync (synchronous Messages API)")
    else:
        print(f"Mode: batch  —  will submit {n_batches} batch(es) of up to {batch_size:,} requests each")

    if dry_run:
        print("\n--dry-run: skipping API calls and file writes.")
        return

    # --- Classify new rows ---
    if not to_classify.empty:
        if not PROMPT_PATH.exists():
            print(f"ERROR: {PROMPT_PATH} not found", file=sys.stderr)
            sys.exit(1)
        system_prompt = PROMPT_PATH.read_text()

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
            sys.exit(1)
        client = anthropic.Anthropic(api_key=api_key)

        if use_sync:
            print("\n=== CLASSIFY (SYNC) ===")
            results = sync_classify(client, to_classify, system_prompt)
            print(f"Total results: {len(results):,}")
        else:
            # Build custom_id → (gazette_id, vacancy_no) map
            id_map: dict[str, tuple[str, str]] = {}
            for _, row in to_classify.iterrows():
                cid = make_custom_id(str(row["gazette_id"]), str(row["vacancy_no"]))
                id_map[cid] = (str(row["gazette_id"]), str(row["vacancy_no"]))

            # Submit or resume
            if BATCH_IDS_PATH.exists():
                batch_ids: list[str] = json.loads(BATCH_IDS_PATH.read_text())
                print(f"\nResuming: found {BATCH_IDS_PATH} with {len(batch_ids)} batch ID(s) — skipping submission.")
            else:
                print("\n=== SUBMIT BATCHES ===")
                BATCH_IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
                batch_ids = []
                for batch_num in range(n_batches):
                    chunk = to_classify.iloc[batch_num * batch_size : (batch_num + 1) * batch_size]
                    requests = []
                    for _, row in chunk.iterrows():
                        cid = make_custom_id(str(row["gazette_id"]), str(row["vacancy_no"]))
                        requests.append({
                            "custom_id": cid,
                            "params": {
                                "model": MODEL,
                                "max_tokens": MAX_TOKENS,
                                "temperature": 0,
                                "system": [{
                                    "type": "text",
                                    "text": system_prompt,
                                    "cache_control": {"type": "ephemeral"},
                                }],
                                "messages": [{"role": "user", "content": build_user_prompt(row)}],
                            },
                        })
                    bid = submit_batch_with_retry(client, requests, f"batch {batch_num + 1}/{n_batches}")
                    batch_ids.append(bid)
                    BATCH_IDS_PATH.write_text(json.dumps(batch_ids, indent=2))
                print(f"\nBatch IDs saved to {BATCH_IDS_PATH}")

            print("\n=== POLL BATCHES ===")
            poll_batches(client, batch_ids)

            print("\n=== RETRIEVE RESULTS ===")
            results = retrieve_batch_results(client, batch_ids, id_map)
            print(f"Total results retrieved: {len(results):,}")

        # Append new results to classifications file
        print("\n=== UPDATE CLASSIFICATIONS FILE ===")
        new_rows = pd.DataFrame([
            {"gazette_id": gid, "vacancy_no": vno, **fields}
            for (gid, vno), fields in results.items()
        ])
        for col in ALL_CLASSIFICATION_COLS:
            if col not in new_rows.columns:
                new_rows[col] = None
        clf = pd.concat([clf, new_rows], ignore_index=True)

        CLASSIFICATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        clf.to_parquet(CLASSIFICATIONS_PATH, index=False)
        n_with_family = clf["job_family"].notna().sum()
        print(f"Classifications file: {len(clf):,} rows "
              f"({n_with_family:,} classified, {len(clf) - n_with_family:,} pending retry)")
        print(f"Written: {CLASSIFICATIONS_PATH}")

        if not use_sync and BATCH_IDS_PATH.exists():
            BATCH_IDS_PATH.unlink()
            print(f"Removed {BATCH_IDS_PATH}")

    # --- Apply manual overrides (survive reruns / R2 refreshes) ---
    if OVERRIDES_PATH.exists():
        overrides = pd.read_csv(OVERRIDES_PATH, dtype=str)
        overrides["gazette_id"] = overrides["gazette_id"].astype(str)
        overrides["vacancy_no"]  = overrides["vacancy_no"].astype(str)
        clf["gazette_id"] = clf["gazette_id"].astype(str)
        clf["vacancy_no"]  = clf["vacancy_no"].astype(str)
        override_keys = overrides["gazette_id"] + "||" + overrides["vacancy_no"]
        clf_keys_str  = clf["gazette_id"] + "||" + clf["vacancy_no"]
        clf = clf[~clf_keys_str.isin(override_keys)].copy()
        for col in ALL_CLASSIFICATION_COLS:
            if col not in overrides.columns:
                overrides[col] = None
        clf = pd.concat([clf, overrides[["gazette_id", "vacancy_no"] + ALL_CLASSIFICATION_COLS]], ignore_index=True)
        print(f"\nApplied {len(overrides)} manual override(s) from {OVERRIDES_PATH}")

    # --- Join classifications into release parquet ---
    join_and_write_release(df, clf)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Classify APS vacancies into APSC 2025 Job Families.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="show how many rows need classification without calling the API",
    )
    parser.add_argument(
        "--reclassify", action="store_true",
        help="clear existing classifications (for matching rows if --agency/--year given) and re-run",
    )
    parser.add_argument(
        "--sample", type=int, metavar="N",
        help="classify only the first N unclassified rows (for testing)",
    )
    parser.add_argument(
        "--agency", metavar="NAME",
        help="classify only rows where agency_canonical contains NAME (case-insensitive)",
    )
    parser.add_argument(
        "--year", type=int, metavar="YEAR",
        help="classify only rows where gazette_date falls in YEAR",
    )
    parser.add_argument(
        "--batch-size", type=int, metavar="N", default=BATCH_SIZE,
        help=f"number of requests per batch (default: {BATCH_SIZE})",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--sync", dest="mode", action="store_const", const="sync",
        help="force synchronous Messages API (default for <1000 rows)",
    )
    mode_group.add_argument(
        "--batch", dest="mode", action="store_const", const="batch",
        help="force Batch API (default for >=1000 rows)",
    )
    parser.set_defaults(mode="auto")
    args = parser.parse_args()

    run(
        dry_run=args.dry_run,
        reclassify=args.reclassify,
        sample=args.sample,
        agency=args.agency,
        year=args.year,
        mode=args.mode,
        batch_size=args.batch_size,
    )
