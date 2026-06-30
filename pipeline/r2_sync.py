#!/usr/bin/env python3
"""
r2_sync.py — Sync pipeline state and release files with Cloudflare R2.

Used by CI to persist state across runs. Local pipeline scripts (01–06) are
unaffected — they only read and write data/ on disk. This script is the CI
wrapper that bookends each run:

  pull       (start of run)  R2 → local  Restore manifest, parse log, and
                                          accumulated raw parquet so the pipeline
                                          only processes files it hasn't seen before.

  push-pdfs  (after download) local → R2  Archive new PDFs to the private bucket
                                          under pdfs/. Idempotent — already-archived
                                          PDFs are skipped by Content-Length check.

  push       (end of run)    local → R2  Persist updated state files and publish
                                          the release parquet and CSV.

  pull-pdfs  (manual only)   R2 → local  Restore the full PDF archive from R2 to
                                          data/pdfs/. Used before a full reparse;
                                          never called in normal CI.

Pull is graceful: missing objects (first run, or partial state) are skipped
without error. Push is idempotent: files whose local size already matches the
R2 Content-Length are skipped.

Two buckets, separate credentials, shared Cloudflare account:
  Private  pipeline state (manifest, parse log, raw parquet) and PDF archive
  Public   release files (parquet, CSV)

Required environment variables:
    R2_ACCOUNT_ID                Cloudflare account ID (shared; builds endpoint URL)
    R2_PRIVATE_ACCESS_KEY_ID     Access key for the private bucket
    R2_PRIVATE_SECRET_ACCESS_KEY Secret key for the private bucket
    R2_PRIVATE_BUCKET            Private bucket name
    R2_PUBLIC_ACCESS_KEY_ID      Access key for the public bucket
    R2_PUBLIC_SECRET_ACCESS_KEY  Secret key for the public bucket
    R2_PUBLIC_BUCKET             Public bucket name

Usage:
    python pipeline/r2_sync.py pull [--dry-run]
    python pipeline/r2_sync.py push-pdfs [--dry-run]
    python pipeline/r2_sync.py push [--dry-run]
    python pipeline/r2_sync.py pull-pdfs [--dry-run]
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

load_dotenv()

# Pull: (r2_key, local_path) — missing R2 objects are skipped silently.
PULL_PRIVATE = [
    ("manifest.csv",                          "data/manifest.csv"),
    ("parse_log.csv",                         "data/parse_log.csv"),
    ("gazette_vacancies_raw.parquet",         "data/gazette_vacancies_raw.parquet"),
    ("job_family_classifications.parquet",    "data/job_family_classifications.parquet"),
]

# Push private: (local_path, r2_key) — missing local files are skipped with a warning.
PUSH_PRIVATE = [
    ("data/manifest.csv",                          "manifest.csv"),
    ("data/parse_log.csv",                         "parse_log.csv"),
    ("data/gazette_vacancies_raw.parquet",         "gazette_vacancies_raw.parquet"),
    ("data/job_family_classifications.parquet",    "job_family_classifications.parquet"),
]

# Push public: (local_path, r2_key)
PUSH_PUBLIC = [
    ("data/release/aps_gazette_vacancies.parquet", "gazette/aps_gazette_vacancies.parquet"),
    ("data/release/aps_gazette_vacancies.csv.gz",  "gazette/aps_gazette_vacancies.csv.gz"),
]

# Multipart threshold: boto3 default is 8 MB.
TRANSFER_CFG = TransferConfig(multipart_threshold=8 * 1024 * 1024)


def _check_env(*vars: str) -> None:
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        for v in missing:
            print(f"Missing required environment variable: {v}", file=sys.stderr)
        sys.exit(1)


def _s3_client(key_id_var: str, secret_var: str):
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ[key_id_var],
        aws_secret_access_key=os.environ[secret_var],
        region_name="auto",
    )


def _pull_one(s3, bucket: str, r2_key: str, local: str, dry_run: bool) -> str:
    dest = Path(local)
    if dry_run:
        return f"would pull: {r2_key} → {local}"
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(Bucket=bucket, Key=r2_key, Filename=str(dest))
        size_mb = dest.stat().st_size / 1e6
        return f"pulled ({size_mb:.1f} MB): {r2_key}"
    except s3.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            return f"skip (not in R2): {r2_key}"
        raise


def _remote_size(s3, bucket: str, key: str) -> int | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except Exception:
        return None


def _push_one(s3, bucket: str, local: str, r2_key: str, dry_run: bool) -> str:
    p = Path(local)
    if not p.exists():
        return f"SKIP (not found locally): {local}"

    local_size = p.stat().st_size
    if _remote_size(s3, bucket, r2_key) == local_size:
        return f"skip (unchanged, {local_size / 1e6:.1f} MB): {r2_key}"

    mb = local_size / 1e6
    if dry_run:
        return f"would push ({mb:.1f} MB): {local} → {r2_key}"

    s3.upload_file(Filename=str(p), Bucket=bucket, Key=r2_key, Config=TRANSFER_CFG)
    return f"pushed ({mb:.1f} MB): {r2_key}"


def pull(dry_run: bool = False) -> None:
    _check_env("R2_ACCOUNT_ID",
               "R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY", "R2_PRIVATE_BUCKET")
    s3     = _s3_client("R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY")
    bucket = os.environ["R2_PRIVATE_BUCKET"]
    if dry_run:
        print("Mode: dry run\n")
    for r2_key, local in PULL_PRIVATE:
        print(f"  {_pull_one(s3, bucket, r2_key, local, dry_run)}")
    print()
    print("Pull complete.")


def push_pdfs(dry_run: bool = False) -> None:
    _check_env("R2_ACCOUNT_ID",
               "R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY", "R2_PRIVATE_BUCKET")
    s3     = _s3_client("R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY")
    bucket = os.environ["R2_PRIVATE_BUCKET"]

    pdf_dir = Path("data/pdfs")
    if not pdf_dir.exists():
        print("data/pdfs/ not found — nothing to archive.")
        return

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    print(f"Archiving {len(pdfs)} PDFs → private R2 pdfs/")
    if dry_run:
        print("Mode: dry run\n")

    total_mb = 0.0
    for pdf in pdfs:
        r2_key = f"pdfs/{pdf.name}"
        msg = _push_one(s3, bucket, str(pdf), r2_key, dry_run)
        print(f"  {msg}")
        if msg.startswith("pushed"):
            total_mb += pdf.stat().st_size / 1e6

    print()
    if dry_run:
        print("Dry run complete.")
    else:
        print(f"Archive complete. {total_mb:.1f} MB uploaded this run.")


def pull_pdfs(dry_run: bool = False) -> None:
    """Download all archived PDFs from R2 to data/pdfs/. Used before a full reparse."""
    _check_env("R2_ACCOUNT_ID",
               "R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY", "R2_PRIVATE_BUCKET")
    s3     = _s3_client("R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY")
    bucket = os.environ["R2_PRIVATE_BUCKET"]

    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix="pdfs/")
        for obj in page.get("Contents", [])
    ]
    print(f"Found {len(keys)} archived PDFs in R2 pdfs/")
    if dry_run:
        print("Mode: dry run\n")

    for key in keys:
        local = f"data/{key}"   # data/pdfs/<filename>.pdf
        print(f"  {_pull_one(s3, bucket, key, local, dry_run)}")

    print()
    print("Pull PDFs complete.")


def push(dry_run: bool = False) -> None:
    _check_env("R2_ACCOUNT_ID",
               "R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY", "R2_PRIVATE_BUCKET",
               "R2_PUBLIC_ACCESS_KEY_ID",  "R2_PUBLIC_SECRET_ACCESS_KEY",  "R2_PUBLIC_BUCKET")
    s3_priv = _s3_client("R2_PRIVATE_ACCESS_KEY_ID", "R2_PRIVATE_SECRET_ACCESS_KEY")
    s3_pub  = _s3_client("R2_PUBLIC_ACCESS_KEY_ID",  "R2_PUBLIC_SECRET_ACCESS_KEY")
    bkt_priv = os.environ["R2_PRIVATE_BUCKET"]
    bkt_pub  = os.environ["R2_PUBLIC_BUCKET"]
    if dry_run:
        print("Mode: dry run\n")

    total_mb = 0.0
    print("  -- private --")
    for local, r2_key in PUSH_PRIVATE:
        msg = _push_one(s3_priv, bkt_priv, local, r2_key, dry_run)
        print(f"  {msg}")
        if msg.startswith("pushed"):
            total_mb += Path(local).stat().st_size / 1e6

    print("  -- public --")
    for local, r2_key in PUSH_PUBLIC:
        msg = _push_one(s3_pub, bkt_pub, local, r2_key, dry_run)
        print(f"  {msg}")
        if msg.startswith("pushed"):
            total_mb += Path(local).stat().st_size / 1e6

    print()
    if dry_run:
        print("Dry run complete.")
    else:
        print(f"Push complete. {total_mb:.1f} MB uploaded this run.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync pipeline state and release files with Cloudflare R2."
    )
    parser.add_argument(
        "direction",
        choices=["pull", "push-pdfs", "push", "pull-pdfs"],
        help=(
            "pull: R2 → local (run before pipeline).  "
            "push-pdfs: archive PDFs to private R2 (run after download).  "
            "push: local → R2 (run after pipeline).  "
            "pull-pdfs: restore full PDF archive (manual, before full reparse)."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print what would be transferred without actually transferring",
    )
    args = parser.parse_args()

    if args.direction == "pull":
        pull(dry_run=args.dry_run)
    elif args.direction == "push-pdfs":
        push_pdfs(dry_run=args.dry_run)
    elif args.direction == "push":
        push(dry_run=args.dry_run)
    else:
        pull_pdfs(dry_run=args.dry_run)
