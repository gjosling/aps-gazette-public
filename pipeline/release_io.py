"""
release_io.py — Single write path for the public release files.

Every step that writes the release parquet/csv (06_build_release, 07_clean_text,
08_classify_job_family) goes through write_release() so that build provenance is
stamped consistently. The release parquet has 30 data columns and previously
carried no provenance at all; this module embeds key-value metadata into the
parquet schema (under aps_gazette:* keys) and writes a sidecar JSON with the same
fields plus per-file sha256 hashes. The CSV cannot carry embedded metadata, so the
sidecar is its provenance channel.

Dataset version policy
----------------------
DATASET_VERSION is a semantic-ish version of the *dataset contract*, not the code:

  MAJOR  — a column is removed/renamed or its meaning changes incompatibly.
  MINOR  — new columns added, or a deliberate retroactive change to published
           values (crosswalk correction, boilerplate method change, MoG
           re-attribution). Anything that makes "the same query returns different
           numbers" true.
  PATCH  — routine weekly appends and bug fixes that only affect rows added since
           the last version.

Routine CI runs do NOT bump the version — the build timestamp and git SHA
distinguish builds. A version bump is a deliberate commit editing DATASET_VERSION
plus a CHANGELOG entry. See docs/CHANGELOG.md.
"""

import hashlib
import importlib.util
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATASET_VERSION = "1.3.0"   # see version policy above

RELEASE_DIR  = Path("data/release")
PARQUET_PATH = RELEASE_DIR / "aps_gazette_vacancies.parquet"
CSV_PATH     = RELEASE_DIR / "aps_gazette_vacancies.csv.gz"
META_PATH    = RELEASE_DIR / "aps_gazette_vacancies.meta.json"

_PIPELINE_DIR = Path(__file__).resolve().parent


# ── Provenance helpers ─────────────────────────────────────────────────────────

def _git_sha() -> str:
    """GITHUB_SHA if set, else `git rev-parse HEAD`; +dirty if the tree is dirty."""
    sha = os.environ.get("GITHUB_SHA")
    if not sha:
        try:
            sha = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True, text=True, check=True,
            ).stdout.strip()
        except Exception:
            return "unknown"
    if not sha:
        return "unknown"
    try:
        porcelain = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        if porcelain:
            sha += "+dirty"
    except Exception:
        pass
    return sha


def _poppler_version() -> str:
    """First line of `pdftotext -v` stderr (poppler prints version to stderr)."""
    try:
        result = subprocess.run(["pdftotext", "-v"], capture_output=True, text=True)
        lines = (result.stderr or "").splitlines()
        return lines[0].strip() if lines and lines[0].strip() else "unknown"
    except Exception:
        return "unknown"


def _load_module_constant(filename: str, attr: str, modname: str) -> str:
    """Load a constant from a numerically-prefixed pipeline module by file path."""
    try:
        path = _PIPELINE_DIR / filename
        spec = importlib.util.spec_from_file_location(modname, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return str(getattr(module, attr))
    except Exception:
        return "unknown"


def _boilerplate_method_version() -> str:
    return _load_module_constant(
        "07_clean_text.py", "BOILERPLATE_METHOD_VERSION", "_bp_method_mod"
    )


def _job_family_prompt_version() -> str:
    return _load_module_constant(
        "08_classify_job_family.py", "PROMPT_VERSION", "_job_family_prompt_mod"
    )


# ── Metadata ───────────────────────────────────────────────────────────────────

def build_metadata(stage: str) -> dict[str, str]:
    """Return the build-provenance fields for a given pipeline stage (all strings)."""
    return {
        "dataset_version":            DATASET_VERSION,
        "build_timestamp_utc":        datetime.now(timezone.utc).isoformat(),
        "git_sha":                    _git_sha(),
        "poppler_version":            _poppler_version(),
        "boilerplate_method_version": _boilerplate_method_version(),
        "job_family_prompt_version":  _job_family_prompt_version(),
        "pipeline_stage":             stage,
        "row_count":                  "",   # filled in by write_release once df is known
    }


# ── Hashing ────────────────────────────────────────────────────────────────────

def _sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Write path ─────────────────────────────────────────────────────────────────

def write_release(df: pd.DataFrame, stage: str) -> None:
    """Write the release parquet (with embedded metadata) + csv.gz + sidecar JSON."""
    RELEASE_DIR.mkdir(parents=True, exist_ok=True)

    meta = build_metadata(stage)
    meta["row_count"] = str(len(df))

    # pandas.DataFrame.to_parquet cannot set file-level key-value metadata, so
    # convert via pyarrow and merge our aps_gazette:* keys into the schema
    # metadata (preserving pandas' own `pandas` blob).
    table = pa.Table.from_pandas(df, preserve_index=False)
    kv = {f"aps_gazette:{k}": v for k, v in meta.items()}
    table = table.replace_schema_metadata({
        **(table.schema.metadata or {}),
        **{k.encode(): v.encode() for k, v in kv.items()},
    })
    pq.write_table(table, PARQUET_PATH)

    df.to_csv(CSV_PATH, index=False, compression="gzip")

    # Sidecar: same fields plus per-file bytes + sha256 (hashed after writing).
    files = [
        {"key": path.name, "bytes": path.stat().st_size, "sha256": _sha256_file(path)}
        for path in (PARQUET_PATH, CSV_PATH)
    ]
    sidecar = {**meta, "files": files}
    META_PATH.write_text(json.dumps(sidecar, indent=2) + "\n")
