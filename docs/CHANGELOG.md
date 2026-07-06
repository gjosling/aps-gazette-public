# APS Gazette dataset changelog

Versioning: see the version policy in `pipeline/release_io.py`. Builds are further
identified by `build_timestamp_utc` and `git_sha` in the parquet metadata and the
`.meta.json` sidecar. Dated snapshots of published releases are kept under
`snapshots/` from July 2026 onward; earlier published versions were not archived.

## Pipeline changes (unversioned)

These affect the build process, not the dataset contract, so they carry no
dataset-version bump.

- **2026-07-06** — Build-time validation suite added; releases can no longer
  publish with failing checks.

## 1.1.2: 2026-07-06
- **Fixed:** interrupted parse runs could mark PDFs as parsed without persisting
  their records; parse log and raw parquet now flush together (every 100 PDFs and
  at end of run). Failed/missing PDFs are now retried on subsequent runs.
  Backfilled the two PS25 daily gazettes of 2026-06-22/23 that were skipped by the
  old logic — 1 new notice recovered (VN-0770373, National Health Funding Body,
  "Assistant Director, People", daily 2026-06-22); the other 49 vacancy numbers were
  already present via the 2026-06-25 weekly. Release rows 87,191 → 87,192.

## 1.1.1: 2026-07-06
- **Fixed:** AIFS misattribution. Notices printed under the
  "Services Australia (part of the Social Services Portfolio)" gazette header with the
  Australian Institute of Family Studies named in the division field were attributed to
  Services Australia. 116 rows (2020→2026) moved from Services Australia (1,498 → 1,382)
  to AIFS (2 → 118). Releases downloaded before 2026-07-06 carry the misattribution.
  (commit 7964b52)

## 1.1.0: 2026-06
- **Added:** `job_family`, `job_family_confidence`, `job_family_secondary` columns —
  LLM classification (claude-sonnet-4-6, prompt 2025-v2) against the APSC 2025 Job
  Family Framework. This is the initial job-family classification; no earlier
  job-family data was ever published.

## 1.0.0: baseline
- Early schema: 27 columns, no job-family classification.
