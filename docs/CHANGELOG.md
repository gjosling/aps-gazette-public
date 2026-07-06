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

## 1.3.0 — 2026-07-07
- **Changed:** `description_clean` method v2 (`2026-07-v2`) adds a corpus-wide
  boilerplate pass. Fixes residual May-2025 gazette-template eligibility text that
  the per-agency method could not remove for small agencies (~3.5–5% of
  post-2025Q3 cleaned descriptions affected; now ≈0). The global pass also removes
  the long-standing RecruitAbility standard passage from small-agency ads across
  the whole 2020→present range, so some pre-2025 `description_clean` values change
  too. `description` (raw) is unchanged. Job-family classifications of
  already-classified rows are not re-run.

## 1.2.0: 2026-07-06
- **Added:** `is_affirmative_measure` and `posting_group_id`. Affirmative-measures
  variants of one posting (same agency, base title, classification, closing date)
  now share a linkage key; naive row counts overstate role counts by ~2.6% — see
  the data dictionary for counting guidance. No existing column changed.
  Measured on the 87,340-row release at implementation: 3,452 AM-flagged rows;
  4,775 rows linked into 2,895 posting groups (1,381 multi-row + 1,514 singleton
  AM groups); role keys (distinct `posting_group_id` else `vacancy_no`) = 85,460,
  i.e. 1,880 excess rows (2.2%).

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
