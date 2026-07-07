# Review Addendum — 2026-07-06

Addendum to `/tmp/gazette_review/review.md`. Records maintainer corrections and post-review verification outcomes. No code changes.

## 1. Question 3 (classification history) — RESOLVED, premise corrected

The review inferred from local and sibling-repo artefacts that a pre-June-2026 classification history might have been overwritten. **That inference was wrong.** Per the maintainer:

- The `topic_id` / `topic_label` / `topic_category` columns found in local files belong to a sibling-repo / abandoned experimental line and were **never in any published release**.
- Publication lineage must not be inferred from local or sibling-repo artefacts. R2 has no object versioning, so published history is not recoverable from the bucket; the maintainer's statement is the evidence of record.
- **Only one version of `job_family` has ever been published.** The June 2026 `classified_at` window (2026-06-19 → 2026-07-01) is the **initial** job_family classification. Nothing was overwritten.

## 2. Question 8 (publishing the AIFS fix) — RESOLVED

- The AIFS crosswalk fix (commit `7964b52`) is **live** in the published release (pushed 2026-07-06 08:14 AEST).
- An independent local rebuild (pipeline steps 05→08) **reproduced the published release exactly (size-identical; content verified on key counts)** — `r2_sync push` found all six objects unchanged and uploaded nothing. The published parquet was also independently downloaded and its key counts confirmed to match the rebuild.
- Verified numbers: **AIFS = 118 rows** (threshold ≥ 118), **Services Australia = 1,382 rows** (~116 below the pre-fix 1,498), **zero duplicate `vacancy_no`**, and **all 17 `06_build_release.py` validation assertions passed**.

## 3. R2 object versioning — not available

Cloudflare R2 does not support S3-style object versioning (`ListObjectVersions` returns `NotImplemented`; the feature does not exist on R2). **Planned substitute: dated snapshot keys pushed from CI (spec 02).**

## 4. Observation: r2_sync upload-skip heuristic

`r2_sync.py` skips uploads when the local file size equals the remote `Content-Length`. Size equality is a weak identity check — files can differ at identical sizes. **Checksum comparison recommended (spec 02/07).**
