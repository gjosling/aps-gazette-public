# APS Gazette Dataset

A Python pipeline that downloads Australian Public Service Gazette PDFs, parses structured vacancy notices, normalises agencies across machinery-of-government changes, and produces a clean parquet dataset.

The dataset currently covers vacancy notices from January 2020 onwards across APS agencies and is updated weekly via CI.

## Dataset

The release dataset is available in two formats:

- [Parquet](https://data.foiforest.org/gazette/aps_gazette_vacancies.parquet)
- [CSV (gzipped)](https://data.foiforest.org/gazette/aps_gazette_vacancies.csv.gz)
- [Metadata sidecar (JSON)](https://data.foiforest.org/gazette/aps_gazette_vacancies.meta.json) — build provenance and per-file SHA-256
- [Changelog](https://data.foiforest.org/gazette/CHANGELOG.md) — dataset version history

See [`docs/data_dictionary.md`](docs/data_dictionary.md) for column descriptions and known limitations.

Each published release also embeds build metadata (dataset version, build timestamp, git SHA, tool versions) in the parquet and the sidecar JSON. A dated snapshot of every release is kept indefinitely under `snapshots/` — dated copies begin July 2026.

This dataset records vacancy advertisements published in the APS Gazette. It represents agency hiring activity (positions advertised for recruitment) not workforce composition. Vacancy volumes and proportions reflect a combination of workforce growth, turnover replacement, and recruitment channel choices. They should not be interpreted as a proxy for agency staffing levels or workforce structure. For current APS workforce composition, see the [APSED headcount dataset](https://github.com/gjosling/apsed-public). Affirmative-measures variants of the same posting are linked by `posting_group_id`; count distinct `posting_group_id` (falling back to `vacancy_no`) for role-level counts.

The `job_family`, `job_family_confidence`, and `job_family_secondary` columns are **model-derived.** They are not sourced from the gazette and do not reflect any APSC classification of these roles. They are assigned by an LLM (`claude-sonnet-4-6`) against the [APSC 2025 Job Family Framework](https://www.apsc.gov.au/initiatives-and-programs/aps-workforce-strategy-2025/workforce-planning-resources/aps-job-family-framework) and should be treated as a best-effort signal, not ground truth. Overall accuracy on a 200-ad hand-labelled validation sample is 88%; see the data dictionary for detail.

## Pipeline

Scripts are numbered in execution order:

| Script | What it does |
|--------|-------------|
| `01_download.py` | Probe the APSC S3 bucket for gazette PDFs and download them locally; recently not-found dates are re-probed for 28 days before becoming a permanent skip |
| `02_parse.py` | Extract structured vacancy notices from each PDF using pdftotext; outcome notice PDFs are skipped. Parse state (`data/parse_log.csv`) and the raw parquet are flushed together in batches; failed or missing PDFs are retried on the next run |
| `03_normalise.py` | Derive salary_min/max, closing_date, duties_text, location_normalised, classification_clean |
| `04_build_crosswalk.py` | Define agency name normalisations (MoG changes, renames, merges); emit `agency_lineage.csv` |
| `05_apply_crosswalk.py` | Apply the crosswalk with date-aware resolution |
| `06_build_release.py` | Deduplicate (drop daily rows where a weekly version exists), run release validation (`pipeline/validation.py`, with bounds in the committed `data/expectations.json`) that **fails the build and blocks publication** on any failing check, rename columns, convert gazette_date to date, derive `is_affirmative_measure` and `posting_group_id`, write the release parquet |
| `07_clean_text.py` | Detect and strip boilerplate from `description`, producing `description_clean` — a per-agency frequency pass plus a global corpus-wide pass that removes gazette-wide template text (RecruitAbility, the May-2025 eligibility passage) even from small agencies; redact email addresses, phone numbers, and contact officer names from description fields; write the boilerplate audit trail as both `data/diagnostics/boilerplate_sentences.csv` (stable name) and a dated archive `data/diagnostics/boilerplate_sentences-<version>-<date>.csv` (kept locally; regenerable by re-running 07) |
| `08_classify_job_family.py` | Classify each vacancy into one of 16 APSC 2025 Job Families using Claude (`claude-sonnet-4-6`) via the Anthropic Batch API. Only processes rows not already in `data/job_family_classifications.parquet`. |
| `09_check_coverage.py` | Check the release parquet for null `agency_canonical` values (crosswalk gaps); emit GitHub Actions warning annotations and a job summary. Does not modify data. |

Each script reads from and writes to `data/`. To run the full pipeline locally:

```bash
uv sync

# System dependency: pdftotext
# Ubuntu/Debian: sudo apt install poppler-utils
# macOS: brew install poppler

# Download gazette PDFs (takes a while - covers weekly and daily gazettes back to 2020)
uv run python pipeline/01_download.py --year-start 2020 --year-end 2026

# Parse, normalise, crosswalk, release
uv run python pipeline/02_parse.py batch
uv run python pipeline/03_normalise.py
uv run python pipeline/05_apply_crosswalk.py
uv run python pipeline/06_build_release.py
uv run python pipeline/07_clean_text.py
uv run python pipeline/08_classify_job_family.py
```

The release parquet is written to `data/release/`. Step 08 requires an `ANTHROPIC_API_KEY` environment variable (or `.env` file). On first run it will classify all rows via the Anthropic Batch API. Subsequent runs only process new rows using the Anthropic Messages API. Step 04 (build crosswalk) is not needed for a normal run, as the crosswalk CSV is committed and only needs regenerating when new agency names appear in the gazette.

## Repository structure

```
pipeline/    Pipeline scripts (01–09) and r2_sync.py
data/        Agency crosswalk and job family overrides (committed); intermediate
             and release parquet files are generated locally and excluded from git
docs/        Data dictionary
prompts/     LLM prompts used by the pipeline
```

## Gazette URL patterns

The APSC publishes gazette PDFs to a public S3 bucket at:

```
https://s3-ap-southeast-2.amazonaws.com/apsc.gazette/{filename}.pdf
```

| Era | Type | Naming pattern |
|-----|------|---------------|
| 2023–present | Weekly vacancy | `PS{N} Weekly Gazette Thursday (Vacancy Notices)  - {DD} {Month} {YYYY}.pdf` |
| 2023–present | Weekly outcome | `PS{N} Weekly Gazette Thursday (Outcome Notices)  - {DD} {Month} {YYYY}.pdf` |
| 2023–present | Daily | `PS{N} Daily Gazette {Weekday} - {DD} {Month} {YYYY}.pdf` |
| 2020–2022 | Weekly (combined) | `PS{N} Weekly Gazette Thursday - {DD} {Month} {YYYY}.pdf` |

Note the **double space** before the dash in post-2023 weekly filenames.

`{N}` is the PS number: resets to 1 each January, increments roughly weekly. The download script probes ±2 offsets around the expected value to handle variations.

### Pre-S3 backlog (prior to 2020)

The S3 bucket starts from January 2020. For gazette issues prior to 2020:

- The [Pandora archive](https://pandora.nla.gov.au/tep/75984) covers gazettes prior to 19 December 2019.
- The [NLA digitised collection](https://nla.gov.au/nla.obj-2566499518) covers January 2020 onwards and may be useful as a backup source.

The older PDF format is substantially different (combined gazette covering vacancies, engagements, promotions, movements, and separations) and would require a separate parser.

## Agency normalisation

Agency names in the gazette reflect whatever name the agency was using at the time of publication. The crosswalk (`data/agency_crosswalk.csv`) maps every observed name to a normalised current name, handling department renames, machinery-of-government mergers and splits, and minor variations.

The crosswalk is maintained manually. When new agency names appear in the gazette (typically after a machinery-of-government change), I update the mapping rules in `04_build_crosswalk.py` and regenerate the CSV.

## Linking to APSED

The [APSED headcount dataset](https://github.com/gjosling/apsed-public) records APS headcount by agency and job family at biannual snapshots. Combining the two datasets lets you contextualise vacancy volumes against workforce size.

### How to join

**1. Pick an APSED snapshot.** The agency × job family table starts from June 2024. That is the earliest point at which an agency-level join is possible.

**2. Aggregate gazette vacancies over a period.** A natural choice is the 12 months leading up to the snapshot date.

**3. Join on `agency_canonical` (both datasets) and `job_family` ↔ `job_family_key` (gazette ↔ APSED).**

```python
import pandas as pd

gazette = pd.read_parquet("aps_gazette_vacancies.parquet")
gazette["gazette_date"] = pd.to_datetime(gazette["gazette_date"])
apsed   = pd.read_csv("apsed_agency_jf.csv", parse_dates=["snapshot_date"])

# Step 1: pick a snapshot
snapshot = apsed[apsed["snapshot_date"] == "2024-06-30"]

# Step 2: count gazette vacancies in the 12 months prior
period = gazette[
    (gazette["gazette_date"] >= "2023-07-01") &
    (gazette["gazette_date"] <  "2024-07-01")
]
counts = (
    period
    .groupby(["agency_canonical", "job_family"])
    .size()
    .reset_index(name="vacancy_count")
)

# Step 3: join with agency_canonical and gazette job_family ↔ APSED job_family_key
merged = counts.merge(
    snapshot[["agency_canonical", "job_family_key", "headcount"]],
    left_on=["agency_canonical", "job_family"],
    right_on=["agency_canonical", "job_family_key"],
    how="left",
)
```

Most APS agencies will match cleanly on `agency_canonical`. Unmatched rows are expected (see below).

### Caveats

**Agencies outside PS Act scope (roughly one in ten gazette rows — 10.6% — flagged by the `ps_act_employer` column).** AFP, CASA, ASIC, parliamentary departments and Commonwealth companies engage staff outside the *Public Service Act 1999*. These — along with the courts and tribunals (whose registry staff APSED also excludes) — advertise vacancies in the gazette but are not included in APSED, so they will be left-join nulls.

**Reliable agency join window: Jun 2024 onwards.** The APSED agency × job family table begins in June 2024. Gazette data goes back to January 2020, but there is no APSED counterpart before June 2024.

**Four APSED job families have no gazette join key.** `Science and Health`, `Monitoring and Audit`, `Senior Executive`, and `No data` have a null `job_family_key` and cannot be joined to gazette rows. See the [APSED README caveats](https://github.com/gjosling/apsed-public#key-caveats) for more detail.

## CI

GitHub Actions runs the pipeline weekly (Fridays) and can be triggered manually via `workflow_dispatch`.

A validation step after parsing reconciles the parse log against the raw parquet; release checks in step 06 block publication on failure.

R2 is used as the persistent state store across runs. At the start of each run, `r2_sync.py pull` restores the manifest, parse log, accumulated raw parquet, and job family classifications from R2 so the pipeline only processes new gazettes and new vacancies. After downloading, `r2_sync.py push-pdfs` archives any new PDFs to the private R2 bucket under `pdfs/` (already-archived PDFs are skipped). At the end, `r2_sync.py push` persists the updated state files (including classifications) and publishes the release parquet, CSV, metadata sidecar, and changelog. Uploads are compared by SHA-256: each object's local hash is checked against a `sha256` metadata value on the remote object, and a file whose hash already matches is skipped (R2/S3 ETags are not content hashes for multipart uploads, so they are not used). Each push also writes a dated snapshot of the published release under `snapshots/` (kept indefinitely; the sync never deletes). PDFs are otherwise transient and are discarded when the runner exits.

CI requires eight repository secrets: `R2_ACCOUNT_ID` (shared), `R2_PRIVATE_ACCESS_KEY_ID`, `R2_PRIVATE_SECRET_ACCESS_KEY`, `R2_PRIVATE_BUCKET` (pipeline state, PDF archive, and job family classifications), `R2_PUBLIC_ACCESS_KEY_ID`, `R2_PUBLIC_SECRET_ACCESS_KEY`, `R2_PUBLIC_BUCKET` (release files), and `ANTHROPIC_API_KEY` (job family classification via Claude).

## Full reparse

If the parser changes in a way that affects existing records, rebuild the dataset from the archived PDFs:

```bash
# Restore pipeline state (manifest, parse log, raw parquet) from R2
uv run python pipeline/r2_sync.py pull

# Restore all archived PDFs from the private R2 bucket
uv run python pipeline/r2_sync.py pull-pdfs

# Reparse everything, ignoring the parse log
uv run python pipeline/02_parse.py batch --full

# Continue with normalise, crosswalk, release as normal
uv run python pipeline/03_normalise.py
uv run python pipeline/05_apply_crosswalk.py
uv run python pipeline/06_build_release.py
uv run python pipeline/07_clean_text.py

# Re-join existing job family classifications into the release parquet.
# Classifications are restored from R2 at the start; this is a no-op for
# already-classified rows. Only vacancies newly surfaced by the reparse need API calls.
uv run python pipeline/08_classify_job_family.py
```

After verifying the output, push updated state files to R2 with `r2_sync.py push`.

## Interpreting longitudinal trends

Three types of machinery-of-government change create discontinuities that affect longitudinal analysis. These reflect real structural changes to the APS, but they require care when comparing agency-level data across time.

**Agency splits and merges.** `agency_canonical` reflects the name at the time of publication. Pre-split vacancies appear under the predecessor, post-split vacancies under the relevant successor. `agency_group` provides a stable label that groups an agency with its primary functional successor across renames and partial splits (e.g. DAWE and DAFF both map to `"Agriculture department"`), but it does not fully resolve genuine functional splits. The two major splits in this dataset's time range are the July 2022 MoG changes: DAWE → DAFF + DCCEEW, and DESE → Department of Education + DEWR. For both, there is no clean solution. Pre-split DESE vacancies covered both education and employment functions, and there is no principled way to attribute them to one successor at the agency level. Rather than group DESE with both successors, DESE and Department of Education share a group; DEWR has its own. The same applies to the DAWE split: DAWE and DAFF share a group; DCCEEW does not.

A machine-readable lineage table (`agency_lineage.csv`, published beside the release) encodes these changes — factual predecessor/successor events only, no split weights; see the data dictionary for why — generated from the `MOG_CHANGES` table in `pipeline/04_build_crosswalk.py`.

**Function transfers.** Branches or divisions can transfer between agencies without either agency being renamed. These transfers are invisible in the data. Both agencies continue to exist, but their functional scope changes. A sudden shift in an agency's job family composition may reflect a function moving in or out rather than a genuine change in hiring priorities.

**Portfolio reshuffles.** An agency may move between portfolios without changing its own functions. This does not affect `agency_canonical` or `agency_group` but does affect any analysis that groups by portfolio.

## Known issues

- Christmas skeleton gazettes (PS51/52 in late December) contain 0–3 notices. They parse correctly but produce near-empty output. This is the source data, not a bug.
- Salary normalisation applies guard rules to handle known data quality issues (inflated values, inverted min/max, hourly/annual mismatches). See the data dictionary.
- Some agencies put informal contact details inside the job description body text rather than in the structured "Position Contact" field. Where this occurs, email addresses, phone numbers, and contact officer names in the description body are redacted (see the data dictionary). The structured contact fields are excluded from the dataset per the gazette's CC BY 4.0 licence terms.

## Source data

Vacancy notices are published in the APS Gazette at [https://www.apsjobs.gov.au/s/gazette-information](https://www.apsjobs.gov.au/s/gazette-information). The gazette is published weekly (Thursdays) with daily supplements.

## Licence

Code: [MIT](LICENSE)

The underlying vacancy notice data is published in the APS Gazette, administered by the Australian Public Service Commission. Vacancy notices are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) per the [gazette information page](https://www.apsjobs.gov.au/s/gazette-information). Personal information (contact officer names, phone numbers, email addresses) is excluded from this licence and is not included in the dataset.

## Development

The code in this repository was written with the assistance of a large language model, working from my specifications. The pipeline design, parsing strategy, normalisation rules, and data quality decisions are my own. All code was reviewed, tested, and iterated by me. The model's role in job family classification is documented separately in the data dictionary.

## Contact

Gabrielle Josling ([mindyourowndata.org](https://mindyourowndata.org))
