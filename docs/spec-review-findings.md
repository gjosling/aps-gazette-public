# Spec review findings — 2026-07-06

Adversarial review of `specs/01–07` against the actual code and data before implementation.
Reviewer: Claude (spec-review session). Nothing in the specs was taken on trust: every
quantitative claim was re-measured against `data/release/aps_gazette_vacancies.parquet`
(87,191 rows) and the pipeline code; every cited line number was checked against the files.

Baseline: all specs as committed at `0dae784`. My spec edits are reviewable via `git diff`;
the "Spec edits" table at the end lists them. Per the standing rule, nothing here infers
published-release history from local artefacts.

Severity scale: **SB** spec-breaking > **WN** wrong-number > **JD** judgment-disagreement > **SI** simplification.

---

## Part 1 — Directed items

### D1. Spec 06 split weights — removed (done)

The weights procedure (§2, APSED June-2023 headcount shares) is deleted. `agency_lineage.csv`
is now factual events only: predecessor, successor, effective_date, relation ∈ {rename, merge,
split}, note. Replacement documentation text (in the spec, flowing to the data dictionary)
records why: vacancy flows track functions and stand-up timing, not headcount shares, so no
defensible general-purpose weights exist; users must make their own apportionment call at
split boundaries (often the right call is "annotate the break, don't bridge"). The
Antarctic/DEE date-split (§3) is untouched. Validation drops the weight-sum check; keeps
referential integrity, adds relation/date format checks. "Split weights of any kind" is now an
explicit out-of-scope item so the idea doesn't creep back.

Side benefit: this removes spec 06's only external dependency (the sibling `apsed-public`
repo), which was also its worst self-containment failure.

While doing this I checked the source dict and found **the spec's own description of it was
wrong, and the dict itself contains a wrong date** — see findings SB-2/WN-4 below. Both are
now corrected in the spec.

### D2. Spec 01 allowlist — four pairs verified with rows (evidence now inline in the spec)

**(a) ("Australian Public Service Commission", "Parliamentary Workplace Support Service") —
misattribution; NOT permanently allowlisted.** Exactly one row: **VN-0704814**, gazette
2022-05-05, "Executive Assistant - APS 6", raw agency = APSC, division = PWSS, branch null.
The "pre-2023 stand-up ads" justification fails: contemporaneous stand-up ads (VN-0701431
2022-02-24, VN-0702077/VN-0702082 2022-03-10, raw agency "Prime Minister and Cabinet", PWSS in
*branch*) all resolve to PWSS via `BRANCH_OVERRIDE_PAIRS`. This row differs only in that PWSS
sits in *division*, which no crosswalk pass inspects when the raw agency is a directly-matched
canonical (Pass 1, `05_apply_crosswalk.py:250-262`). Under the documented PWSS sub-entity
exception the row should be PWSS. Resolution: kept as a clearly-marked TEMPORARY allowlist
entry (so spec 01 lands green), fixed by new **spec 07 item 7** (row-level override
VN-0704814 → PWSS, then the pair is deleted).

**(b) ("Department of the Treasury", "Australian Taxation Office") — justified; kept.**
Exactly one row: **VN-0687359**, gazette 2021-03-18, job_title "Second Commissioner of
Taxation", branch "Executive", classification_code "Statutory Appointment". This is a
statutory-office appointment gazetted by the portfolio department that runs the appointment
process — not an ATO staff vacancy, and not the AIFS shape (bulk staff misattribution under a
portfolio header). Employing-entity attribution to Treasury is defensible; comment recorded in
the spec. Note the general implication for check 2's maintenance cost in JD-2 below.

**(c) ("Therapeutic Goods Administration", "Independent Hospital Pricing Authority") —
misattribution; NOT permanently allowlisted.** Exactly one row: **VN-0700097**, gazette
2022-02-03 (weekly), "Finance Officer", branch "Office of the CEO", division = IHPA. Checked
against the raw parquet: the *source PDF itself* prints agency = "Therapeutic Goods
Administration" (neighbouring notices parse cleanly; fields are not bled) — so it is a
**gazette-source data-entry error, not a division-field parse artefact**. The role is plainly
IHPA's (Finance Officer in IHPA's Office of the CEO); TGA and IHPA are unrelated bodies.
Allowlisting it permanently would launder a genuine misattribution. Resolution: TEMPORARY
allowlist entry + spec 07 item 7 reattributes VN-0700097 → IHPA and deletes the pair.

**(d) ("IHACPA", "IHPA") — not a rename mismatch; comment corrected; cross-reference
inapplicable.** The two rows (**VN-0735741, VN-0735755**, gazette 2024-02-08) are IHACPA rows
whose *internal division* is literally named "Independent Hospital Pricing Authority Division"
— a unit of IHACPA named after its predecessor. Attribution is correct; the pair is a
**prefix-match false positive of check 2 itself**, and the spec now says so. The directed idea
of cross-referencing spec 06's lineage table was doubly inapplicable: (i) the rows aren't a
rename echo, and (ii) `MOG_CHANGES` did not even contain the IHPA→IHACPA rename — the lineage
table would have had nothing to point at. That missing rename is now a required addition in
spec 06 (see SB-2).

**Pair-count reconciliation: the reported "26 pairs" was wrong twice over.** The committed
spec listed 25 pairs, and my re-run of the specified scan (prefix pool = crosswalk-CSV
canonicals) reproduces **exactly those 25 pairs, 433 rows** — so "26" was a miscount but the
list itself was faithful to the data. However, the *pool choice* was itself flawed: rebuilt
with the module's `CANONICAL_AGENCIES` (171 names vs the CSV's 141), the scan finds 28 pairs /
1,115 rows. The three extra families: (DISR ← DISER-division, 532 rows — deliberate
`CANONICAL_REMAP` echoes, excluded from the pool by the corrected spec), (ACCC ← AER-division,
143 rows — employing-entity convention, AER staff are ACCC employees; now allowlisted with
comment), (ASSEA ← old ASEA name, 7 rows — rename echo; now allowlisted). The spec now
specifies the module pool minus `CANONICAL_REMAP` keys and seeds **27 pairs / 583 rows**, and
explains why the CSV pool is unsafe (see SB-4).

### D3. No-exit claim — CONFIRMED

`06_build_release.py` accumulates failures in `ERRORS` (`fail()` at lines 111–114), prints a
summary at lines 209–211 (`print(f"VALIDATION: {len(ERRORS)} failure(s)")`) and then
**unconditionally** writes both release files at lines 230–231
(`df.to_parquet(PARQUET_OUT...)` / `df.to_csv(CSV_OUT...)`). There is no `sys.exit`, no
`raise`, and no conditional around the writes; `sys` is not even imported. A failing build
publishes. Spec 01's premise and its "17 checks" count both verified (17 printed checks; note
one of them — agency_canonical nulls, lines 127–128 — has no fail branch at all: it prints
`OK` whatever the count is).

---

## Part 2 — Findings, ranked

### Spec-breaking

**SB-1 (spec 01 × 05, fixed by edit): check 6 could never run where spec 01 put it.**
Spec 01 places the boilerplate-residual check in `validate_release`, called from
`06_build_release.py`. But 06's DataFrame comes from `gazette_vacancies_crosswalk.parquet`,
which **has no `description_clean` column** (verified: 27 columns, no clean-text fields —
the column is created by `07_clean_text.py`, which runs *after* 06 and rewrites the release
in place). As written, check 6 would either crash 06 or silently never run; and spec 05's
"one-run lag" note describing 06 as validating "the previous run's cleaning" was wrong —
there is no lag, there is no run. Worse, spec 05 deferred the fix ("add a validate_release
call at the end of 07") to itself, leaving an enforcement hole between spec 01 and spec 05
landing. **Fix applied:** spec 01 now specifies check 6 skips-with-notice when the column is
absent and makes the end-of-07 `validate_release` call a spec-01 deliverable; spec 05's note
is corrected to match.

**SB-2 (spec 06, fixed by edit): the lineage table would have published a factually wrong
date and omitted renames the dataset itself exhibits.**
(i) `MOG_CHANGES["Health_disability_rename"]` says effective `2024-07-01`. The release data
disproves it: last DoHAC row gazetted 2025-05-29, first DHDA row 2025-06-05 — the rename is a
May-2025 event (the May-2025 AAO), not July 2024. Spec 06 said "emit as-is; the note carries
the caveat" — unacceptable for a published data product. (ii) `MOG_CHANGES` contains no
IHPA→IHACPA entry, though both are canonical agencies with released rows (56 and 91 rows,
boundary between 2022-08-25 and 2022-10-06); ditto ASEA→ASSEA. (iii) The derivation rule
"split iff >1 successor, else rename" would label ACLEI→NACC a `rename` although its own note
says "merged into new NACC". **Fixes applied:** correct-the-date instruction (with the data
bounds and verify-the-AAO step), two required rename additions (statutory dates to be
verified at implementation), and an explicit-`relation`-key override so ACLEI→NACC emits as
`merge`.

**SB-3 (spec 06, fixed by edit): the spec misdescribed its own source dict.** "All eight
`MOG_CHANGES` entries … the six renames → 1 row each" — the dict has **nine** entries (two
splits + seven single-successor changes), so the CSV has 11 rows before the additions above.
An implementer following the spec's arithmetic would think they'd mis-parsed the dict.

**SB-4 (spec 01, fixed by edit): check 2's prefix pool was built from the wrong source.**
The spec said to use the distinct `agency_canonical` values of `data/agency_crosswalk.csv`.
That CSV is regenerated manually and currently lacks ~30 canonical names that resolve only via
the live `match_canonical()` fallback — including Airservices Australia, Northern Land
Council and Aboriginal Hostels Limited, all of which have released rows. A future AIFS-class
misattribution whose sub-entity is any of those names would be **invisible to the check that
exists specifically to catch AIFS-class misattributions**. Fix: pool = module
`CANONICAL_AGENCIES` minus the two `CANONICAL_REMAP` stale names; allowlist re-seeded for the
two benign families the wider pool surfaces (D2 above).

**SB-5 (spec 01, fixed by edit): check 3's reconciliation join was underspecified and would
false-FAIL (or silently under-check) on day one.** 34 parsed log rows have
`record_count == 0` (31 holiday dailies, 3 weeklies) and therefore no group in the raw
parquet: the raw parquet has 1,896 `(gazette_id, gazette_date, gazette_type)` groups vs 1,930
parsed log rows. A naive equality join either FAILs on the 34 or, if inner-joined, never
detects a lost group. The spec now requires a bidirectional check with the zero-record
asymmetry called out.

### Wrong numbers

All re-measured against the 2026-07-06 release. Corrected in the spec files where they steer
implementation; the full verification ledger is at the end.

**WN-1 (spec 04, fixed by edit): the suggested `am_linkage` expectations were wrong on their
own data.** Running spec 04's exact algorithm yields **2,891 distinct posting_group_ids**
(1,379 multi-row + 1,512 singleton AM groups) — above the illustrative `max_groups: 2600`, so
the check would WARN on its first ever run. The draft bounded the multi-row-group count, not
the id count the check actually counts. Fixed to 2000/3800 with the measured numbers recorded.
Also filled in the "~85,3xx, compute the exact number" placeholder: **85,313 role keys / 1,878
excess rows (2.2%)**. Note the excess is 2.2%, not the review's "~2.6%" (the review's 2,248
figure used a coarser title normalisation); spec 04's own numbers were consistent.

**WN-2 (spec 05, fixed by edit): "pre-2025 rows must be byte-identical" is false under the
spec's own design.** I simulated the specified global pass (07's actual `split_sentences` /
`normalise`, half-year bins, 20% + 30-title thresholds) over the full release: the
RecruitAbility passage pair is flagged in **every** bin from 2020H1 (57.8–77.9%), so the union
set strips it from pre-2025 ads of sub-`MIN_BIN` agencies — pre-2025 `description_clean`
changes. Defensible (it *is* gazette-wide template text), but the spec claimed the opposite
while specifying a mechanism that guarantees it. The edge case now states what actually
happens and routes the guarantee through the retention-quantile and sample-diff guards; the
changelog entry discloses the pre-2025 changes.

**WN-3 (spec 05, fixed by edit): the 20% global threshold does not mean what the spec said it
means.** The rationale "20% of a corpus-wide bin cannot be role-specific content" is
empirically false: Defence's own recruitment blurb family crosses 20% in seven bins
(20.2–26.8%) purely because Defence is ~a quarter of the gazette. Genuine gazette-wide
families sit at 58–78%. Threshold raised to **0.40** with the measured evidence in the spec —
the target families clear it by 18+ points in every relevant bin; no single-agency family
comes close.

**WN-4 (spec 05, minor, fixed in passing): "raw hit-rate 0.5% pre-2025Q2"** — measured 0.00%
for the marker phrase (2025Q2 itself is 9.9%: the template arrived mid-May). The review's 0.5%
was an overestimate; harmless, but the spec's edge-case bullet now carries the measured value.

**WN-5 (spec 01, fixed by edit): "26 pairs" → the spec listed 25; correct count under the
corrected pool is 27 / 583 rows** (see D2). Also FCA-family row count updated (376 across four
pairs, not "375").

**WN-6 (spec 06, minor, fixed by edit): the data-dictionary edit targeted text that doesn't
exist.** "Update the agency_group note (line ~35) to remove any implication that pre-2022
Antarctic rows sit under DCCEEW" — the note at line 35 never mentions Antarctic rows.
Rewritten as an addition, not a correction.

### Judgment disagreements

**JD-1 (spec 07 item 5): the classification join-key hardening solves a problem with zero
observed instances — recommend cutting it to a tripwire.** Claimed risk: `vacancy_no` reuse
across year boundaries colliding on `(gazette_id, vacancy_no)`. Measured: exactly **6**
vacancy_nos span two calendar years in the raw data, and all six are the *same notice*
republished across the Dec/Jan gazette boundary (PS50/PS1, PS51–PS2) — precisely the case the
dedup already collapses. VN numbers behave as a global monotonic counter; no reuse for a
different vacancy has ever occurred. Against that, item 5 buys: a private-parquet migration, a
5-site key rewrite in 08 (`clf_keys`, reclassify filter, overrides expansion, `make_custom_id`,
public join), and new failure modes in the overrides path. **Recommendation:** replace with a
cheap FAIL check in `validation.py` (duplicate `(gazette_id, vacancy_no)` in the classifications
parquet, or one key matching release rows in ≥2 distinct gazette years → FAIL) and do the
re-keying only if the tripwire ever fires. The null-family-retry half of item 5 and the
"pending retry" log fix are real and should stay. *Not edited — maintainer call; the spec as
written is implementable.*

**JD-2 (spec 01 check 2): FAIL severity means legitimate one-row novelties block Friday
publication.** The Treasury/ATO row shows the pattern: portfolio departments occasionally
gazette statutory-office appointments naming another body in `division`. Each future one is a
broken CI run until a human adds an allowlist pair. I think FAIL is still right — WARN is how
AIFS accreted 116 rows over six years, and the maintainer's stated preference is silent bias >
blocked build — but the cost should be understood: expect roughly one false-positive build
block per year based on the historical rate (~5 single-row portfolio-host pairs over 6 years).
*No change.*

**JD-3 (spec 01 check 1c): agency_canonical nulls as WARN is consistent with its rationale but
inconsistent with check 2.** A new agency the crosswalk can't resolve → WARN (build publishes,
nulls appear); a new agency the crosswalk resolves *wrongly* under a host → FAIL. So the
better-behaved failure mode (visible null) publishes while the worse one (plausible wrong
answer) blocks — which is exactly the right ordering. Noted only because the asymmetry looks
odd until spelled out; suggest a one-line comment in `validation.py`. *No change.*

**JD-4 (spec 05): half-year binning for the global pass is fine; the rejected quarterly
alternative would only sharpen the 2025H1 edge bin.** Verified the residual profile: the
expectations handshake (`since: 2025-07-01`) cleanly avoids the mixed 2025H1 bin, so the
"2Q, decided against quarterly" call stands. *No change.*

**JD-5 (spec 02): retroactive version numbering (1.0.0/1.1.0/1.1.1) is invented history.**
Harmless and clearly labelled, but note the changelog's "1.1.1 — 2026-07-06" describes a
release that carried no version metadata; anyone diffing bytes will find no `1.1.1` stamp.
The CHANGELOG header sentence already says earlier versions weren't archived — suggest one
extra clause "(versions before 1.1.1 are retrospective labels; the files themselves carry no
version metadata)". *Not edited — wording suggestion only.*

### Simplification (cut candidates for a solo-maintained project)

**SI-1 (spec 02 §3): cut the snapshot retention pruner.** The rule (keep 365 days dense, then
first-of-quarter; regex-guarded deletes; dry-run plumbing; paginator walk) exists to save ~3.5
GB/year of R2 storage — about **$0.60/year** at R2 pricing. It is the only code in the whole
spec programme that *deletes published artefacts*, and its failure modes (regex mismatch, date
parsing, pagination) all point at the archive that exists to be the immutable history
substitute. Keep every snapshot; revisit if the bucket ever costs real money. This deletes
roughly a third of spec 02's r2_sync work and its entire deletion-risk surface.

**SI-2 (spec 05 §3): don't push dated audit CSVs to private R2.** The audit CSV is a
deterministic diagnostic, fully reproducible by re-running 07 on any snapshot (which spec 02
now archives). Dynamic glob-push machinery in r2_sync for ~1 MB files nobody has ever needed
remotely is process for its own sake. Keep the dated local archive + the version stamp;
drop the R2 push. (If CI-durability of audits ever matters, attach them to the workflow run
as artifacts — one YAML line.)

**SI-3 (spec 07 item 5): see JD-1 — the tripwire variant deletes a migration, a key rewrite,
and a new private-parquet column.**

**SI-4 (spec 02 metadata): `poppler_version` inside every release write is the only field
that shells out to an external binary from three different scripts.** Keep it, but compute it
in `build_metadata` with full failure-swallowing (the spec already says degrade to
"unknown") — flagging only that this is the one metadata field with a runtime dependency;
resist any temptation to add more shell-outs (e.g. `uv`/pandas versions) later.

Everything else is appropriately lean: spec 03 is minimal and correct; spec 01's
`--print-current` earns its place (it powers the bounds policy); spec 04 already rejected the
over-engineered PN-linkage design with data.

---

## Verification ledger

**Verified — data claims** (re-measured 2026-07-06 against the release/raw parquets):

| claim | spec | result |
|---|---|---|
| release 87,191 rows / 30 cols | 01/02 | exact |
| AIFS 118; eSafety ≥196 (now 201); agency_canonical nulls = 2 (Various 2020; Health/Hearing Australia 2020) | 01 | exact |
| division-mismatch scan (CSV pool): 25 pairs listed / 433 rows | 01 | exact (the "26" prose was the error) |
| parse log: 1,930 parsed; Σrecord_count = 170,512 = raw rows; record_count == vn_count all; 2 missing_pdf, 0 error; verbatim PS25 rows present | 01/03 | exact; **plus**: 34 parsed rows have record_count 0 (SB-5), and both "missing" PS25 PDFs are in fact present in `data/pdfs/` (backfill step 1 is trivially satisfied) |
| 2,080 PDFs in data/pdfs | 03 | exact |
| AM rows 3,448; 1,379 multi-row groups (aug. key); AUSTRAC 2025-05: 34 rows → 12 role keys; PN non-null 98.8% | 04 | exact (AUSTRAC acceptance test re-run with the spec's verbatim regexes) |
| raw template hit ~77% from 2025Q3; clean residual 3.5→5.1%; 378 residual rows since Apr 2025; check-6 population 3.72% < 6.0 ceiling | 01/05 | exact (per-quarter: 76.7/75.3/77.9/75.1/62.7) |
| pre-2022-07 DCCEEW = 75 (73 Antarctic + 2 DEE) | 06 | exact |
| classification_code nulls 3,098; ACECQA 252/252; ANSTO 203/203; NHVR 179/182; CSIRO 47.6%; CASA 30.7%; AFP 24.3%; Statutory Appointment 327 | 07 | exact |
| 94 not_found manifest dates; 25 null descriptions; 456 daily rows | 07 | exact |
| job_family_confidence 51,701 / 34,231 / 1,259 | (review) | exact |
| empty strings in release = 0; duplicate vacancy_no = 0 | 01 | exact |

**Verified — code claims:** every line reference cited in specs 01–07 was checked against the
files and is correct, including: 06's no-exit validation block (D3); r2_sync `_push_one`
size-skip 126–140 / multipart 8 MB at :82 / docstring 24–26; 02_parse log-append :589, parquet
write :619, `_load_parse_log` :465–470 status-blind, docstring :20–21, `--full` clear
:503–506, log schema; 07_clean_text constants (0.30 / MIN_BIN 10 / min-titles 3 / `2Q`), flags
(`--threshold`, `--min-titles`, `--dry-run`), retention-quantile and fully-stripped prints,
`_clean` :311–315, AUDIT_CSV path+columns, PII-after-clean ordering, `clean_description` →
None; 08_classify PROMPT_VERSION :69, model `claude-sonnet-4-6`, key sites :120–122 /
:407–412 / :416–418 / :546–548, join :348, null-family rows written vs errored rows not
(:293–306), "pending retry" log :531–532, `ALL_CLASSIFICATION_COLS`, `--dry-run`; 01_download
:174–176 / :259–271, `(date,"none")` sentinel, `--no-skip` / `--year-start`; CI workflow step
"Parse PDFs" exists (spec 01's insertion point is valid), poppler-utils unpinned at line 27;
pandas 3.0.3 `to_parquet` has no key-value-metadata argument (spec 02's pyarrow detour is
necessary); `prompts/versions.json` absent; `gazette_year` absent from the classifications
parquet; PUSH_PUBLIC/PUSH_PRIVATE = the six push objects the addendum describes.

**Wrong (all now corrected in the specs):** SB-1…SB-5, WN-1…WN-6 above.

**Could not verify — and what it would take:**

| item | why | what's needed |
|---|---|---|
| Published R2 objects match local release; AIFS fix live | standing rule: no publication-lineage inference; no R2 access from this session | maintainer statement (addendum §2) is the evidence of record — taken on trust |
| Statutory effective dates: May-2025 AAO (DHDA), IHACPA establishment, ASEA→ASSEA rename, PWSS Act commencement | no external lookup performed this session | check the AAO / legislation register at implementation; spec 06 now bounds each date with in-data evidence and requires verification before emitting |
| Spec 04's 30-group hand-check (28/30 correct, 2 over-merges fixed by classification key) | archived in `/tmp/gazette_review/` — ephemeral, gone | cannot be re-examined; the in-spec AUSTRAC acceptance test passes and the corpus-level statistics reproduce, so the design conclusion stands on re-derivable evidence; re-draw a fresh sample at implementation (spec already requires a 10-group spot-check) |
| Review's holdout calibration (high 97.9% / medium 64.9%) | validation CSVs exist in `validation/` but re-auditing the label comparison was out of scope | re-run `validation/refinement_comparison_report.md`'s numbers if job_family checks are ever added to the suite |
| r2_sync checksum behaviour against real R2 (ETag-vs-multipart claim) | requires credentials + live bucket | test in CI on first spec-02 push (the claim matches S3/R2 documented behaviour) |
| ASD employment basis (spec 07 seed list "verify and include if own-Act") | legislation question, flagged inline by the spec itself | resolve at implementation with a citation, per the spec's own rule |

**Additional observation (no spec change):** the seeded `NON_PS_ACT_EMPLOYERS` names in spec
07 were checked against the *release's* canonical values — all 27 seed names correspond to
canonicals that exist (via module list or live fallback), including three absent from the
committed crosswalk CSV (Airservices Australia, Northern Land Council, Aboriginal Hostels
Limited). The same CSV-staleness that motivated SB-4 applies to any implementation that
validates the seed list against the CSV: validate against `CANONICAL_AGENCIES` instead.

---

## Spec edits made this session

| spec | edit | reason |
|---|---|---|
| 01 | allowlist reseeded: 27 pairs / 583 rows, per-pair evidence comments, TEMPORARY block for the two 1-row misattributions; "26 pairs" corrected | directed item 2; WN-5 |
| 01 | check 2 prefix pool: crosswalk-CSV → module `CANONICAL_AGENCIES` minus `CANONICAL_REMAP` keys | SB-4 |
| 01 | check 3: bidirectional join + 34 zero-record-PDF asymmetry specified | SB-5 |
| 01 | check 6: placement caveat (column absent in 06), skip-with-notice, end-of-07 `validate_release` call made a spec-01 deliverable; deliverable 4 updated | SB-1 |
| 02 | *(none — SI-1 and JD-5 left as recommendations)* | |
| 03 | *(none — verified clean; note the two PS25 PDFs are already in `data/pdfs/`)* | |
| 04 | `am_linkage` bounds 2000/3800 with measured 2,891 ids; measured role-key numbers (85,313 / 1,878 / 2.2%) filled in | WN-1 |
| 05 | GLOBAL_THRESHOLD 0.20 → 0.40 with measured rationale; byte-identical claim replaced with what actually happens (RecruitAbility strips pre-2025); "0.5% pre-2025Q2" corrected to 0.0%; changelog + dictionary wording updated; the wrong "one-run lag" note corrected | WN-2, WN-3, WN-4, SB-1 |
| 06 | split-weights procedure removed end-to-end; §2 replaced with the why-no-weights rationale; validation/commands/docs/changelog/out-of-scope updated | directed item 1 |
| 06 | `MOG_CHANGES` corrections required before emitting: DHDA date (data-disproven), IHPA→IHACPA and ASEA→ASSEA renames added, explicit `relation: merge` for ACLEI→NACC; entry/row counts fixed (9 entries → 11 rows); dictionary edit re-scoped to an addition | SB-2, SB-3, WN-6 |
| 07 | new item 7: `VACANCY_NO_OVERRIDES` reattributing VN-0704814 → PWSS and VN-0700097 → IHPA, then deleting the two temporary allowlist pairs; item count and migration table updated | directed item 2 (a), (c) |

## Things that should be cut (plainly)

1. **Spec 02's snapshot retention pruner** (SI-1) — the only artefact-deleting code in the
   programme, protecting ~$0.60/year. Cut it; keep every snapshot.
2. **Spec 05's dynamic R2 push of dated audit CSVs** (SI-2) — reproducible diagnostics don't
   need durable remote archival. Keep the dated local files.
3. **Spec 07 item 5's re-keying migration** (JD-1/SI-3) — zero observed collisions in six
   years of data; a one-line tripwire check gives the same protection without touching five
   key paths and the private parquet. Keep the null-family-retry half.

No whole spec should be cut. Specs 01, 03, 04 and 06 (as amended) are tight; spec 02 is sound
minus the pruner; spec 05's design survived adversarial measurement once its thresholds and
claims were corrected; spec 07 is a grab-bag by design and its weakest item is flagged above.

---

## Maintainer decisions — addendum, 2026-07-06

Recorded after the maintainer reviewed the findings above. Both decisions are now applied as
spec edits (second round; same session).

**1. Spec 06 statutory dates — settled by external verification (maintainer-supplied
sources); the "verify before committing" blockers are replaced with fixed values:**

- DoHAC → Department of Health, Disability and Ageing: **effective 2025-05-13**
  (Administrative Arrangements Order made 13 May 2025; ANAO portfolio page and the PM&C AAO
  page concur). "Tentative" wording dropped.
- IHPA → IHACPA: **effective 2022-08-12** (National Health Reform Act 2011 amendments
  commenced 12 August 2022, per IHACPA's own FAQ).
- ASEA → ASSEA: **effective 2023-12-15** (commencement per the Fair Work Legislation
  Amendment (Closing Loopholes) Act 2023; Finance PGPA newsletter 99 and the DEWR amendments
  document both state 15 December 2023). A caution is recorded in the spec: DEWR's overview
  page says 7 December 2023 — that is the amending Act's *passage*, not commencement; do not
  "correct" to it.

Spec 06 additionally now states (docs section + a comment beside `MOG_CHANGES`) that lineage
`effective_date` values are **statutory** dates and that gazette usage lags renames by weeks
(IHPA-named rows appear until 2022-08-25, after the 2022-08-12 rename) — so date-bounded
heuristics from row data run late, and lineage-table users should expect old names to persist
briefly past each effective date. This resolves the corresponding "could not verify" rows in
the ledger above.

**2. All three recommended cuts accepted and converted from recommendations into spec edits:**

- **Spec 02 (SI-1):** snapshot retention pruner removed entirely — snapshots are kept
  indefinitely and the sync code deletes nothing, ever. The retention paragraph, the
  would-delete `--dry-run` behaviour, and the snapshot-key-regex delete guard are gone (the
  guard is moot when nothing is deleted). Rationale recorded in the spec: the only
  artefact-deleting code in the programme protected ~$0.60/year of storage. Deliverable 4,
  the edge-case list, the data-dictionary snapshots note ("kept indefinitely"), the r2_sync
  docstring instruction, and the validation commands were updated to match.
- **Spec 05 (SI-2):** R2 push of dated audit CSVs removed; audits remain local dated files
  only. Rationale recorded in the spec: regenerable from committed code + raw data (and, once
  spec 02 lands, from any archived snapshot). README doc instruction updated ("kept locally;
  regenerable by re-running 07").
- **Spec 07 item 5 (JD-1/SI-3):** the `(gazette_id, vacancy_no, gazette_year)` re-keying
  migration is replaced by the collision tripwire: at join time in `08`, exit 1 if the
  classifications parquet contains a duplicated `(gazette_id, vacancy_no)` key or any key
  matches release rows in ≥2 distinct gazette years. Rationale recorded in the spec: zero
  collisions observed in six years; the six multi-year vacancy_nos are the same notice
  republished over New Year. The null-family retry and the "pending retry" log fix stay. The
  migration table, 1.5.0 changelog wording, and validation commands were updated (the 1.5.0
  entry also now mentions item 7's two row reattributions, previously only in item 7's own
  text).
