#!/usr/bin/env python3
"""
09_check_coverage.py — Report unmapped agency names in the release parquet.

Rows where agency_canonical is null indicate agencies that appear in the gazette
but are not yet in the crosswalk. Exits with 0 (warning only) so the release
still publishes.

In CI, surfaces as a GitHub Actions warning annotation on the step and a table
in the job summary.
"""

import os
import sys
from pathlib import Path

import pandas as pd

RELEASE_PATH = Path("data/release/aps_gazette_vacancies.parquet")

# Known parse failures that cannot be resolved regardless of crosswalk state.
# Excluding these keeps the warning focused on actionable crosswalk gaps.
KNOWN_PARSE_FAILURES = frozenset({"VN-0752313"})


def _github_warning(message: str) -> None:
    print(f"::warning title=Crosswalk gap::{message}", flush=True)


def _append_step_summary(content: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(content)


def main() -> None:
    df = pd.read_parquet(
        RELEASE_PATH,
        columns=["vacancy_no", "gazette_date", "agency", "division", "agency_canonical"],
    )

    null_rows = df[
        df["agency_canonical"].isna()
        & ~df["vacancy_no"].isin(KNOWN_PARSE_FAILURES)
    ]

    if null_rows.empty:
        print("Coverage check: OK — all agency names resolved.")
        return

    total = len(null_rows)

    # Summarise by (agency, division), sorted by agency then row count descending.
    summary = (
        null_rows.groupby(["agency", "division"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["agency", "rows"], ascending=[True, False])
    )

    # ── stdout ───────────────────────────────────────────────────────────────

    print(f"\nWARNING: {total} row(s) with null agency_canonical (crosswalk gaps):\n")
    for agency, group in summary.groupby("agency", sort=True):
        print(f"  agency = {agency!r}")
        for _, row in group.iterrows():
            div = row["division"] if pd.notna(row["division"]) else "(no division)"
            print(f"    division = {div!r}  ({row['rows']} row(s))")
    print(
        "\nAdd new agency names to data/agency_crosswalk.csv "
        "(and pipeline/04_build_crosswalk.py) to resolve.\n"
    )

    # ── GitHub Actions annotation ─────────────────────────────────────────────

    _github_warning(
        f"{total} row(s) have null agency_canonical — "
        "add new agency names to data/agency_crosswalk.csv to resolve. "
        "See job summary for details."
    )

    # ── GitHub Actions job summary ────────────────────────────────────────────

    lines = [
        f"## ⚠️ Crosswalk gaps — {total} unmapped row(s)\n",
        "\n",
        "The following raw gazette agency names could not be resolved to a canonical name. "
        "Add them to `data/agency_crosswalk.csv` (and `pipeline/04_build_crosswalk.py`) "
        "to resolve.\n",
        "\n",
        "| Raw agency | Division | Rows |\n",
        "|------------|----------|------|\n",
    ]
    for _, row in summary.iterrows():
        # Truncate very long strings (e.g. garbled parse bleed into agency field)
        agency_cell = str(row["agency"])[:80]
        div_cell = str(row["division"])[:80] if pd.notna(row["division"]) else ""
        lines.append(f"| `{agency_cell}` | `{div_cell}` | {row['rows']} |\n")

    _append_step_summary("".join(lines))

    sys.exit(0)  # warning only — do not block publication


if __name__ == "__main__":
    main()
