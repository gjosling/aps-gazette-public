#!/usr/bin/env python3
"""
04_build_crosswalk.py — generates data/agency_crosswalk.csv

Maps every distinct raw agency string found in the parsed vacancy data to:
  agency_canonical  — entity name at time of the gazette notice (date-aware)
  agency_group      — stable label for time-series (groups pre/post MoG-split entities)
  match_method      — how the match was found

Handles:
  - Exact and prefix matches against a canonical agency list
  - Portfolio-prefix stripping ("Defence Department of Defence..." → "Department of Defence")
  - Rows where agency = source filename (*.pdf): uses division+branch as lookup key
  - Machinery of Government changes (Jul 2022 election transition, 2023 NACC, 2024 ART)
  - Portfolio-name-only strings where the real entity is in 'division' (flagged "division_required")

Usage:
    python pipeline/04_build_crosswalk.py           # writes data/agency_crosswalk.csv
    python pipeline/04_build_crosswalk.py --dry-run # print uncertain mappings only

Note: data/agency_crosswalk.csv is committed to the repo. Re-run this script only
if you update the canonical list or matching rules below.
"""

import re
import sys
import csv
import pandas as pd
from pathlib import Path

# ============================================================
# SECTION 1 — CANONICAL AGENCY MASTER LIST
#
# One entry per operationally distinct APS entity.
# Entities under a portfolio department that have a separate
# CEO/Secretary, annual report, and distinct function get their own row.
# For MoG-renamed entities, BOTH old and new names appear so prefix
# matching resolves correctly in either direction.
#
# Sorted longest-first at load time for greedy prefix matching.
# ============================================================

CANONICAL_AGENCIES = [
    # ── DEFENCE PORTFOLIO ───────────────────────────────────────────────
    "Department of Defence",
    "Australian Signals Directorate",
    "Australian War Memorial",
    "Defence Housing Australia",
    "Australian Submarine Agency",
    "Australian Naval Nuclear Power Safety Regulator",

    # ── ATTORNEY-GENERAL'S PORTFOLIO ────────────────────────────────────
    "Attorney-General's Department",
    "Australian Federal Police",
    "Australian Criminal Intelligence Commission",
    "Australian Security Intelligence Organisation",
    "Australian Transaction Reports and Analysis Centre",
    "Australian Human Rights Commission",
    "Australian Government Solicitor",
    "Commonwealth Ombudsman",
    "Office of the Australian Information Commissioner",
    "Office of Parliamentary Counsel",
    "Federal Court of Australia",
    "Federal Circuit and Family Court of Australia",
    "Administrative Appeals Tribunal",            # abolished 2024 → ART
    "Administrative Review Tribunal",             # from 2024
    "National Anti-Corruption Commission",        # from Jul 2023 (replaced ACLEI)
    "Australian Commission for Law Enforcement Integrity",   # until Jul 2023
    "Australian Financial Security Authority",
    "Office of the Inspector-General of Intelligence and Security",
    "Director of Public Prosecutions",

    # ── HOME AFFAIRS PORTFOLIO ──────────────────────────────────────────
    "Department of Home Affairs",
    "National Emergency Management Agency",

    # ── TREASURY PORTFOLIO ──────────────────────────────────────────────
    "Department of the Treasury",
    "Australian Taxation Office",
    "Australian Bureau of Statistics",
    "Australian Securities and Investments Commission",
    "Australian Prudential Regulation Authority",
    "Australian Office of Financial Management",
    "Royal Australian Mint",
    "Productivity Commission",
    "Australian Competition and Consumer Commission",
    "Australian Energy Regulator",

    # ── FINANCE PORTFOLIO ───────────────────────────────────────────────
    "Department of Finance",
    "Future Fund Management Agency",
    "Digital Transformation Agency",
    "Parliamentary Workplace Support Service",
    "Independent Parliamentary Expenses Authority",
    "Australian National Audit Office",

    # ── PRIME MINISTER AND CABINET PORTFOLIO ────────────────────────────
    "Department of the Prime Minister and Cabinet",
    "Australian Public Service Commission",
    # Full name matches before the truncated "Australian Institute of Aboriginal and Torres Strait" form
    "Australian Institute of Aboriginal and Torres Strait Islander Studies",
    "National Indigenous Australians Agency",
    "Aboriginal Hostels Limited",
    "Torres Strait Regional Authority",
    "Northern Land Council",
    "Central Land Council",
    "Workplace Gender Equality Agency",
    "Office of the Official Secretary to the Governor-General",
    "Office of National Intelligence",
    "National Archives of Australia",

    # ── HEALTH PORTFOLIO ────────────────────────────────────────────────
    # MoG Jul 2022: "Department of Health" → "Department of Health and Aged Care"
    # Both entries kept so prefix matching works for either era.
    "Department of Health and Aged Care",         # from Jul 2022
    "Department of Health, Disability and Ageing",  # late-2024 rename variant
    "Department of Health",                       # pre-Jul 2022 (shorter, must come AFTER longer forms)
    "Therapeutic Goods Administration",
    "Australian Institute of Health and Welfare",
    "Australian Digital Health Agency",
    "Aged Care Quality and Safety Commission",
    "Australian Radiation Protection and Nuclear Safety Agency",
    "Australian Commission on Safety and Quality in Health Care",
    "National Health and Medical Research Council",
    "National Blood Authority",
    "National Health Funding Body",
    "Organ and Tissue Authority",
    "Cancer Australia",
    "Food Standards Australia New Zealand",
    "National Mental Health Commission",
    "Health Professional Services Review",
    "Independent Health and Aged Care Pricing Authority",
    "Independent Hospital Pricing Authority",     # earlier name
    "Office of the Inspector-General of Aged Care",
    "Australian Centre for Disease Control",
    "Australian Sports Commission",

    # ── INDUSTRY PORTFOLIO ──────────────────────────────────────────────
    # MoG Jul 2022: DISER → DISR (energy & resources moved to DCCEEW)
    "Department of Industry, Science, Energy and Resources",   # pre-Jul 2022
    "Department of Industry, Science and Resources",           # from Jul 2022
    "Department of Industry Science and Resources",            # no-comma variant in data
    "Commonwealth Scientific and Industrial Research Organisation",
    "Geoscience Australia",
    "Australian Institute of Marine Science",
    "IP Australia",
    "Australian Nuclear Science and Technology Organisation",
    "National Reconstruction Fund Corporation",

    # ── AGRICULTURE PORTFOLIO ───────────────────────────────────────────
    # MoG Jul 2022: DAWE split into DAFF + DCCEEW
    "Department of Agriculture, Fisheries and Forestry",       # from Jul 2022
    "Department of Agriculture, Water and the Environment",    # pre-Jul 2022
    "Australian Pesticides and Veterinary Medicines Authority",
    "Australian Fisheries Management Authority",
    "Murray-Darling Basin Authority",
    "Sydney Harbour Federation Trust",                         # heritage land management; under DAWE/DCCEEW

    # ── CLIMATE / ENVIRONMENT PORTFOLIO (from Jul 2022) ─────────────────
    "Department of Climate Change, Energy, the Environment and Water",
    "Bureau of Meteorology",
    "Clean Energy Regulator",
    "Great Barrier Reef Marine Park Authority",
    "Climate Change Authority",

    # ── INFRASTRUCTURE PORTFOLIO ────────────────────────────────────────
    # MoG Jul 2022: DITRDC → DITRDCA (Arts added)
    "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Department of Infrastructure, Transport, Regional Development and Communications",
    "Civil Aviation Safety Authority",
    "Airservices Australia",
    "Australian Maritime Safety Authority",
    "Australian Transport Safety Bureau",
    "Infrastructure Australia",
    "National Heavy Vehicle Regulator",
    "National Faster Rail Agency",
    "High Speed Rail Authority",
    "Infrastructure and Project Financing Agency",
    "National Museum of Australia",
    "Museum of Australian Democracy - Old Parliament House",
    "National Portrait Gallery of Australia",
    "National Film and Sound Archive of Australia",

    # ── EDUCATION PORTFOLIO ─────────────────────────────────────────────
    # MoG Jul 2022: DESE split into Dept Education + DEWR
    "Department of Education, Skills and Employment",          # pre-Jul 2022
    "Department of Education",                                 # from Jul 2022
    "Australian Research Council",
    "Tertiary Education Quality and Standards Agency",
    "Australian Skills Quality Authority",
    "Australian Children's Education and Care Quality Authority",
    "Australian Curriculum, Assessment and Reporting Authority",
    "National Gallery of Australia",
    "Screen Australia",

    # ── EMPLOYMENT AND WORKPLACE RELATIONS PORTFOLIO (from Jul 2022) ────
    "Department of Employment and Workplace Relations",
    "Comcare",
    "Fair Work Ombudsman",
    "Fair Work Commission",
    "Safe Work Australia",
    "Asbestos and Silica Safety and Eradication Agency",
    "Asbestos Safety and Eradication Agency",              # pre-rename alias; enables prefix-match on division strings

    # ── FOREIGN AFFAIRS AND TRADE PORTFOLIO ─────────────────────────────
    "Department of Foreign Affairs and Trade",
    "Australian Secret Intelligence Service",
    "Austrade",
    "Australian Centre for International Agricultural Research",

    # ── VETERANS' AFFAIRS PORTFOLIO ─────────────────────────────────────
    "Department of Veterans' Affairs",

    # ── SOCIAL SERVICES PORTFOLIO ───────────────────────────────────────
    "Department of Social Services",
    "Services Australia",
    "National Disability Insurance Agency",
    # Full name first, then the NDIS QSC short form used in the data
    "National Disability Insurance Scheme Quality and Safeguards Commission",
    "NDIS Quality and Safeguards Commission",
    "Australian Institute of Family Studies",

    # ── COMMUNICATIONS AND THE ARTS (pre-Jul 2022 portfolio) ────────────
    "Australian Communications and Media Authority",

    # ── PARLIAMENT AND RELATED ──────────────────────────────────────────
    "Department of Parliamentary Services",
    "Department of the House of Representatives",
    "Department of the Senate",
    "Parliamentary Budget Office",
    "Australian Strategic Policy Institute",

    # ── CROSS-PORTFOLIO / INDEPENDENT ───────────────────────────────────
    "National Recovery and Resilience Agency",     # COVID-era, under PM&C 2021-2022
    "National Drought and North Queensland Flood Response and Recovery Agency",  # 2019-2022, under PM&C
    "Australian Institute for Teaching and School Leadership",
    "Office of the Inspector-General of Taxation",
    "National Library of Australia",
    "National Capital Authority",
    "National Native Title Tribunal",
    "Australian National Maritime Museum",
    "Australian Charities and Not-for-profits Commission",
    "Australian Building and Construction Commission",
    "Australian Law Reform Commission",
    "National Offshore Petroleum Safety and Environmental Management Authority",
    "Australian Electoral Commission",
    "Export Finance Australia",
    "Indigenous Land and Sea Corporation",
    "Indigenous Business Australia",
    "Australian Sports Anti-Doping Authority",      # pre-Jul 2020; renamed to Sport Integrity Australia
    "Sport Integrity Australia",
    "North Queensland Water Infrastructure Authority",
    "Northern Territory Aboriginal Investment Corporation",
    "Domestic, Family and Sexual Violence Commission",
    "National Commission for Aboriginal and Torres Strait Islander Children and Young People",
    "Australian Financial Security Authority",   # listed again in AG's above; dedup at runtime
    "Asbestos and Silica Safety and Eradication Agency",  # listed again in Employment above
    "High Court of Australia",
    "National Transport Commission",
    "Office of the eSafety Commissioner",
    # 2024 variant: Sport added to portfolio/department name
    "Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts",
    # Australian Antarctic Division: operations division of DCCEEW/DAWE; appears as standalone in data
    # Maps to DCCEEW (post-2022) or DAWE (pre-2022) via MANUAL_OVERRIDES, not as own canonical
]

# Deduplicate while preserving order.
# set.add() returns None (falsy), so `c in _seen or _seen.add(c)` is True only
# when c was already in _seen — the not(...) then filters it out.
_seen = set()
CANONICAL_AGENCIES = [c for c in CANONICAL_AGENCIES
                      if not (c in _seen or _seen.add(c))]


# ============================================================
# SECTION 1b — CANONICAL FORWARD MAP
#
# Maps old or variant canonical names to their current successors.
# Applied inside match_canonical after _prefix_match returns a result,
# so raw strings that prefix-match an old name are automatically
# redirected to the current canonical without needing a manual override.
# ============================================================

CANONICAL_FORWARD_MAP = {
    # MoG Jul 2022: DISER → DISR
    "Department of Industry, Science, Energy and Resources":
        "Department of Industry, Science and Resources",
    # MoG Jul 2022: DITRDC → DITRDCA
    "Department of Infrastructure, Transport, Regional Development and Communications":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    # 2024 Sport variant → standard DITRDCA name
    "Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    # Long-form NDIS QSC → short canonical form used consistently in the data
    "National Disability Insurance Scheme Quality and Safeguards Commission":
        "NDIS Quality and Safeguards Commission",
    # Pre-rename alias → current canonical (renamed when silica-related diseases added to scope)
    "Asbestos Safety and Eradication Agency":
        "Asbestos and Silica Safety and Eradication Agency",
}


# ============================================================
# SECTION 2 — PORTFOLIO PREFIX STRIPPING
#
# When the parser prepended the portfolio name to the entity name,
# these prefixes are stripped before canonical matching.
# Ordered LONGEST-FIRST to avoid a short prefix stealing the match.
# ============================================================

PORTFOLIO_PREFIXES_ORDERED = [
    "Services Australia (part of the Social Services Portfolio)",
    "Veterans' Affairs (part of the Defence Portfolio)",
    "Agriculture, Water and the Environment",
    "Climate Change, Energy, the Environment and Water",
    "Industry, Science, Energy and Resources",
    "Industry, Science and Resources",
    "Education, Skills and Employment",
    "Employment and Workplace Relations",
    "Foreign Affairs and Trade",
    "Communications and the Arts",
    "Prime Minister and Cabinet",
    "Attorney-General's",
    "Social Services",
    "Home Affairs",
    "Environment and Energy",
    # Full Infrastructure portfolio names BEFORE short "Infrastructure" prefix
    "Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Infrastructure, Transport, Regional Development and Communications",
    "Infrastructure",
    "Treasury",
    "Finance",
    "Defence",
    "Health",
    "Education",
    "Sport",
    # Entity-as-prefix patterns (entity appears as prefix for sub-body)
    "Parliamentary Department",         # strips to "Department of Parliamentary Services..."
    "Australian Competition and Consumer Commission",  # strips to "Australian Energy Regulator..."
]

# Portfolio-name-only strings: when the raw string IS just the portfolio name
# (nothing after), these map to the portfolio department itself.
# None = date-dependent or multi-entity (requires division field — flagged separately).
PORTFOLIO_ONLY_MAP = {
    "Services Australia (part of the Social Services Portfolio)": "Services Australia",
    "Veterans' Affairs (part of the Defence Portfolio)":          "Department of Veterans' Affairs",
    "Agriculture, Water and the Environment":                      None,   # DAWE pre-2022 OR portal-prefix post-2022
    "Climate Change, Energy, the Environment and Water":           "Department of Climate Change, Energy, the Environment and Water",
    "Industry, Science, Energy and Resources":                     "Department of Industry, Science and Resources",
    "Industry, Science and Resources":                             "Department of Industry, Science and Resources",
    "Education, Skills and Employment":                            None,   # multi-entity
    "Employment and Workplace Relations":                          "Department of Employment and Workplace Relations",
    "Foreign Affairs and Trade":                                   "Department of Foreign Affairs and Trade",
    "Communications and the Arts":                                 None,   # multi-entity (ACMA, NMA, ...)
    "Prime Minister and Cabinet":                                  "Department of the Prime Minister and Cabinet",
    "Attorney-General's":                                          "Attorney-General's Department",
    "Social Services":                                             "Department of Social Services",
    "Home Affairs":                                                "Department of Home Affairs",
    "Environment and Energy":                                      "Clean Energy Regulator",
    "Infrastructure, Transport, Regional Development, Communications and the Arts":
                                                                   "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Infrastructure, Transport, Regional Development and Communications":
                                                                   "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Infrastructure":                                              "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Treasury":                                                    "Department of the Treasury",
    "Finance":                                                     "Department of Finance",
    "Defence":                                                     "Department of Defence",
    "Health":                                                      None,   # pre-2022 = Dept of Health; post-2022 = DHAC
    "Education":                                                   None,   # multi-entity (Dept Ed, NGA, ARC, ...)
    "Sport":                                                       "Sport Integrity Australia",
    "Parliamentary Department":                                    "Department of Parliamentary Services",
    "Australian Competition and Consumer Commission":              "Australian Competition and Consumer Commission",
}

# Multi-entity portfolio-only strings that always need division-field lookup.
# These map to canonical=None in the crosswalk; resolved in application step.
DIVISION_REQUIRED = {
    "Agriculture, Water and the Environment",  # DAWE (pre-2022) OR BoM/DCCEEW/APVMA/... (post-2022)
    "Education, Skills and Employment",        # DESE / DEWR / Dept Ed / NGA / SWA / ...
    "Communications and the Arts",             # ACMA / NMA / ...
    "Education",                               # Dept Ed / NGA / ARC / TEQSA / ...
    "Industry, Science, Energy and",           # truncated portfolio name — wraps at PDF column boundary
    "Industry, Innovation and Science",        # pre-2020 portfolio; division may identify sub-entity
    "National Commission for Aboriginal and Torres Strait",   # div = "Islander Children and Young People"
    # 'Health' is date-dependent but maps to a single entity per date — handled separately
}


# ============================================================
# SECTION 3 — MANUAL OVERRIDES
#
# Raw strings that need special handling beyond the algorithm.
# Covers: garbled names, tautological repeats, COVID-era entities,
# known data quirks.
# ============================================================

MANUAL_OVERRIDES = {
    # ── Pre-2020 predecessor names (before Feb 2020 MoG) ────────────────────

    # Industry, Innovation and Science (DIIS) — renamed to DISER Feb 2020 → DISR Jul 2022
    "Industry, Innovation and Science Department of Industry, Innovation and Science":
        "Department of Industry, Science and Resources",
    "Department of Industry, Innovation and Science":
        "Department of Industry, Science and Resources",

    # Agriculture 2020: pre-Feb 2020 name before DAWE rename
    "Department of Agriculture":
        "Department of Agriculture, Water and the Environment",

    # Environment and Energy 2020: pre-DAWE/DCCEEW predecessor
    "Department of the Environment and Energy":
        "Department of Climate Change, Energy, the Environment and Water",

    # Infrastructure portfolio 2020: "Cities" variant before Feb 2020 rename to DITRDC
    "Department of Infrastructure, Transport, Cities and Regional Development":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Department of Communications and the Arts":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Communications Infrastructure":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Infrastructure, Transport, Cities and":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Infrastructure, Transport, Cities and Regional Development Department of Infrastructure, Transport, Regional Development and Communications":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",
    "Infrastructure, Transport, Cities and Regional Development Department of Infrastructure, Transport, Cities and Regional Development":
        "Department of Infrastructure, Transport, Regional Development, Communications and the Arts",

    # ── Truncated strings that won't prefix-match ────────────────────────────
    "National Offshore Petroleum Safety and":
        "National Offshore Petroleum Safety and Environmental Management Authority",
    "Australian Institute of Aboriginal and Torres Strait":
        "Australian Institute of Aboriginal and Torres Strait Islander Studies",
    "Australian Institute for Teaching and School":
        "Australian Institute for Teaching and School Leadership",
    "Commonwealth Scientific and Industrial Research":
        "Commonwealth Scientific and Industrial Research Organisation",
    "Australian Children's Education and Care Quality":
        "Australian Children's Education and Care Quality Authority",
    "Australian Curriculum, Assessment and Reporting":
        "Australian Curriculum, Assessment and Reporting Authority",
    "Office of the Inspector-General of Intelligence and":
        "Office of the Inspector-General of Intelligence and Security",
    "Australian Pesticides and Veterinary Medicines":
        "Australian Pesticides and Veterinary Medicines Authority",
    "Museum of Australian Democracy - Old Parliament":
        "Museum of Australian Democracy - Old Parliament House",
    "Australian Radiation Protection and Nuclear Safety":
        "Australian Radiation Protection and Nuclear Safety Agency",
    "Australian Nuclear Science and Technology":
        "Australian Nuclear Science and Technology Organisation",

    # PDF line-wrap truncations — agency name cuts at column boundary
    "Australian Centre for International Agricultural":
        "Australian Centre for International Agricultural Research",
    "Australian Commission on Safety and Quality in":
        "Australian Commission on Safety and Quality in Health Care",
    "National Disability Insurance Scheme (NDIS) Quality":
        "NDIS Quality and Safeguards Commission",
    "Department of Climate Change, Energy, the":
        "Department of Climate Change, Energy, the Environment and Water",

    # Word-order misspelling (two 2022-07-05 rows)
    "Department of Agriculture, Forestry and Fisheries":
        "Department of Agriculture, Fisheries and Forestry",

    # PWSS name variants used in branch fields: "The..." prefix and plural form
    # both fail prefix-match against the canonical; manual overrides required.
    "The Parliamentary Workplace Support Service":
        "Parliamentary Workplace Support Service",
    "Parliamentary Workplace Support Services":
        "Parliamentary Workplace Support Service",
    # Shortened form of eSafety Commissioner used in branch fields
    "eSafety Commissioner":
        "Office of the eSafety Commissioner",

    # ── Garbled / misspelled names ───────────────────────────────────────────
    "Australian Prudential Regulatory Authority":
        "Australian Prudential Regulation Authority",
    "Asbestos Safety and Eradication Agency":
        "Asbestos and Silica Safety and Eradication Agency",

    # ── Miscellaneous genuine overrides ─────────────────────────────────────
    "Department of Surge Testing":
        "Department of Health and Aged Care",   # notional mapping — Surge Testing was DH-led
    "Taxation Ombudsman":
        "Office of the Inspector-General of Taxation",
    "Taxation Ombudsman Deputy Tax Ombudsman - Reviews Reviews":
        "Office of the Inspector-General of Taxation",
    "ICT Services":
        "Department of Defence",   # data note: Defence sub-unit used as agency
    "Professional Services Review Case Management Unit Case Management Unit":
        "Health Professional Services Review",

    # ── ACCC/AER — ambiguous: ACCC is both portfolio prefix and canonical ────
    # Prefix stripping would resolve ACCC → ACCC (the canonical), not AER.
    # These need explicit overrides.
    "Australian Competition and Consumer Commission AER - Australian Energy Regulator":
        "Australian Energy Regulator",
    "Australian Competition and Consumer Commission Australian Energy Regulator":
        "Australian Energy Regulator",

    # ── Australian Antarctic Division ────────────────────────────────────────
    # Division of DCCEEW (post-2022) / DAWE (pre-2022); application step may refine by date
    "Australian Antarctic":
        "Department of Climate Change, Energy, the Environment and Water",
    "Australian Antarctic Division":
        "Department of Climate Change, Energy, the Environment and Water",
    "Australian Antarctic Division Australian Antarctic Division Various Various":
        "Department of Climate Change, Energy, the Environment and Water",

    # ── Parsing artifacts — description/boilerplate text bled into agency ────

    # PII-containing strings also handled by MANUAL_PREFIX_OVERRIDES for prefix matching
    "Social Services Worker Screening check":
        "National Disability Insurance Agency",
    "If you have any questions, please contact":
        "National Disability Insurance Agency",

    # DVA: "Incapacity Payments team" + "veterans and their families" identifies DVA
    "The Team Leader role is essential in the daily running and resource management of the Incapacity Payments team. This role manages the workload, performance, accuracy, adherence to behaviours and productive relationship management across a large team. Ensuring a safe, organised and harmonious workplace with back-up strategies to cope with unexpected disruption to the Team's operations is the primary task of the Team Leader. This role requires a person with strong leadership skills who actively supervises the operations of a team. They focus on delivering a high quality service to veterans and their families by promoting the use of best practice techniques, processes and technology applications and supervising service delivery behaviour and adherence to attendance plans.":
        "Department of Veterans' Affairs",

    # DVA: RecruitAbility + "DVA" + "veteran community" throughout body text
    "Senior Client Contact Officers work as part of a team contributing to integrated service delivery in a Client Service delivery environment. Senior Client Contact Officers perform moderately complex customer service and administrative work under limited direction in accordance with policy, process and legislative frameworks. This includes telephony support (taking calls) and face to face services as directed. Senior Client Contact Officers require an ability to understand and follow service delivery processes and competently use associated systems.":
        "Department of Veterans' Affairs",

    # Bureau of Meteorology: agency name explicit in body text; confirmed by neighbouring
    # row for same VN (agency = "Agriculture, Water and the Environment", div = "Bureau of Meteorology...")
    "The Bureau of Meteorology is undergoing a significant transformation to deliver a more customer centric, unified and resilient national operation. This is an exciting strategic direction for the Bureau which will transform the way we deliver services to Australian communities. The future operating model for the Bureau will open new career pathways and enhance our culture to empower our people to learn and grow. As part of this transformation, we are introducing new roles to assist the Bureau in delivering world class weather products and services within the newly formed Community Services Group. This group is comprised of Decision Support Services (DSS), Environmental Prediction Services (EPS), and National Production Services (NPS) which have been configured to enable scalable, national and resilient services. The DSS program will lead customer engagement within the Community Services Group with an overarching mission to deliver tailored, relevant and timely information to enable better decision making. The DSS program is characterised by its shared understanding of the impact that weather, water, climate and oceans have on the decisions that Bureau customers make every day. Operating with a national capability, the program will be accountable for leading engagement with the Australian community and the emergency management sector. This will be delivered by two dedicated teams: Hazard Preparedness and Response (HPR) and Community Engagement (CE). The state/territory Senior Hydrologist HPR is located in each capital city across Australia and will report into the State HPR Manager. The role will work alongside other state and territory based HPR and CE staff. The successful candidate will have a unique opportunity to help grow the Bureau's HPR capabilities in support of our emergency management partners. During extreme weather events, this team is expected to undertake extended working hours as directed.":
        "Bureau of Meteorology",

    # Bureau of Meteorology: "Job Description" bled into agency; neighbouring row for same
    # VN-0741610 (PS19 2024-05-09) has agency = "Agriculture, Water and the Environment",
    # div = "Bureau of Meteorology Data and Digital Application Services"
    "Job Description":
        "Bureau of Meteorology",

    # Dept of Education: "International Division" + international education policy context;
    # gazette dates Jan–Mar 2023 (after DESE → Education rename Jul 2022)
    "In the International Division, we operate as policy advisors, representatives, analysts, administrators, and diplomats. We work across the Australian Government, state and territory governments, and education providers to protect and support the sustainable growth of our international education sector and ensure Australia is recognised as a world leader in education and training. With staff based in 10 countries, we also work closely with foreign partner governments to advance Australia's economic growth, public diplomacy, and national security. We engage closely with priority countries in Latin America, North Asia, Southeast Asia and South":
        "Department of Education",

    "Across the International Division, we work on international education policy challenges and high profile initiatives that support the sustainable development and growth of our international education sector. The Division has multiple current and expected vacancies for APS 4 Policy Officers. These positions are responsible for contributing to the development and implementation of innovative and high quality strategic policy, undertaking research using a range of sources, and analysing and presenting findings, including through the use of data and evidence. Positions will also be responsible for supporting bilateral and multilateral engagement, liaising with international and domestic stakeholders, and managing discrete projects.":
        "Department of Education",
}


# ============================================================
# SECTION 4 — MACHINERY OF GOVERNMENT CHANGE TABLE
#
# Key splits/renames 2021-2026. Used to:
#  1. Validate that pre/post-split names don't bleed across the boundary
#  2. Determine agency_group for renamed entities
# ============================================================

# Format: {raw_name_or_signal: (effective_date, successor_or_note)}
MOG_CHANGES = {
    # Jul 2022 election transition (official date: 1 Jul 2022 for most)
    "DAWE_split": {
        "effective": "2022-07-01",
        "predecessors": ["Department of Agriculture, Water and the Environment"],
        "successors":   ["Department of Agriculture, Fisheries and Forestry",
                         "Department of Climate Change, Energy, the Environment and Water"],
        "note": "DAWE split: agriculture functions → DAFF; environment/climate/water → DCCEEW",
    },
    "DISER_rename": {
        "effective": "2022-07-01",
        "predecessors": ["Department of Industry, Science, Energy and Resources"],
        "successors":   ["Department of Industry, Science and Resources"],
        "note": "Energy and resources functions moved to DCCEEW; DISER became DISR",
    },
    "DESE_split": {
        "effective": "2022-07-01",
        "predecessors": ["Department of Education, Skills and Employment"],
        "successors":   ["Department of Education",
                         "Department of Employment and Workplace Relations"],
        "note": "DESE split: education functions → Dept Education; employment → DEWR",
    },
    "DITRDC_rename": {
        "effective": "2022-07-01",
        "predecessors": ["Department of Infrastructure, Transport, Regional Development and Communications"],
        "successors":   ["Department of Infrastructure, Transport, Regional Development, Communications and the Arts"],
        "note": "Arts added to Infrastructure portfolio following communications/arts merger",
    },
    "Health_rename": {
        "effective": "2022-07-01",
        "predecessors": ["Department of Health"],
        "successors":   ["Department of Health and Aged Care"],
        "note": "Aged care functions formally incorporated into Health portfolio",
    },
    # 2020 sports integrity reform
    "ASADA_to_SIA": {
        "effective": "2020-07-01",
        "predecessors": ["Australian Sports Anti-Doping Authority"],
        "successors":   ["Sport Integrity Australia"],
        "note": "ASADA renamed and expanded to Sport Integrity Australia",
    },
    # 2023 integrity reforms
    "ACLEI_to_NACC": {
        "effective": "2023-07-01",
        "predecessors": ["Australian Commission for Law Enforcement Integrity"],
        "successors":   ["National Anti-Corruption Commission"],
        "note": "ACLEI merged into new NACC (broader jurisdiction)",
    },
    # 2024 tribunal reform
    "AAT_to_ART": {
        "effective": "2024-10-14",    # Administrative Review Tribunal Act 2024 commencement
        "predecessors": ["Administrative Appeals Tribunal"],
        "successors":   ["Administrative Review Tribunal"],
        "note": "AAT abolished and replaced by Administrative Review Tribunal",
    },
    # Health late-2024 disability rename (tentative; confirm with gazette dates)
    "Health_disability_rename": {
        "effective": "2024-07-01",
        "predecessors": ["Department of Health and Aged Care"],
        "successors":   ["Department of Health, Disability and Ageing"],
        "note": "Disability functions re-integrated following NDIS review; rename tentative",
    },
}


# ============================================================
# SECTION 5 — AGENCY GROUP MAP
#
# Maps each canonical agency name to a stable label for time-series.
# Groups predecessor/successor entities together so trend lines don't
# break at MoG change dates.
#
# Rule: if an entity has no MoG predecessor/successor, agency_group = agency_canonical.
# ============================================================

AGENCY_GROUP = {
    # Agriculture MoG split — both DAWE (pre) and DAFF (post) map to "Agriculture department"
    "Department of Agriculture, Water and the Environment":     "Agriculture department",
    "Department of Agriculture, Fisheries and Forestry":        "Agriculture department",

    # Climate/environment — DCCEEW (post-split) and DAWE-in-environment-role both map here
    # NOTE: DAWE rows resolved via division field; DAWE itself maps to Agriculture department above.
    "Department of Climate Change, Energy, the Environment and Water": "Climate and environment department",

    # Industry MoG rename
    "Department of Industry, Science, Energy and Resources":    "Industry department",
    "Department of Industry, Science and Resources":            "Industry department",
    "Department of Industry Science and Resources":             "Industry department",

    # Education MoG split
    "Department of Education, Skills and Employment":           "Education department",
    "Department of Education":                                  "Education department",

    # Employment MoG split
    "Department of Employment and Workplace Relations":         "Employment department",

    # Infrastructure MoG rename
    "Department of Infrastructure, Transport, Regional Development and Communications":  "Infrastructure department",
    "Department of Infrastructure, Transport, Regional Development, Communications and the Arts": "Infrastructure department",

    # Health MoG rename
    "Department of Health":                                     "Health department",
    "Department of Health and Aged Care":                       "Health department",
    "Department of Health, Disability and Ageing":              "Health department",

    # Sports integrity MoG
    "Australian Sports Anti-Doping Authority":                  "Sport Integrity Australia",

    # Integrity bodies MoG
    "Australian Commission for Law Enforcement Integrity":      "NACC",
    "National Anti-Corruption Commission":                      "NACC",

    # Tribunal MoG
    "Administrative Appeals Tribunal":                         "Administrative review body",
    "Administrative Review Tribunal":                          "Administrative review body",
}


# ============================================================
# SECTION 5b — DIVISION-MISMATCH ALLOWLIST
#
# Used by pipeline/validation.py check 2 (the AIFS-class regression guard).
# For each release row whose `division` string starts with a canonical agency
# name (longest-prefix match against CANONICAL_AGENCIES, minus the two stale
# pre-MoG names collapsed by CANONICAL_REMAP in 05_apply_crosswalk.py) that
# DIFFERS from the row's `agency_canonical`, the ordered pair
# (agency_canonical, division_canonical) must appear here. Any unlisted pair
# FAILs the build — this is what would have caught the AIFS misattribution.
#
# Seeded with exactly the 27 pairs (583 rows) observed in the 2026-07-06
# release under the module prefix pool. Each entry is a deliberate
# employing-entity/sub-entity convention, a prefix-matcher false positive, or a
# TEMPORARY single-row misattribution flagged for spec 07. Do NOT add entries
# without row-level evidence.
# ============================================================

ALLOWED_DIVISION_MISMATCH = {
    # Employing-entity convention: Federal Court is the employing entity for
    # FCFCoA and NNTT staff; division names the court/tribunal (376 rows:
    # 322 + 52 + 1 + 1 across the four pairs).
    ("Federal Court of Australia", "Federal Circuit and Family Court of Australia"),
    ("Federal Court of Australia", "National Native Title Tribunal"),
    ("Federal Circuit and Family Court of Australia", "National Native Title Tribunal"),
    ("National Native Title Tribunal", "Federal Court of Australia"),
    # Deliberate sub-entity exceptions: division names the *hosting* body,
    # canonical is the analytically meaningful sub-entity (BRANCH_OVERRIDE_PAIRS).
    ("Parliamentary Workplace Support Service", "Australian Public Service Commission"),
    ("Domestic, Family and Sexual Violence Commission", "Department of Social Services"),
    ("Independent Health and Aged Care Pricing Authority", "Department of Health, Disability and Ageing"),
    ("Independent Health and Aged Care Pricing Authority", "Department of Health and Aged Care"),
    ("Independent Hospital Pricing Authority", "Department of Health and Aged Care"),
    ("National Mental Health Commission", "Department of Health, Disability and Ageing"),
    ("Office of the eSafety Commissioner", "Australian Communications and Media Authority"),
    ("Australian Centre for Disease Control", "Department of Health and Aged Care"),
    ("Australian Submarine Agency", "Department of Defence"),
    ("Australian Naval Nuclear Power Safety Regulator", "Department of Defence"),
    # Prefix-match false positive of THIS CHECK, not an attribution issue:
    # 2 IHACPA rows (VN-0735741, VN-0735755, gazette 2024-02-08) whose *internal*
    # division is literally named "Independent Hospital Pricing Authority Division"
    # (a unit of IHACPA named after its predecessor). Attribution is correct;
    # the longest-prefix matcher fires on the predecessor's canonical name.
    ("Independent Health and Aged Care Pricing Authority", "Independent Hospital Pricing Authority"),
    # Portfolio-department hosts: department is the employing entity, division
    # names a portfolio body it advertised on behalf of (single-digit rows each;
    # underlying rows verified 2026-07-06, see per-pair notes).
    ("Department of the Treasury", "Royal Australian Mint"),
    ("Department of the Treasury", "Australian Charities and Not-for-profits Commission"),
    # 1 row: VN-0687359, 2021-03-18, "Second Commissioner of Taxation",
    # classification_code = "Statutory Appointment". A statutory-office
    # appointment run by the portfolio department, not an ATO staff vacancy —
    # employing-entity attribution to Treasury is deliberate. NOT the AIFS
    # shape (that was bulk staff-vacancy misattribution).
    ("Department of the Treasury", "Australian Taxation Office"),
    ("Department of the Treasury", "Infrastructure and Project Financing Agency"),
    ("Department of the Treasury", "Productivity Commission"),
    ("Department of Employment and Workplace Relations", "Comcare"),
    ("Department of Agriculture, Water and the Environment", "Sydney Harbour Federation Trust"),
    ("Department of Health", "Therapeutic Goods Administration"),
    # 143 rows (2020→present): AER is a constituent part of the ACCC; AER staff
    # are ACCC employees under the PS Act. Employing-entity convention, same as
    # Federal Court/NNTT. (AER currently has 0 rows as its own canonical; the
    # MANUAL_OVERRIDES that would map combined "ACCC ... AER" agency strings to
    # AER match no current raw strings.)
    ("Australian Competition and Consumer Commission", "Australian Energy Regulator"),
    # 7 rows: division carries the agency's pre-rename name (Asbestos Safety and
    # Eradication Agency → Asbestos and Silica Safety and Eradication Agency);
    # attribution is correct, the prefix matcher fires on the old name.
    ("Asbestos and Silica Safety and Eradication Agency", "Asbestos Safety and Eradication Agency"),
    # ── TEMPORARY entries: known single-row misattributions, allowlisted only
    # so this check can land green before the fix. Spec 07 item 7 reattributes
    # both rows and DELETES these two pairs. Do not add new entries here
    # without row-level evidence.
    #
    # 1 row: VN-0704814, 2022-05-05, "Executive Assistant - APS 6", raw agency
    # = APSC, division = PWSS, branch null. Contemporaneous PWSS stand-up ads
    # (Feb–Dec 2022, raw agency "Prime Minister and Cabinet", PWSS in branch)
    # resolve to PWSS via BRANCH_OVERRIDE_PAIRS; this one names PWSS in
    # *division*, which no pass inspects for a non-portfolio raw agency, so it
    # stays on the host. Under the documented PWSS sub-entity exception it
    # should be PWSS.
    ("Australian Public Service Commission", "Parliamentary Workplace Support Service"),
    # 1 row: VN-0700097, 2022-02-03, "Finance Officer", branch = "Office of
    # the CEO", division = IHPA, raw agency = TGA (printed that way in the
    # source PDF — verified against the raw parquet; fields parsed cleanly,
    # so this is a gazette-source error, not a parse artefact). The role is
    # plainly IHPA's; TGA and IHPA are unrelated bodies.
    ("Therapeutic Goods Administration", "Independent Hospital Pricing Authority"),
}


# ============================================================
# SECTION 6 — NORMALISATION AND MATCHING
# ============================================================

def _norm(s: str) -> str:
    """Lowercase, strip parenthetical acronyms, remove punctuation, collapse whitespace.

    Order matters:
      1. Normalise Unicode curly quotes → ASCII apostrophe (dataset uses U+2019)
      2. Strip parenthetical acronyms BEFORE lowercasing (regex requires uppercase)
      3. Lowercase
      4. Remove remaining ASCII punctuation
    """
    # Normalise Unicode quotes: ' (U+2019) → '  and  ' (U+2018) → '
    s = s.replace('’', "'").replace('‘', "'")
    # Remove parenthetical acronyms like "(CSIRO)", "(ACIC)", "(NDIS)", "(NOPSEMA)"
    s = re.sub(r'\s*\([A-Z][A-Z0-9\-]+\)\s*', ' ', s)
    s = s.lower()
    s = re.sub(r"[',.()\-/&]", ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# Build sorted (longest-first) list of (normalized, canonical) tuples
_CANONICAL_NORM = sorted(
    [(_norm(c), c) for c in CANONICAL_AGENCIES],
    key=lambda x: -len(x[0])
)

def _prefix_match(s_norm: str) -> str | None:
    """Return the canonical agency whose normalized name is the longest prefix of s_norm."""
    for nc, canon in _CANONICAL_NORM:
        if s_norm == nc:
            return canon
        if s_norm.startswith(nc) and (len(s_norm) == len(nc) or s_norm[len(nc)] == ' '):
            return canon
    return None


# Build the reverse lookup for manual overrides using normalised keys
MANUAL_NORM_LOOKUP = {_norm(k): v for k, v in MANUAL_OVERRIDES.items()}

# Prefix-matching overrides for bleed artifacts where the raw agency field contains
# description/contact text. Keys are PII-free leading substrings; the actual raw
# strings are longer and contain personal names/emails from the source gazette.
# Sorted longest-first so more-specific entries win.
MANUAL_PREFIX_OVERRIDES = [
    (_norm("Social Services Worker Screening check"), "National Disability Insurance Agency"),
    (_norm("If you have any questions, please contact"),  "National Disability Insurance Agency"),
]


def match_canonical(raw: str) -> tuple[str | None, str]:
    """
    Returns (canonical_agency_name_or_None, match_method).

    match_method values:
      exact            — exact normalized match
      prefix           — entity name is a prefix of the raw string (entity + group/branch noise)
      portfolio_strip  — portfolio prefix stripped, then prefix match
      portfolio_only   — raw == portfolio name, mapped via PORTFOLIO_ONLY_MAP
      manual           — entry in MANUAL_OVERRIDES or MANUAL_PREFIX_OVERRIDES
      division_required — multi-entity portfolio-name; real entity in division field
      uncertain        — no confident match found
    """
    raw = raw.strip()
    if not raw:
        return (None, 'empty')

    nr = _norm(raw)

    # 1. Manual overrides (exact)
    if nr in MANUAL_NORM_LOOKUP:
        return (MANUAL_NORM_LOOKUP[nr], 'manual')

    # 1b. Manual prefix overrides (bleed artifacts with PII-free truncated keys)
    for nk, val in MANUAL_PREFIX_OVERRIDES:
        if nr.startswith(nk) and (len(nr) == len(nk) or nr[len(nk)] == ' '):
            return (val, 'manual')

    # 2. Exact / prefix match
    m = _prefix_match(nr)
    if m:
        method = 'exact' if _norm(m) == nr else 'prefix'
        m = CANONICAL_FORWARD_MAP.get(m, m)
        return (m, method)

    # 3. Division required?
    if raw.strip() in DIVISION_REQUIRED:
        return (None, 'division_required')

    # 4. Portfolio prefix strip
    for portfolio in PORTFOLIO_PREFIXES_ORDERED:
        np = _norm(portfolio)
        if nr == np:
            if portfolio in DIVISION_REQUIRED:
                return (None, 'division_required')
            mapped = PORTFOLIO_ONLY_MAP.get(portfolio)
            if mapped is None:
                return (None, 'uncertain')
            return (mapped, 'portfolio_only')
        if nr.startswith(np + ' '):
            remainder = nr[len(np):].strip()
            if not remainder:
                if portfolio in DIVISION_REQUIRED:
                    return (None, 'division_required')
                mapped = PORTFOLIO_ONLY_MAP.get(portfolio)
                if mapped is None:
                    return (None, 'uncertain')
                return (mapped, 'portfolio_only')
            m2 = _prefix_match(remainder)
            if m2:
                m2 = CANONICAL_FORWARD_MAP.get(m2, m2)
                return (m2, 'portfolio_strip')
            return (None, f'uncertain_after_strip:{portfolio}')

    return (None, 'uncertain')


def get_agency_group(canonical: str | None) -> str | None:
    if canonical is None:
        return None
    return AGENCY_GROUP.get(canonical, canonical)   # default: group == canonical


# ============================================================
# SECTION 7 — MAIN: BUILD CROSSWALK FROM DATA
# ============================================================

def build_crosswalk(data_path: str = "data/gazette_vacancies_normalised.parquet",
                    out_path:  str = "data/agency_crosswalk.csv",
                    dry_run:   bool = False) -> pd.DataFrame:

    print(f"Loading {data_path} …")
    df = pd.read_parquet(data_path)
    df['gazette_date'] = pd.to_datetime(df['gazette_date'])
    print(f"  {len(df):,} rows, {df['agency'].nunique():,} distinct agency values\n")

    # ── Collect all distinct raw strings to map ────────────────────────────
    # 1. Non-.pdf agency values
    non_pdf = df[~df['agency'].str.contains(r'\.pdf', na=False, regex=True)]
    agency_strings = set(non_pdf['agency'].dropna().unique())

    # 2. .pdf rows: reconstruct from division + branch
    pdf_rows = df[df['agency'].str.contains(r'\.pdf', na=False, regex=True)].copy()
    pdf_rows['_recon'] = (
        pdf_rows['division'].fillna('') +
        pdf_rows['branch'].apply(lambda x: (' ' + x) if pd.notna(x) and str(x).strip() else '')
    ).str.strip()
    recon_strings = set(pdf_rows['_recon'].unique())

    # strings from both pools that need mapping
    all_raw = agency_strings | recon_strings
    print(f"Distinct strings to map: {len(all_raw):,}  "
          f"({len(agency_strings):,} agency field + {len(recon_strings):,} .pdf reconstructed)\n")

    # ── Apply matching ─────────────────────────────────────────────────────
    rows = []
    uncertain = []
    division_required_list = []

    for raw in sorted(all_raw):
        canonical, method = match_canonical(raw)
        group = get_agency_group(canonical)
        rows.append({
            'agency_raw':       raw,
            'agency_canonical': canonical,
            'agency_group':     group,
            'match_method':     method,
        })
        if method.startswith('uncertain'):
            uncertain.append((raw, method))
        elif method == 'division_required':
            division_required_list.append(raw)

    crosswalk_df = pd.DataFrame(rows)

    # ── Summary ────────────────────────────────────────────────────────────
    method_counts = crosswalk_df['match_method'].value_counts()
    print("=== MATCH METHOD SUMMARY ===")
    for method, cnt in method_counts.items():
        print(f"  {cnt:5d}  {method}")

    mapped_rows = crosswalk_df['agency_canonical'].notna().sum()
    total = len(crosswalk_df)
    print(f"\nMapped:           {mapped_rows:,} / {total:,} strings ({100*mapped_rows/total:.1f}%)")
    print(f"Division required:{len(division_required_list):,} strings (need division-field lookup)")
    print(f"Uncertain:        {len(uncertain):,} strings (need manual review)\n")

    # ── Canonical coverage ─────────────────────────────────────────────────
    canon_counts = (crosswalk_df[crosswalk_df['agency_canonical'].notna()]
                    .groupby('agency_canonical')
                    .size()
                    .sort_values(ascending=False))
    print(f"Distinct canonical agencies: {len(canon_counts)}")

    # ── Report: division_required strings ─────────────────────────────────
    if division_required_list:
        print("\n=== DIVISION_REQUIRED STRINGS (crosswalk can't resolve — need division lookup) ===")
        for s in division_required_list:
            rows_for_s = len(df[df['agency'] == s])
            print(f"  {rows_for_s:5d} rows  {repr(s)}")

    # ── Report: uncertain strings ──────────────────────────────────────────
    if uncertain:
        print(f"\n=== UNCERTAIN STRINGS ({len(uncertain)}) — NEED MANUAL REVIEW ===")
        for raw, method in sorted(uncertain, key=lambda x: x[0]):
            rows_for_s = len(df[df['agency'] == raw])
            print(f"  [{rows_for_s:4d} rows] [{method}]  {repr(raw[:110])}")

    # ── Redact PII from agency_raw before writing ─────────────────────────
    # Some raw agency strings are description/contact text that bled from the notice
    # body. These may contain personal names or email addresses. Replace any agency_raw
    # value whose normalised form starts with a MANUAL_PREFIX_OVERRIDES key with a
    # placeholder, so the committed CSV does not contain PII.
    def _redact_agency_raw(raw: str) -> str:
        nr = _norm(raw)
        for nk, _ in MANUAL_PREFIX_OVERRIDES:
            if nr.startswith(nk) and (len(nr) == len(nk) or nr[len(nk)] == ' '):
                return '[contact text redacted]'
        return raw

    n_redacted = crosswalk_df['agency_raw'].apply(lambda r: _norm(r).startswith(
        tuple(nk for nk, _ in MANUAL_PREFIX_OVERRIDES)
    )).sum()
    if n_redacted:
        crosswalk_df['agency_raw'] = crosswalk_df['agency_raw'].apply(_redact_agency_raw)
        print(f"Redacted PII from {n_redacted} agency_raw value(s) before writing CSV")

    # ── Save ───────────────────────────────────────────────────────────────
    if not dry_run:
        crosswalk_df.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"\nSaved → {out_path}  ({len(crosswalk_df):,} rows)")
    else:
        print("\n[dry-run] not saving")

    return crosswalk_df


if __name__ == '__main__':
    dry = '--dry-run' in sys.argv
    build_crosswalk(dry_run=dry)
