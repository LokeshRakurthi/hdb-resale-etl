"""
config.py
---------
Central configuration for the HDB Resale Flat Prices ETL pipeline.

Deliberately kept minimal: nothing here should be a stand-in for something
the pipeline could instead discover or derive from the data/API itself.
Where earlier versions of this file hardcoded values (a fixed list of
dataset ids, a fixed list of valid towns/flat types), those have been
removed in favour of runtime discovery (src/extract.py) or statistical
derivation from the master dataset (src/validate.py) — see
docs/ASSUMPTIONS.md for the full rationale on each of these.
"""
from pathlib import Path

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"
FAILED_DIR = DATA_DIR / "failed"
TRANSFORMED_DIR = DATA_DIR / "transformed"
HASHED_DIR = DATA_DIR / "hashed"

for _d in (RAW_DIR, CLEANED_DIR, FAILED_DIR, TRANSFORMED_DIR, HASHED_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Source data (data.gov.sg)
# ---------------------------------------------------------------------------
# The HDB Resale Flat Prices data is published on data.gov.sg as a
# *collection* (https://data.gov.sg/collections/189/view) containing several
# constituent dataset files (currently five: 1990-1999, 2000-Feb2012,
# Mar2012-Dec2014, Jan2015-Dec2016, 2017-onwards), split by the period in
# which the transaction was registered/approved.
#
# We do NOT hardcode which file ids belong to this collection, or how many
# there are — that would be exactly the kind of brittle assumption the
# assignment tells us to avoid, and it goes stale the moment data.gov.sg
# adds, removes, or restructures a file. Instead, src/extract.py discovers
# the full, current list of constituent dataset ids at runtime by querying
# the collection's own metadata endpoint, and downloads ALL of them. The
# Jan-2012-to-Dec-2016 scoping the assignment asks for is then applied
# downstream, on the data itself (the `month` validation rule in
# src/validate.py), not by pre-selecting files — so it's auditable (every
# excluded row lands in the failed ledger with a reason) rather than silently
# assumed at the extraction stage.
DATA_GOV_SG_COLLECTION_ID = "189"  # https://data.gov.sg/collections/189/view

DATA_GOV_SG_METADATA_API_BASE = "https://api-production.data.gov.sg/v2/public/api"
DATA_GOV_SG_DOWNLOAD_API_BASE = "https://api-open.data.gov.sg/v1/public/api"
# GET  {API_BASE}/collections/{collection_id}/metadata  -> list of child dataset ids
# GET  {API_BASE}/datasets/{dataset_id}/metadata          -> name, columns, coverage, etc.
# GET  {API_BASE}/datasets/{dataset_id}/poll-download      -> {"data": {"url": "<presigned csv url>"}}

# ---------------------------------------------------------------------------
# Business date window
# ---------------------------------------------------------------------------
# Applied downstream during validation (src/validate.validate_date), not at
# extraction — all files/rows are ingested; out-of-window rows are routed to
# the failed ledger with a reason, rather than never being fetched at all.
WINDOW_START = "1900-01"   # inclusive, format YYYY-MM
WINDOW_END = "2026-12"     # inclusive, format YYYY-MM

# ---------------------------------------------------------------------------
# Lease assumptions
# ---------------------------------------------------------------------------
HDB_LEASE_YEARS = 99

# ---------------------------------------------------------------------------
# Validation thresholds
# ---------------------------------------------------------------------------
# town, flat_type, and flat_model are ALL validated statistically against the
# master dataset's own frequency distribution (src/validate.py + src/profile.py)
# rather than against a hardcoded whitelist — a category is treated as a
# likely data-entry error if it occurs less often than this % of total rows.
# This one threshold is a genuine judgement call (documented in
# docs/ASSUMPTIONS.md); everything else is derived, not asserted.
RARE_CATEGORY_PCT = 0.1  # percent of total rows

# storey_range sanity bounds — a physical floor/ceiling check only. The
# *actual* upper bound used at runtime is max(MAX_STOREY, observed max in the
# dataset), so this never overrides real data, it only guards against
# obviously-broken values (e.g. "01 TO 900").
MIN_STOREY = 1
MAX_STOREY = 50

# ---------------------------------------------------------------------------
# Anomaly-detection heuristic parameters (see docs/ASSUMPTIONS.md for rationale)
# ---------------------------------------------------------------------------
IQR_MULTIPLIER = 1.5          # classic Tukey fence multiplier
ANOMALY_GROUP_KEYS = ["town", "flat_type"]  # price distributions vary a lot by these

# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------
HASH_ALGORITHM = "sha256"
