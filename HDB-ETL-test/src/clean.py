"""
clean.py
--------
Data-quality transformations that turn the raw combined dataset into the
"cleaned" dataset, plus the corresponding "failed" ledger:

  1. dtype coercion for the columns the pipeline actually needs
  2. remaining lease recomputation (assume 99-year HDB lease, "as of today")
  3. duplicate composite-key resolution (keep max resale_price)
  4. anomalous resale price detection (documented heuristic below)
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Tuple

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)

CORE_COLUMNS = [
    "month", "town", "flat_type", "block", "street_name", "storey_range",
    "floor_area_sqm", "flat_model", "lease_commence_date", "resale_price",
]


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast the core columns to their working dtypes. Leaves other columns untouched."""
    out = df.copy()
    for col in ("floor_area_sqm", "resale_price", "lease_commence_date"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ("town", "flat_type", "flat_model", "block", "street_name"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    if "month" in out.columns:
        out["month"] = out["month"].astype(str).str.strip()
    return out


def compute_remaining_lease(df: pd.DataFrame, as_of: dt.date | None = None) -> pd.DataFrame:
    """
    Assumption: every HDB flat's lease is HDB_LEASE_YEARS (99) years from
    `lease_commence_date` (a year). Remaining lease = 99 years minus the time
    elapsed from 1-Jan of the commence year to `as_of` (defaults to today),
    floored (rounded down) to whole years + whole months.

    Adds two columns:
      - remaining_lease_years / remaining_lease_months (ints, floored)
      - remaining_lease (string, e.g. "61 years 3 months") for readability
    """
    out = df.copy()
    as_of = as_of or dt.date.today()

    commence_year = out["lease_commence_date"]
    lease_start = pd.to_datetime(commence_year, format="%Y", errors="coerce")

    # Whole months elapsed since 1-Jan of the lease commencement year, floored
    # (lease commencement is only known to year-granularity in the source
    # data, so we treat commencement as 1-Jan of that year — the standard,
    # conservative convention for remaining-lease disclosures).
    total_months_elapsed = (
        (as_of.year - lease_start.dt.year) * 12 + (as_of.month - lease_start.dt.month)
    )

    total_lease_months = config.HDB_LEASE_YEARS * 12
    remaining_months_total = (total_lease_months - total_months_elapsed).clip(lower=0)

    out["remaining_lease_years"] = (remaining_months_total // 12).astype("Int64")
    out["remaining_lease_months"] = (remaining_months_total % 12).astype("Int64")
    out["remaining_lease"] = (
        out["remaining_lease_years"].astype(str) + " years " +
        out["remaining_lease_months"].astype(str) + " months"
    )
    return out


def resolve_duplicate_keys(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Composite key = every column except `resale_price` (and bookkeeping
    columns added by the pipeline itself, e.g. __source_file).
    When two+ rows share the same key, keep the row with the HIGHEST
    resale_price and route the rest to the failed ledger.

    Returns (kept_df, discarded_df).
    """
    ignore_cols = {"resale_price", "__source_file", "__source_dataset_id"}
    key_cols = [c for c in df.columns if c not in ignore_cols]

    df = df.sort_values("resale_price", ascending=False)
    is_dup = df.duplicated(subset=key_cols, keep="first")

    kept = df.loc[~is_dup].copy()
    discarded = df.loc[is_dup].copy()
    if len(discarded):
        discarded["failure_reason"] = "duplicate composite key - lower resale_price discarded"
        logger.info("Duplicate-key resolution: discarded %d of %d rows", len(discarded), len(df))
    return kept, discarded


def flag_anomalous_prices(df: pd.DataFrame,
                           group_keys=None,
                           iqr_multiplier: float | None = None) -> pd.DataFrame:
    """
    Heuristic: Tukey's IQR fence, applied WITHIN each (town, flat_type) group
    rather than globally.

    Rationale (documented per assignment requirement 6):
    Resale prices vary enormously by town and flat type (a 5-room flat in a
    mature estate is not comparable to a 2-room flat in a non-mature estate).
    A single global IQR fence would flag almost all high-value, perfectly
    legitimate transactions in expensive towns as "anomalous", and would fail
    to catch a genuinely-too-cheap/too-expensive transaction inside a cheaper
    town/flat-type segment. Grouping by (town, flat_type) before applying the
    1.5x-IQR fence keeps the check locally meaningful.

    A row is flagged `is_price_anomalous=True` if resale_price falls outside
    [Q1 - k*IQR, Q3 + k*IQR] for its (town, flat_type) group. Groups with too
    few observations (<10) to compute a meaningful IQR are not flagged (all
    False) rather than risk false positives on sparse segments.

    This flags anomalies but does NOT automatically discard them, since an
    unusually high/low price is a legitimate outlier candidate for the data
    science team to inspect, not necessarily bad data. Documented as an
    assumption in docs/ASSUMPTIONS.md.
    """
    group_keys = group_keys or config.ANOMALY_GROUP_KEYS
    k = iqr_multiplier if iqr_multiplier is not None else config.IQR_MULTIPLIER

    out = df.copy()
    out["is_price_anomalous"] = False

    def _flag(group: pd.DataFrame) -> pd.Series:
        if len(group) < 10:
            return pd.Series(False, index=group.index)
        q1, q3 = group["resale_price"].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower, upper = q1 - k * iqr, q3 + k * iqr
        return ~group["resale_price"].between(lower, upper)

    flags = out.groupby(group_keys, group_keys=False).apply(_flag)
    out["is_price_anomalous"] = flags.reindex(out.index).fillna(False)
    n_flagged = int(out["is_price_anomalous"].sum())
    logger.info("Flagged %d / %d rows as price anomalies (IQR x%.1f, grouped by %s)",
                n_flagged, len(out), k, group_keys)
    return out
