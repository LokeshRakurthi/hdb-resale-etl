"""
validate.py
-----------
Field-level and record-level validation rules.

Each `validate_*` function takes the master dataframe and returns a boolean
Series ("is_valid" mask, True = passes the rule) plus a human-readable reason
string used for the failed-records ledger.

`town`, `flat_type`, and `flat_model` are all validated the SAME way: purely
from the statistical properties of the master dataset itself (frequency
distributions, via `validate_categorical_by_rarity` below) — a category is
flagged invalid only if it's a statistical outlier (occurs far less often
than everything else), never against a hardcoded whitelist. Earlier versions
of this module cross-checked `town`/`flat_type` against fixed lists in
config.py; those have been removed (see docs/ASSUMPTIONS.md) because a
hardcoded list of "correct" values is itself an unverified assumption and
goes stale the moment HDB adds/renames a town or category — exactly the
"as it is, without manual modification" principle the assignment asks us to
respect, applied to judgement calls as well as to the raw file.
"""
from __future__ import annotations

import re
from typing import Tuple

import numpy as np
import pandas as pd

import config
from src.profile import frequency_table, rare_category_threshold

DATE_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
STOREY_PATTERN = re.compile(r"^\s*(\d{1,2})\s*TO\s*(\d{1,2})\s*$", re.IGNORECASE)


def validate_date(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    `month` must match YYYY-MM and fall within the assignment window
    (2012-01 to 2016-12 inclusive). Anything outside the window isn't wrong
    data per se, but it's outside scope for this ETL run and is treated as
    a validation failure so it's routed to the "failed" (out-of-scope) ledger.
    """
    s = df["month"].astype(str).str.strip()
    fmt_ok = s.str.match(DATE_PATTERN)
    in_window = fmt_ok & (s >= config.WINDOW_START) & (s <= config.WINDOW_END)

    reason = pd.Series(np.where(~fmt_ok, "invalid month format (expected YYYY-MM)",
                        np.where(~in_window, "month outside Jan2012-Dec2016 window", "")),
                        index=df.index)
    return in_window, reason


def validate_categorical_by_rarity(df: pd.DataFrame, column: str,
                                    rare_pct: float = config.RARE_CATEGORY_PCT
                                    ) -> Tuple[pd.Series, pd.Series]:
    """
    Generic statistical validation for a categorical column: a value is
    considered valid unless it is a "rare category" in the master dataset —
    i.e. it occurs in fewer than `rare_pct`% of rows. This is the single rule
    used for `town`, `flat_type`, and `flat_model`, since all three are
    closed-ish categorical fields where a genuinely wrong/typo'd value should
    show up as a near-unique outlier once you look at the actual data,
    regardless of whether we happen to have an up-to-date reference list for
    that field.

    Trade-off, documented here and in docs/ASSUMPTIONS.md: a legitimate but
    genuinely rare category (e.g. a flat type that HDB built very few of)
    could get flagged by this rule. We accept that trade-off in exchange for
    never silently rejecting or accepting data based on a stale hardcoded
    list — every flagged row is routed to the failed ledger with a
    reason, so a reviewer can always override a false positive.
    """
    s = df[column].astype(str).str.strip().str.upper()
    freq = frequency_table(df.assign(**{column: s}), column)
    rare_cutoff = rare_category_threshold(freq, rare_pct=rare_pct)
    rare_values = set(freq.loc[freq["count"] < rare_cutoff, column])

    valid_mask = s.notna() & (s != "") & ~s.isin(rare_values)
    reason = pd.Series(
        np.where(~valid_mask, f"{column} is a statistical rare/likely-typo category ("
                               f"below {rare_pct}% of rows)", ""),
        index=df.index,
    )
    return valid_mask, reason


def validate_town(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """town, validated purely by frequency (see validate_categorical_by_rarity)."""
    return validate_categorical_by_rarity(df, "town")


def validate_flat_type(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """flat_type, validated purely by frequency (see validate_categorical_by_rarity)."""
    return validate_categorical_by_rarity(df, "flat_type")


def validate_flat_model(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """flat_model, validated purely by frequency (see validate_categorical_by_rarity)."""
    return validate_categorical_by_rarity(df, "flat_model")


def validate_storey_range(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """
    storey_range must match "NN TO MM" with NN <= MM, and both bounds within
    a sane physical range for HDB blocks (config.MIN_STOREY..MAX_STOREY,
    cross-checked against the max actually observed in the dataset).
    """
    s = df["storey_range"].astype(str).str.strip()
    matches = s.str.match(STOREY_PATTERN)

    lower = pd.to_numeric(s.str.extract(STOREY_PATTERN)[0], errors="coerce")
    upper = pd.to_numeric(s.str.extract(STOREY_PATTERN)[1], errors="coerce")

    observed_max = upper.max() if upper.notna().any() else config.MAX_STOREY
    upper_bound = max(config.MAX_STOREY, observed_max)

    order_ok = matches & (lower <= upper)
    range_ok = matches & (lower >= config.MIN_STOREY) & (upper <= upper_bound)
    valid_mask = order_ok & range_ok

    reason = pd.Series(np.where(~matches, "storey_range does not match 'NN TO MM' pattern",
                        np.where(~order_ok, "storey_range lower bound exceeds upper bound",
                        np.where(~range_ok, "storey_range outside plausible bounds", ""))),
                        index=df.index)
    return valid_mask, reason


def run_all_validations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs every field validation rule and attaches:
      - one boolean `<field>_valid` column per rule
      - `is_valid`: overall AND of all rules
      - `validation_reason`: concatenated reasons for any failing rule(s)
    """
    out = df.copy()
    checks = {
        "date": validate_date,
        "town": validate_town,
        "flat_type": validate_flat_type,
        "flat_model": validate_flat_model,
        "storey_range": validate_storey_range,
    }

    overall_valid = pd.Series(True, index=out.index)
    reasons = pd.Series("", index=out.index)

    for name, fn in checks.items():
        valid_mask, reason = fn(out)
        out[f"{name}_valid"] = valid_mask
        overall_valid &= valid_mask

        has_reason = reason.astype(str) != ""
        combined = np.where(reasons == "", reason, reasons + "; " + reason)
        reasons = pd.Series(np.where(has_reason, combined, reasons), index=out.index)

    out["is_valid"] = overall_valid
    out["validation_reason"] = reasons
    return out
