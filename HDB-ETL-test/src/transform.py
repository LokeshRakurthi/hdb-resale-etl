"""
transform.py
------------
Implements the Data Transformation Requirements:

  1. Build the "Resale Identifier" column:
       S + block(3 digits) + avg_price_2digits + month(2 digits) + town_initial
  2. Duplicate resolution (re-applied post-transform, in case the
     transformation surfaces new duplicate identifiers)
  3. Irreversible hashing of the identifier (src/hash_utils.py)
"""
from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

_DIGITS_ONLY = re.compile(r"\D+")


def _block_digits(block: str) -> str:
    """
    First 3 digits of the block column after removing any non-digit
    characters, left-zero-padded to 3 characters if the block has fewer
    than 3 digits (e.g. block "19" -> "019", block "123A" -> "123").
    """
    digits = _DIGITS_ONLY.sub("", str(block))
    digits = digits[:3]  # first 3 digits
    return digits.zfill(3)


def add_avg_price_bucket(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds `avg_price_group`: the average resale_price, grouped by
    (year-month, town, flat_type) — this is the basis for the identifier's
    "2-digit average price" component.
    """
    out = df.copy()
    out["avg_price_group"] = out.groupby(["month", "town", "flat_type"])["resale_price"] \
                                 .transform("mean")
    return out


def _avg_price_two_digits(avg_price: float) -> str:
    """
    "Taking the 1st and 2nd digit of the average resale price" — interpreted
    as the first two digits of the integer part of the average price when
    written out normally (e.g. $230,000 -> "23"; $95,500 -> "09", since the
    number read digit-by-digit starts with 0-9-5-5-0-0).
    """
    if pd.isna(avg_price):
        return "00"
    int_part = str(int(round(avg_price)))
    int_part = int_part.zfill(2)  # guard tiny values
    return int_part[:2]


def _month_two_digits(month_str: str) -> str:
    """Last two digits of the identifier: the MM part of a YYYY-MM month string."""
    return str(month_str)[-2:]


def _town_initial(town: str) -> str:
    return str(town).strip()[:1].upper()


def build_resale_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the `resale_identifier` column per the assignment's composition rule."""
    out = add_avg_price_bucket(df)

    block_part = out["block"].apply(_block_digits)
    price_part = out["avg_price_group"].apply(_avg_price_two_digits)
    month_part = out["month"].apply(_month_two_digits)
    town_part = out["town"].apply(_town_initial)

    out["resale_identifier"] = "S" + block_part + price_part + month_part + town_part
    return out


def resolve_duplicate_transformed_rows(df: pd.DataFrame):
    """
    Requirement 2 under Data Transformation: if there are duplicate records
    at this stage, keep the higher resale_price and discard the lower one.
    We treat "duplicate" here as duplicate on the full original composite key
    (identical to src/clean.resolve_duplicate_keys) since that resolution
    already ran upstream — this is a defensive re-check in case the
    transformation step itself produced any new duplication.
    """
    from src.clean import resolve_duplicate_keys
    return resolve_duplicate_keys(df)
