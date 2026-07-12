"""
generate_sample_data.py
------------------------
Generates a small, schema-accurate SYNTHETIC dataset that mimics data.gov.sg's
HDB Resale Flat Prices files for Jan-2012 to Dec-2016. This is only used for
local testing/demo purposes (e.g. --use-sample-data flag, or in a network-
restricted sandbox) — it is NOT a substitute for running the pipeline against
the real data.gov.sg files, which is what src/extract.py does by default.

The generator deliberately injects a handful of "dirty" records so that every
data-quality rule in src/validate.py and src/clean.py has something to catch:
  - a duplicate composite key (different price, same everything else)
  - an invalid town name
  - a malformed storey_range
  - an out-of-window month
  - a couple of extreme price outliers within a (town, flat_type) group
"""
from __future__ import annotations

import random

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

TOWNS = ["ANG MO KIO", "BEDOK", "CLEMENTI", "TAMPINES", "YISHUN", "QUEENSTOWN"]
FLAT_TYPES = ["2 ROOM", "3 ROOM", "4 ROOM", "5 ROOM", "EXECUTIVE"]
FLAT_MODELS = ["IMPROVED", "NEW GENERATION", "MODEL A", "MAISONETTE", "STANDARD"]
STREETS = ["ANG MO KIO AVE 3", "BEDOK NTH RD", "CLEMENTI AVE 4", "TAMPINES ST 21",
           "YISHUN RING RD", "QUEENSTOWN RD"]

BASE_PRICE = {
    "2 ROOM": 180000, "3 ROOM": 280000, "4 ROOM": 380000,
    "5 ROOM": 480000, "EXECUTIVE": 560000,
}


def _random_month():
    year = random.randint(2012, 2016)
    month = random.randint(1, 12)
    return f"{year:04d}-{month:02d}"


def build_sample_master_dataset(n_rows: int = 600) -> pd.DataFrame:
    rows = []
    for i in range(n_rows):
        town = random.choice(TOWNS)
        flat_type = random.choice(FLAT_TYPES)
        base = BASE_PRICE[flat_type]
        price = max(50000, int(np.random.normal(base, base * 0.12)))
        storey_lo = random.choice([1, 4, 7, 10, 13, 16])
        storey_hi = storey_lo + 2
        rows.append({
            "month": _random_month(),
            "town": town,
            "flat_type": flat_type,
            "block": str(random.randint(1, 999)) + random.choice(["", "A", "B"]),
            "street_name": random.choice(STREETS),
            "storey_range": f"{storey_lo:02d} TO {storey_hi:02d}",
            "floor_area_sqm": random.choice([45, 60, 68, 75, 90, 105, 120]),
            "flat_model": random.choice(FLAT_MODELS),
            "lease_commence_date": random.randint(1975, 2005),
            "resale_price": price,
            "__source_dataset_id": "sample",
        })

    df = pd.DataFrame(rows)

    # --- inject deliberately "dirty" records for testing every rule ---

    # 1) duplicate composite key (same everything except price)
    dup_row = df.iloc[0].copy()
    dup_row["resale_price"] = dup_row["resale_price"] - 5000
    df = pd.concat([df, pd.DataFrame([dup_row])], ignore_index=True)

    # 2) invalid town
    bad_town_row = df.iloc[1].copy()
    bad_town_row["town"] = "ATLANTIS"
    df = pd.concat([df, pd.DataFrame([bad_town_row])], ignore_index=True)

    # 3) malformed storey_range
    bad_storey_row = df.iloc[2].copy()
    bad_storey_row["storey_range"] = "HIGH FLOOR"
    df = pd.concat([df, pd.DataFrame([bad_storey_row])], ignore_index=True)

    # 4) out-of-window month
    oow_row = df.iloc[3].copy()
    oow_row["month"] = "2017-03"
    df = pd.concat([df, pd.DataFrame([oow_row])], ignore_index=True)

    # 5) extreme price outliers within their (town, flat_type) group
    for idx in (4, 5):
        outlier_row = df.iloc[idx].copy()
        outlier_row["resale_price"] = outlier_row["resale_price"] * 6  # extreme high
        df = pd.concat([df, pd.DataFrame([outlier_row])], ignore_index=True)

    return df.reset_index(drop=True)


if __name__ == "__main__":
    d = build_sample_master_dataset()
    print(d.shape)
    print(d.head())
