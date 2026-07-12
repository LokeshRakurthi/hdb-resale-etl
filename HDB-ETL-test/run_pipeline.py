"""
run_pipeline.py
----------------
End-to-end orchestration of the HDB Resale Flat Prices ETL pipeline.

Usage:
    python run_pipeline.py [--force-download] [--use-sample-data]

Outputs (all under data/):
    raw/           raw CSVs as downloaded, untouched
    cleaned/       records that pass all data-quality rules
    failed/        records rejected at any stage, with a reason column
    transformed/   cleaned data + resale_identifier
    hashed/        cleaned data + hashed resale_identifier
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd

import config
from src import extract, profile, validate, clean, transform, hash_utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pipeline")


def run(force_download: bool = False, use_sample_data: bool = False) -> None:
    all_failed_frames = []

    # ------------------------------------------------------------------
    # 1. EXTRACT
    # ------------------------------------------------------------------
    logger.info("STEP 1/6: Extraction")
    if use_sample_data:
        from tests.generate_sample_data import build_sample_master_dataset
        master = build_sample_master_dataset()
        logger.info("Using generated sample dataset (%d rows) - see --use-sample-data flag", len(master))
    else:
        master = extract.extract(force_download=force_download)

    master.to_csv(config.RAW_DIR / "master_raw_combined.csv", index=False)

    # ------------------------------------------------------------------
    # 2. PROFILE
    # ------------------------------------------------------------------
    logger.info("STEP 2/6: Profiling")
    report = profile.build_profile_report(master)
    for name, rep_df in report.items():
        rep_df.to_csv(config.DATA_DIR / f"profile_{name}.csv", index=False)
    logger.info("Profiling artifacts written to %s", config.DATA_DIR)

    # ------------------------------------------------------------------
    # 3. CLEAN (type coercion, remaining lease, validation, dedup, anomalies)
    # ------------------------------------------------------------------
    logger.info("STEP 3/6: Cleaning & Validation")
    typed = clean.coerce_types(master)
    typed = clean.compute_remaining_lease(typed)

    validated = validate.run_all_validations(typed)

    valid_df = validated.loc[validated["is_valid"]].copy()
    invalid_df = validated.loc[~validated["is_valid"]].copy()
    if len(invalid_df):
        invalid_df["failure_reason"] = invalid_df["validation_reason"]
        all_failed_frames.append(invalid_df)
    logger.info("Validation: %d passed, %d failed", len(valid_df), len(invalid_df))

    deduped_df, dup_failed_df = clean.resolve_duplicate_keys(valid_df)
    if len(dup_failed_df):
        all_failed_frames.append(dup_failed_df)

    cleaned_df = clean.flag_anomalous_prices(deduped_df)
    cleaned_df.to_csv(config.CLEANED_DIR / "cleaned_resale_prices.csv", index=False)
    logger.info("Cleaned dataset written: %d rows", len(cleaned_df))

    # ------------------------------------------------------------------
    # 4. TRANSFORM (resale identifier)
    # ------------------------------------------------------------------
    logger.info("STEP 4/6: Transformation")
    transformed_df = transform.build_resale_identifier(cleaned_df)
    transformed_df, transform_dup_failed = transform.resolve_duplicate_transformed_rows(transformed_df)
    if len(transform_dup_failed):
        all_failed_frames.append(transform_dup_failed)

    transformed_df.to_csv(config.TRANSFORMED_DIR / "transformed_resale_prices.csv", index=False)
    logger.info("Transformed dataset written: %d rows", len(transformed_df))

    # ------------------------------------------------------------------
    # 5. HASH
    # ------------------------------------------------------------------
    logger.info("STEP 5/6: Hashing")
    hashed_df = hash_utils.hash_identifier_column(transformed_df)
    hashed_df.to_csv(config.HASHED_DIR / "hashed_resale_prices.csv", index=False)
    logger.info("Hashed dataset written: %d rows", len(hashed_df))

    # ------------------------------------------------------------------
    # 6. FAILED LEDGER
    # ------------------------------------------------------------------
    logger.info("STEP 6/6: Writing failed-records ledger")
    if all_failed_frames:
        failed_df = pd.concat(all_failed_frames, axis=0, ignore_index=True, sort=False)
    else:
        failed_df = pd.DataFrame()
    failed_df.to_csv(config.FAILED_DIR / "failed_records.csv", index=False)
    logger.info("Failed ledger written: %d rows", len(failed_df))

    logger.info("Pipeline complete. raw=%d cleaned=%d transformed=%d hashed=%d failed=%d",
                len(master), len(cleaned_df), len(transformed_df), len(hashed_df), len(failed_df))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HDB Resale Flat Prices ETL Pipeline")
    parser.add_argument("--force-download", action="store_true",
                         help="Re-download raw files even if they already exist locally")
    parser.add_argument("--use-sample-data", action="store_true",
                         help="Use generated sample data instead of live data.gov.sg download "
                              "(useful when data.gov.sg is unreachable, e.g. in a sandboxed network)")
    args = parser.parse_args()
    run(force_download=args.force_download, use_sample_data=args.use_sample_data)
