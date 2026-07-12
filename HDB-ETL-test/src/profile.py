"""
profile.py
----------
Lightweight, dependency-free data profiling.

Rather than pulling in a heavy third-party profiling framework (e.g.
ydata-profiling) as a hard runtime dependency, this module implements the
handful of profiling primitives the ETL actually needs to *derive its
validation rules from the data itself* (per the "based on the statistical
properties of this master dataset" requirement):

  * per-column completeness / cardinality / dtype inference
  * frequency tables for categorical columns (town, flat_type, flat_model)
  * numeric distribution stats (min/max/mean/std/quantiles) used for
    storey_range and resale_price outlier bounds

An optional `run_ydata_profiling()` helper is also provided for teams that
want the full HTML profiling report as a supplementary artifact; it degrades
gracefully (logs a warning) if the package isn't installed, since it is not
required for the pipeline to run end-to-end.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def column_summary(df: pd.DataFrame) -> pd.DataFrame:
    """One row per column: dtype, non-null %, unique count, sample values."""
    rows = []
    n = len(df)
    for col in df.columns:
        s = df[col]
        non_null = s.notna().sum()
        rows.append({
            "column": col,
            "dtype": str(s.dtype),
            "non_null_count": non_null,
            "null_pct": round(100 * (1 - non_null / n), 2) if n else 0.0,
            "n_unique": s.nunique(dropna=True),
            "sample_values": list(s.dropna().unique()[:5]),
        })
    return pd.DataFrame(rows)


def frequency_table(df: pd.DataFrame, column: str, top_n: int = 50) -> pd.DataFrame:
    """Value counts for a categorical column, used to spot rare/typo'd categories."""
    vc = df[column].value_counts(dropna=False).head(top_n)
    return vc.rename_axis(column).reset_index(name="count")


def numeric_stats(df: pd.DataFrame, column: str) -> Dict[str, float]:
    """Core distribution stats for a numeric column."""
    s = pd.to_numeric(df[column], errors="coerce")
    return {
        "count": int(s.notna().sum()),
        "min": s.min(),
        "max": s.max(),
        "mean": s.mean(),
        "std": s.std(),
        "p01": s.quantile(0.01),
        "p25": s.quantile(0.25),
        "p50": s.quantile(0.50),
        "p75": s.quantile(0.75),
        "p99": s.quantile(0.99),
    }


def rare_category_threshold(freq_df: pd.DataFrame, count_col: str = "count",
                             rare_pct: float = 0.1) -> float:
    """
    Returns the absolute count below which a category is considered "rare"
    (i.e. < rare_pct% of total observations) — used to flag likely data-entry
    typos in free-text-ish categorical fields such as flat_model.
    """
    total = freq_df[count_col].sum()
    return max(1, total * (rare_pct / 100))


def run_ydata_profiling(df: pd.DataFrame, output_html: Path) -> bool:
    """
    Optional: generate a full HTML profiling report using ydata-profiling,
    if the package is available. Returns True on success, False if skipped.
    """
    try:
        from ydata_profiling import ProfileReport  # type: ignore
    except ImportError:
        logger.warning(
            "ydata-profiling not installed; skipping HTML profiling report. "
            "Install with `pip install ydata-profiling` to enable it."
        )
        return False

    profile = ProfileReport(df, title="HDB Resale Flat Prices - Master Dataset Profile",
                             minimal=True)
    profile.to_file(output_html)
    logger.info("Wrote profiling report to %s", output_html)
    return True


def build_profile_report(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Convenience bundle of the profiling artifacts the validation rules rely on."""
    report = {"column_summary": column_summary(df)}
    for cat_col in ("town", "flat_type", "flat_model", "storey_range"):
        if cat_col in df.columns:
            report[f"freq_{cat_col}"] = frequency_table(df, cat_col)
    return report
