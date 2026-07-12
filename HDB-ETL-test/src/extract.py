"""
extract.py
----------
Handles programmatic extraction of the HDB Resale Flat Prices dataset from
data.gov.sg and combination into a single "master" dataframe.

Design notes
------------
* No hardcoded file list: the collection's constituent dataset ids are
  discovered at runtime from data.gov.sg's own collection-metadata endpoint,
  not assumed or hand-typed. Every dataset id returned by that call is
  downloaded — nothing is pre-filtered at this stage.
* No manual downloads: every file is pulled via HTTP using data.gov.sg's
  Open Data API (poll-download pattern: request a presigned URL for a
  dataset id, then GET that URL for the actual CSV bytes).
* No manual row/column edits: files are read byte-for-byte as published.
  Any "cleaning" happens later (src/clean.py), never at extraction time.
* Because the constituent files come from different eras of HDB's own
  publishing schema, their columns differ (e.g. `remaining_lease` only
  appears in more recent files, older files don't have it at all).
  Requirement 1 asks for the union of all attributes across files, so we
  concat with an outer join on columns rather than an inner join.
* The Jan-2012-to-Dec-2016 scoping the assignment asks for is applied
  downstream (src/validate.validate_date), not by pre-selecting which files
  to fetch — so a file that only partially overlaps the window (e.g. one
  covering 2000-Feb2012) is still ingested in full, and only its
  out-of-window rows are excluded, auditably, later in the pipeline.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import time

import config

logger = logging.getLogger(__name__)


def discover_collection_dataset_ids(collection_id: str = config.DATA_GOV_SG_COLLECTION_ID,
                                     timeout: int = 30) -> List[str]:
    """
    Queries data.gov.sg's collection-metadata endpoint for the current, full
    list of dataset ids that belong to a collection (e.g. the HDB Resale
    Flat Prices collection, id 189) and returns them.

    This is the dynamic replacement for hardcoding a fixed dict of dataset
    ids: whatever files data.gov.sg currently publishes under this
    collection — however many there are — get discovered and returned here.

    The exact key name for the child-dataset-id list in data.gov.sg's Open
    API response has changed across API versions in the past (seen as
    `childDatasets`, or nested under `data.collectionMetadata.childDatasets`
    in different releases). To stay robust to that without guessing wrong,
    this function tries the known key paths in order and falls back to
    logging the full raw response if none match, so a maintainer can update
    the parsing in one place rather than the pipeline silently mis-behaving.
    """
    endpoint = f"{config.DATA_GOV_SG_METADATA_API_BASE}/collections/{collection_id}/metadata"
    resp = requests.get(endpoint, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()

    candidate_paths = [
        lambda p: p["data"]["collectionMetadata"]["childDatasets"],
        lambda p: p["data"]["childDatasets"],
        lambda p: p["childDatasets"],
    ]
    for get_path in candidate_paths:
        try:
            dataset_ids = get_path(payload)
            if dataset_ids:
                logger.info("Discovered %d dataset id(s) in collection %s: %s",
                            len(dataset_ids), collection_id, dataset_ids)
                return list(dataset_ids)
        except (KeyError, TypeError):
            continue

    raise RuntimeError(
        f"Could not locate child dataset ids in collection {collection_id} metadata "
        f"response. Raw payload for debugging: {payload}"
    )


def get_dataset_metadata(dataset_id: str, timeout: int = 30) -> dict:
    """Fetches a single dataset's metadata (name, column list, coverage period, etc.)."""
    endpoint = f"{config.DATA_GOV_SG_METADATA_API_BASE}/datasets/{dataset_id}/metadata"
    resp = requests.get(endpoint, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _poll_download_url(dataset_id: str,
                       timeout: int = 30,
                       max_retries: int = 6) -> str:

    endpoint = (
        f"{config.DATA_GOV_SG_DOWNLOAD_API_BASE}/datasets/"
        f"{dataset_id}/poll-download"
    )

    delay = 2

    for attempt in range(max_retries):

        response = requests.get(endpoint, timeout=timeout)

        if response.status_code == 429:
            print(f"Rate limited. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2
            continue

        response.raise_for_status()

        payload = response.json()

        url = payload.get("data", {}).get("url")

        if url:
            return url

        raise RuntimeError(
            f"No download URL returned for dataset {dataset_id}"
        )

    raise RuntimeError("Exceeded maximum retry attempts.")


def download_dataset(dataset_id: str, dest_dir: Path = config.RAW_DIR,
                      force: bool = False, timeout: int = 60) -> Path:
    """
    Download a single data.gov.sg dataset (by its dataset id) to
    `dest_dir/<dataset_id>.csv`. Idempotent: skips re-download if the file
    already exists, unless force=True. Filenames are keyed by dataset id
    (not a hand-picked friendly name) precisely because the set of files is
    discovered dynamically and we don't want to invent labels for files we
    haven't hardcoded knowledge of.
    """

    name_url = f"{config.DATA_GOV_SG_METADATA_API_BASE}/datasets/{dataset_id}/metadata"
    name_resp = requests.get(name_url)
    name_payload = name_resp.json()
    dataset_name = name_payload['data']['name']
    dest_path = dest_dir / f"{dataset_name}.csv"
    if dest_path.exists() and not force:
        logger.info("Raw file already present, skipping download: %s", dest_path)
        return dest_path

    logger.info("Requesting presigned download URL for dataset id=%s", dataset_id)
    csv_url = _poll_download_url(dataset_id, timeout=timeout)

    logger.info("Downloading %s -> %s", csv_url, dest_path)
    resp = requests.get(csv_url, timeout=timeout)
    resp.raise_for_status()
    dest_path.write_bytes(resp.content)
    return dest_path


def download_all(dataset_ids: Optional[List[str]] = None, force: bool = False) -> Dict[str, Path]:
    """
    Downloads every dataset in `dataset_ids`. If not provided, discovers the
    full, current list of dataset ids from the collection at runtime
    (see discover_collection_dataset_ids). Returns {dataset_id: local_path}.
    """
    dataset_ids = dataset_ids or discover_collection_dataset_ids()
    paths = {}
    for dsid in dataset_ids:
        try:
            paths[dsid] = download_dataset(dsid, force=force)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to download dataset %s: %s", dsid, exc)
            raise
    return paths


def load_raw_files(paths: Dict[str, Path]) -> Dict[str, pd.DataFrame]:
    """Read every raw CSV as-is (no dtype coercion, no dropped rows/columns)."""
    frames = {}
    for dsid, path in paths.items():
        df = pd.read_csv(path, dtype=str)  # read as string first; typing happens in clean.py
        df["__source_dataset_id"] = dsid
        frames[dsid] = df
        logger.info("Loaded %s: %d rows, %d columns", dsid, len(df), df.shape[1])
    return frames


def combine_master_dataset(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Union all source files into one master dataframe.
    Uses an outer concat so the resulting frame contains every attribute that
    appears in ANY source file (requirement: "contain all the attributes
    found in all files"). Columns missing in a given source are filled with NaN.
    """
    master = pd.concat(frames.values(), axis=0, ignore_index=True, sort=False, join="outer")
    logger.info("Combined master dataset: %d rows, %d columns (from %d source file(s))",
                len(master), master.shape[1], len(frames))
    return master


def extract(force_download: bool = False) -> pd.DataFrame:
    """
    End-to-end extraction entrypoint: discover every file in the collection
    -> download all of them -> load -> combine. No file selection/filtering
    happens here; date-window scoping is applied later, during validation.
    """
    dataset_ids = discover_collection_dataset_ids()
    raw_paths = download_all(dataset_ids=dataset_ids, force=force_download)
    frames = load_raw_files(raw_paths)
    master = combine_master_dataset(frames)
    return master


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = extract()
    print(df.head())
    print(df.shape)
