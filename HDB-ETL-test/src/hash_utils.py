"""
hash_utils.py
-------------
Requirement: "Hash this identifier column using an irreversible hashing
algorithm, while preserving its uniqueness."

Algorithm chosen: SHA-256.

Why SHA-256:
  * Cryptographically irreversible (one-way) — unlike encryption or simple
    encodings (base64, ROT13), there is no key or inverse function that
    recovers the original identifier from the hash.
  * Effectively collision-free for this dataset's scale (~a few hundred
    thousand rows): SHA-256 has a 256-bit output space, so the probability
    of an accidental collision among far fewer than 2^128 inputs (the
    birthday-bound threshold) is astronomically small — uniqueness of the
    original identifier is preserved in the hashed column with overwhelming
    probability.
  * Deterministic (no random salt) so the same identifier always hashes to
    the same value, which matters for reproducibility. Since the identifier
    itself is already a derived, non-sensitive composite (not raw PII), the
    lack of salting is an acceptable trade-off documented here rather than a
    security gap for this use case.
  * Fast and available in Python's standard library (hashlib) with no extra
    dependency.

Not used / considered:
  * MD5 / SHA-1 — both are cryptographically broken (collision attacks are
    practical), so they don't meet "irreversible ... while preserving
    uniqueness" to the same standard.
  * Base64/hex encoding — reversible, not a hash at all.
  * Encryption (AES etc.) — reversible by design given the key; the
    requirement explicitly calls for irreversibility.
"""
from __future__ import annotations

import hashlib

import pandas as pd


def sha256_hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def hash_identifier_column(df: pd.DataFrame, source_col: str = "resale_identifier",
                            target_col: str = "resale_identifier_hashed") -> pd.DataFrame:
    out = df.copy()
    out[target_col] = out[source_col].apply(sha256_hash)

    # Sanity check: hashing must not introduce collisions among previously-unique ids
    n_unique_before = out[source_col].nunique()
    n_unique_after = out[target_col].nunique()
    if n_unique_after != n_unique_before:
        raise ValueError(
            f"Hash collision detected: {n_unique_before} unique identifiers "
            f"produced only {n_unique_after} unique hashes."
        )
    return out
