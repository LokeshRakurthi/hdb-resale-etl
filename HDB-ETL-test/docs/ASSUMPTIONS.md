# Assumptions & Heuristics

This document consolidates every assumption and heuristic used in the Part 1 ETL pipeline,
as required by the assignment ("document your heuristic and assumptions").

## 1. Source data scope
- The assignment window is Jan-2012 to Dec-2016. HDB's own data.gov.sg collection
  (id 189) splits resale transactions across multiple constituent files by
  registration/approval-date era.
- **No file ids are hardcoded.** `src/extract.discover_collection_dataset_ids` queries
  data.gov.sg's collection-metadata API endpoint at runtime for the collection's current,
  full list of constituent dataset ids, and `download_all` fetches **every one of them** —
  not a pre-selected subset. This was a deliberate correction: an earlier version of this
  pipeline hardcoded 3 dataset ids based on general knowledge of the collection's contents,
  which is exactly the kind of unverified, staleness-prone assumption the assignment asks
  us to avoid (if data.gov.sg adds, removes, renames, or restructures a file, a hardcoded
  list silently goes wrong with no signal that anything changed).
- Each discovered file is ingested **in full** (no manual row/column deletion), including
  files that only partially overlap the assignment window (e.g. a file spanning
  2000-Feb2012). Out-of-window rows are filtered downstream by the `month` validation rule
  and routed to the `failed` ledger with reason `"month outside Jan2012-Dec2016 window"`
  rather than silently dropped at extraction time — so the exclusion is auditable, and the
  scoping decision lives in one place (`src/validate.validate_date`), not scattered across
  which files happen to get fetched.
- If data.gov.sg's collection-metadata response shape ever changes, `discover_collection_dataset_ids`
  is written to try several known key-path variants and raise a clear error with the raw
  payload if none match — so a maintainer fixes one parsing function rather than the
  pipeline silently fetching the wrong (or no) files.

## 2. Combining files into a master dataset
- Requirement 1 asks for a single master dataset containing **all attributes found in all
  files**. Since HDB's schema evolved over time (e.g. `remaining_lease` was only added to
  the most recent files), an **outer join on columns** is used when concatenating
  (`pd.concat(..., join="outer")`). Missing attributes for a given source file are `NaN`.

## 3. Remaining lease computation
- Assumption: every flat's total lease is exactly `HDB_LEASE_YEARS = 99` years.
- `lease_commence_date` in the source data is a **year only**. We treat the commencement
  date as 1-Jan of that year (the standard, conservative convention used in HDB's own
  remaining-lease disclosures).
- Remaining lease = 99 years − (whole months elapsed from 1-Jan of the commence year to
  today), **floored** to whole years + whole months, per the requirement.

## 4. Validation rules — derived from statistical properties, not hardcoded
- **month**: must match `YYYY-MM` and fall inside the Jan-2012–Dec-2016 window. This is the
  one rule that references a fixed external boundary (the assignment's own scope), not a
  data-derived one.
- **town, flat_type, flat_model**: all three are validated with the **same purely
  statistical rule** (`src/validate.validate_categorical_by_rarity`): a value is flagged
  invalid only if it's a rare category in the master dataset — occurring in fewer than
  `config.RARE_CATEGORY_PCT` (default 0.1%) of total rows. No hardcoded whitelist is used
  for any of the three.
  - An earlier version of this pipeline validated `town` and `flat_type` against fixed
    lists in `config.py` (drawn from general knowledge of Singapore's HDB towns/flat
    types, not derived from the dataset). That was inconsistent with how `flat_model`
    was already handled, and carried the same staleness risk as the hardcoded dataset
    ids above — a renamed/merged town, or a new flat type category, would be silently
    misclassified. Both fields now use the identical statistical rule as `flat_model`.
  - **Known trade-off, stated plainly:** a purely frequency-based rule is scale-dependent.
    On a small dataset, a single typo (e.g. a fat-fingered town name occurring once) may
    not fall below the 0.1% threshold and won't get flagged — this was observed directly
    when testing against the ~600-row synthetic sample (1 occurrence = 0.165%, above the
    0.1% cutoff). On the real dataset (hundreds of thousands of resale transactions),
    0.1% corresponds to hundreds of rows, so a genuine one-off data-entry error is
    reliably far below that threshold and gets caught correctly. This rule is only
    reliable at the real dataset's actual scale — testing against a small synthetic
    sample understates the false-negative risk and should not be read as validating the
    rule's behaviour on the full dataset.
  - The inverse risk also exists: a legitimate but genuinely rare category (e.g. a flat
    type HDB built very few of) could be flagged as invalid. Every flagged row is routed
    to the failed ledger with a specific reason, so a reviewer can always inspect and
    override a false positive rather than have it silently dropped.
- **storey_range**: must match the `"NN TO MM"` pattern with `NN <= MM`, and both bounds
  within a plausible physical range (`MIN_STOREY`=1, `MAX_STOREY`=50, whichever is larger
  between the config default and the actual observed maximum in the dataset).
- **Rare-category threshold**: defined as 0.1% of total row count
  (`config.RARE_CATEGORY_PCT`). This is a judgement call — tightening it (e.g. to 0.01%)
  would flag fewer categories as rare; loosening it would flag more. It is the single
  remaining tunable "gut-feel" parameter in the validation logic, and is called out here
  as such rather than buried in code.

## 5. Duplicate composite-key resolution
- Composite key = every column **except** `resale_price` (and internal bookkeeping columns
  like `__source_file`), per the assignment's explicit definition.
- When duplicates are found, the row with the **higher** `resale_price` is kept; the rest are
  discarded to the `failed` ledger with reason `"duplicate composite key - lower resale_price
  discarded"`.
- This same resolution is defensively re-run after the transformation step, in case building
  the `resale_identifier` column surfaces any new duplication (it shouldn't, since the
  identifier is derived from already-deduplicated data, but the check is cheap insurance).

## 6. Anomalous resale price heuristic
- **Heuristic:** Tukey's 1.5×IQR fence (`Q1 - 1.5*IQR`, `Q3 + 1.5*IQR`), computed **within
  each (town, flat_type) group** rather than globally.
- **Why grouped, not global:** resale prices vary enormously by town and flat type (e.g. a
  5-room flat in a mature, central estate is not comparable to a 2-room flat in a
  non-mature estate). A single global IQR fence would flag almost every legitimate
  high-value transaction in expensive towns as "anomalous" while failing to catch a
  genuinely mispriced transaction within a cheaper segment. Grouping first keeps the
  statistical test locally meaningful.
- **Sparse groups:** (town, flat_type) combinations with fewer than 10 observations are not
  flagged at all (rather than computing an unreliable IQR on too little data).
- **Flag, don't discard:** anomalous prices are marked (`is_price_anomalous=True`) but kept
  in the cleaned dataset — an unusually high/low price is a legitimate signal for the Data
  Science team to investigate (e.g. penthouse-level units, corner units), not necessarily bad
  data, so the pipeline does not unilaterally discard it.

## 7. Resale Identifier construction
- `avg_price_group` (used for the 2-digit price component) is computed as the mean
  `resale_price` grouped by `(month, town, flat_type)` — i.e. the exact grouping specified in
  the assignment ("year-month, town and flat_type").
- "Taking the 1st and 2nd digit of the average resale price" is interpreted as the first two
  digits of the integer part of that average, read left-to-right as printed (e.g. average
  $230,000 → "23"). Values with fewer than 2 digits are zero-padded (extremely unlikely for
  resale prices, but handled defensively).
- Block digits: non-digit characters (e.g. suffix letters like "748A") are stripped before
  taking the first 3 digits; blocks with fewer than 3 digits are zero-padded on the left
  (e.g. block "19" → "019").

## 8. Hashing
- **SHA-256**, applied to the plain (unsalted) `resale_identifier` string.
- Irreversible (one-way), and at this dataset's scale (hundreds of thousands of rows, far
  below the ~2^128 birthday-bound threshold for a 256-bit hash) the probability of an
  accidental collision is negligible, so uniqueness of the original identifier is preserved
  in the hashed output with overwhelming probability. The pipeline also runs an automatic
  uniqueness sanity check (`nunique` before/after hashing) and raises an error if a collision
  is ever detected.
- No salt is used, since the identifier is a derived, non-sensitive composite value (not raw
  PII) and determinism (same input → same hash, every run) was judged more valuable here than
  salting for this use case.

## 9. Additional cleaning judgement calls
- All string categorical fields (`town`, `flat_type`, `flat_model`, `block`, `street_name`)
  are trimmed and upper-cased before validation, to avoid spurious failures from
  inconsistent casing/whitespace across the three source files (a real, observed issue in
  HDB's historical file releases).
- Numeric columns (`floor_area_sqm`, `resale_price`, `lease_commence_date`) are coerced with
  `errors="coerce"`, turning any non-numeric garbage into `NaN` rather than crashing the
  pipeline; `NaN` resale prices would fail duplicate-key comparison and are surfaced via the
  profiling step's null-percentage column for review.
