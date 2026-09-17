# socratr 0.2.0

## New features

* `format = "csv"` on `read_socrata()` / `read_socrata_parallel()` for SODA 2
  CSV downloads (often faster on large public datasets).
* `coerce = TRUE` for automatic schema-based type coercion (dates → `POSIXct`,
  numbers → `numeric`, checkboxes → `logical`), including `floating_timestamp`
  and currency-style money fields.
* Parallel CSV reads via offset-based SODA 2 pagination.

## Bug fixes

* Restored serial `write_socrata()`; fixed infinite recursion on REPLACE via
  `write_socrata_parallel()`.
* Fixed `ls_socrata()` / `get_metadata()` crashes from an undefined `verbose`
  argument.
* Re-enabled `page_size` and `max_active` validation; empty uploads no longer
  error on `seq()`.
* Parallel reads abort on failed pages instead of returning partial results.
* Safer `NROW()` handling when a JSON page is non-tabular.

## Packaging

* Quarantined legacy `RSocrata` sources under `legacy/` (not installed).
* Cleaned NAMESPACE / DESCRIPTION; plotting deps are Suggests only.
* Fixed `tests/testthat.R` to load `socratr` (no hardcoded credentials).

# socratr 0.1.0

First release.

* `read_socrata()` — SODA 3 JSON with automatic pagination
* `read_socrata_parallel()` — concurrent page fetching
* `write_socrata()` / `write_socrata_parallel()` — SODA 2 upsert / replace
* `ls_socrata()`, `get_metadata()`, `coerce_socrata_types()`
* `tune_socrata_parallel()`, `is_four_by_four()`, `posixify()`

Inspired by [`RSocrata`](https://github.com/Chicago/RSocrata) (City of Chicago).
