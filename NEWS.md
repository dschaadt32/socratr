# socratr 0.1.0

First release.

## New features

* `read_socrata()` — fetch Socrata datasets via SODA 3 with automatic pagination
* `read_socrata_parallel()` — concurrent page fetching for large datasets
* `write_socrata()` — chunked upsert/replace via SODA 2
* `write_socrata_parallel()` — concurrent chunk uploading for large upserts
* `ls_socrata()` — list datasets on a domain via the Discovery API
* `get_metadata()` — fetch schema and dataset-level metadata without downloading rows
* `coerce_socrata_types()` — apply Socrata schema types to a tibble post-read
* `tune_socrata_parallel()` — benchmark `max_active` and `page_size` for a dataset
* `is_four_by_four()` and `posixify()` — utility functions

## Acknowledgements

Inspired by [`RSocrata`](https://github.com/Chicago/RSocrata) (City of Chicago, 2014),
which pioneered R access to Socrata portals.