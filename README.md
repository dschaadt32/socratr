# socratr

A fast, modern R interface to [Socrata](https://www.tylertech.com/products/data-insights) open data portals.

- **Read** via SODA 3 (`POST /api/v3/views/{id}/query.json`) with automatic pagination
- **Write** via SODA 2 (`POST|PUT /resource/{id}.json`) with chunked upsert/replace
- **List** datasets via the Socrata Discovery API v1
- **Parallel** read and write for large datasets

---

## Installation

```r
# install.packages("devtools")
devtools::install_github("dschaadt/socratr")
```

---

## Quick start

```r
library(socratr)

# Read a full dataset
df <- read_socrata("https://data.example.gov/resource/abcd-1234")

# With a SoQL filter and app token
df <- read_socrata(
  url       = "https://data.example.gov/resource/abcd-1234",
  soql      = "SELECT name, opened_date WHERE status = 'Open'",
  app_token = Sys.getenv("SOCRATA_APP_TOKEN")
)

# List datasets on a domain
ls_socrata("data.example.gov")

# Upload data
write_socrata(
  dataframe    = my_df,
  domain       = "data.example.gov",
  dataset_id   = "abcd-1234",
  update_mode  = "UPSERT",
  socrata_user = Sys.getenv("SOCRATA_USER"),
  password     = Sys.getenv("SOCRATA_KEY")
)
```

---

## Functions

### `read_socrata()`

Fetches a dataset via SODA 3. Pagination is handled automatically — callers never touch page numbers. All columns are returned as character strings; use `coerce_socrata_types()` or `posixify()` / `as.numeric()` to convert as needed.

```r
df <- read_socrata(
  url          = "https://data.example.gov/resource/abcd-1234",
  soql         = "SELECT name, value WHERE value > 100 ORDER BY value DESC",
  app_token    = Sys.getenv("SOCRATA_APP_TOKEN"),
  socrata_user = Sys.getenv("SOCRATA_USER"),   # required for private datasets
  password     = Sys.getenv("SOCRATA_KEY"),
  page_size    = 5000L,   # rows per request (max 50 000)
  max_rows     = Inf,     # hard cap on total rows returned
  verbose      = FALSE
)
```

**Pagination note:** SODA 3 exposes only page-number pagination — there is no server-side cursor. `read_socrata()` stops as soon as a page returns fewer rows than `page_size`. For large datasets with SoQL filters, prefer explicit `WHERE` clauses over high page numbers, as Socrata performance degrades at high offsets.

---

### `read_socrata_parallel()`

A drop-in replacement for `read_socrata()` that fetches pages concurrently. Runs a metadata preflight to determine the total row count, pre-builds all page requests, then fires them in parallel via `httr2::req_perform_parallel()`.

```r
df <- read_socrata_parallel(
  url        = "https://data.example.gov/resource/abcd-1234",
  app_token  = Sys.getenv("SOCRATA_APP_TOKEN"),
  page_size  = 5000L,
  max_active = 10L   # concurrent connections; keep ≤ 10 to avoid rate limiting
)
```

**When to use:** Datasets with more than ~10 000 rows where network latency is the bottleneck. For smaller datasets the preflight `COUNT(*)` overhead outweighs the benefit.

**Limitation:** Does not support automatic retry on failure. Failed pages are reported as warnings. Use `read_socrata()` if retry-on-failure is required.

---

### `write_socrata()`

Uploads a data frame to Socrata via SODA 2. Large uploads are split into chunks automatically.

```r
write_socrata(
  dataframe    = my_df,
  domain       = "data.example.gov",
  dataset_id   = "abcd-1234",
  update_mode  = "UPSERT",   # or "REPLACE"
  socrata_user = Sys.getenv("SOCRATA_USER"),
  password     = Sys.getenv("SOCRATA_KEY"),
  app_token    = Sys.getenv("SOCRATA_APP_TOKEN"),
  chunk_size   = 10000L
)
```

| `update_mode` | HTTP method | Behaviour |
|---|---|---|
| `"UPSERT"` | `POST` | Add or update rows by primary key |
| `"REPLACE"` | `PUT` | Overwrite the full dataset atomically |

---

### `write_socrata_parallel()`

A drop-in replacement for `write_socrata()` for large `UPSERT` operations. Sends chunks concurrently. `REPLACE` mode always falls back to `write_socrata()` since it must be a single atomic `PUT`.

```r
write_socrata_parallel(
  dataframe    = my_large_df,
  domain       = "data.example.gov",
  dataset_id   = "abcd-1234",
  update_mode  = "UPSERT",
  socrata_user = Sys.getenv("SOCRATA_USER"),
  password     = Sys.getenv("SOCRATA_KEY"),
  chunk_size   = 10000L,
  max_active   = 10L
)
```

---

### `ls_socrata()`

Lists datasets available on a Socrata domain using the Discovery API.

```r
ls_socrata("data.example.gov")
ls_socrata("data.example.gov", search = "permits", limit = 20)
```

Returns a tibble with columns: `name`, `id`, `type`, `updated` (POSIXct), `description`, `url`.

---

### `get_metadata()`

Fetches schema and dataset-level metadata without downloading any rows. Useful for inspecting column types before reading, or for building dynamic SoQL `SELECT` clauses.

```r
meta <- get_metadata("https://data.example.gov/resource/abcd-1234")

meta$name        # dataset display name
meta$row_count   # approximate row count
meta$updated     # last-modified timestamp (POSIXct)
meta$columns     # tibble: field_name, display_name, data_type, description

# Build a SELECT from metadata
fields <- paste(meta$columns$field_name, collapse = ", ")
df <- read_socrata(
  "https://data.example.gov/resource/abcd-1234",
  soql = paste("SELECT", fields)
)
```

---

### `coerce_socrata_types()`

Applies type conversions to a tibble returned by `read_socrata()`, based on the schema from `get_metadata()`. Numeric columns become `numeric`, date columns become `POSIXct`, checkbox columns become `logical`.

```r
meta <- get_metadata("https://data.example.gov/resource/abcd-1234")
df   <- read_socrata("https://data.example.gov/resource/abcd-1234")
df   <- coerce_socrata_types(df, meta)
```

---

### `tune_socrata_parallel()`

Benchmarks `max_active` and `page_size` for a specific dataset to find the optimal settings for `read_socrata_parallel()`. Runs two sequential sweeps and saves a two-panel plot.

```r
tune <- tune_socrata_parallel(
  url       = "https://data.example.gov/resource/abcd-1234",
  app_token = Sys.getenv("SOCRATA_APP_TOKEN"),
  path      = "socrata_tune.png"
)

tune$optimal_max_active
tune$optimal_page_size
```

---

### Utilities

| Function | Description |
|---|---|
| `is_four_by_four(x)` | Returns `TRUE` if `x` matches the `xxxx-xxxx` Socrata dataset ID format |
| `posixify(x)` | Parses ISO 8601 and `mm/dd/yyyy` date strings to `POSIXct` |

---

## Authentication

All read and write functions accept `app_token`, `socrata_user`, and `password`. Store credentials in environment variables rather than in scripts:

```r
Sys.setenv(
  SOCRATA_APP_TOKEN = "your_token",
  SOCRATA_USER      = "your@email.com",
  SOCRATA_KEY       = "your_password_or_api_key"
)
```

An app token is not required but is strongly recommended — it raises the anonymous rate limit significantly. Register for one at [dev.socrata.com/register](https://dev.socrata.com/register).

---

## Acknowledgements

`socratr` was inspired by [`RSocrata`](https://github.com/Chicago/RSocrata), 
originally developed by the City of Chicago. While `socratr` is a ground-up 
rewrite using SODA 3, `httr2`, and a parallel fetch architecture, RSocrata 
pioneered R access to Socrata portals and deserves full credit for that. I 
would also like to thank the SomerStat office in the City of Somerville 
for supporting the delopment process of `socratr`.

## Issues

Please report bugs and feature requests via [GitHub Issues](https://github.com/your-org/socratr/issues).
