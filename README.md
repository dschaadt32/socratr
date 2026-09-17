# socratr

A fast R interface to [Socrata](https://dev.socrata.com) open data portals.

- **Read** via SODA 3 JSON or SODA 2 CSV, with automatic pagination
- **Coerce** column types from schema metadata (`coerce = TRUE`)
- **Write** via SODA 2 upsert / replace (chunked, optional parallel)
- **List** datasets via the Discovery API
- **Parallel** read and write for large datasets

## Installation

```r
# install.packages("devtools")
devtools::install_github("dschaadt32/socratr")
```

## Quick start

```r
library(socratr)

# JSON (SODA 3) — default
df <- read_socrata("https://data.example.gov/resource/abcd-1234")

# CSV (SODA 2) — often faster for large public datasets
df <- read_socrata(
  "https://data.example.gov/resource/abcd-1234",
  format = "csv",
  coerce = TRUE,
  app_token = Sys.getenv("SOCRATA_APP_TOKEN")
)

# Parallel fetch
df <- read_socrata_parallel(
  "https://data.example.gov/resource/abcd-1234",
  format = "csv",
  max_active = 10L,
  app_token = Sys.getenv("SOCRATA_APP_TOKEN")
)

# List datasets
ls_socrata("data.example.gov")

# Upload
write_socrata(
  dataframe    = my_df,
  domain       = "data.example.gov",
  dataset_id   = "abcd-1234",
  update_mode  = "UPSERT",
  socrata_user = Sys.getenv("SOCRATA_USER"),
  password     = Sys.getenv("SOCRATA_PASSWORD")
)
```

## Authentication

Store credentials in environment variables (never commit them):

```r
Sys.setenv(
  SOCRATA_APP_TOKEN  = "your_app_token",
  SOCRATA_USER       = "your@email.com_or_api_key_id",
  SOCRATA_PASSWORD   = "your_password_or_api_secret"
)
```

An app token is optional but strongly recommended — it raises anonymous
rate limits. Register at [dev.socrata.com/register](https://dev.socrata.com/register).

Live tests look for `SOCRATA_USER`, `SOCRATA_PASSWORD`, and `SOCRATA_TOKEN`
(alias for the app token).

## Functions

### `read_socrata()`

Fetches a dataset with automatic pagination.

| Argument | Notes |
|---|---|
| `format` | `"json"` (SODA 3, default) or `"csv"` (SODA 2) |
| `coerce` | `TRUE` → dates / numbers / checkboxes typed from metadata |
| `page_size` | Default 5 000 (JSON) or 50 000 (CSV); max 50 000 |
| `soql` | SoQL string; do not include `LIMIT` / `OFFSET` |

```r
df <- read_socrata(
  url       = "https://data.example.gov/resource/abcd-1234",
  soql      = "SELECT name, value WHERE value > 100 ORDER BY value DESC",
  app_token = Sys.getenv("SOCRATA_APP_TOKEN"),
  format    = "json",
  coerce    = FALSE
)
```

### `read_socrata_parallel()`

Same interface as `read_socrata()`, but fetches pages concurrently. Supports
both JSON and CSV. Failed pages abort the whole read (no silent partial
results). Prefer the serial reader when you need per-page retries.

```r
df <- read_socrata_parallel(
  url        = "https://data.example.gov/resource/abcd-1234",
  format     = "csv",
  max_active = 10L,
  app_token  = Sys.getenv("SOCRATA_APP_TOKEN")
)
```

### `write_socrata()` / `write_socrata_parallel()`

Upload via SODA 2. `UPSERT` can be chunked (and parallelized);
`REPLACE` is always a single atomic `PUT`.

```r
write_socrata(
  dataframe    = my_df,
  domain       = "data.example.gov",
  dataset_id   = "abcd-1234",
  update_mode  = "UPSERT",  # or "REPLACE"
  socrata_user = Sys.getenv("SOCRATA_USER"),
  password     = Sys.getenv("SOCRATA_PASSWORD"),
  chunk_size   = 10000L
)
```

### `ls_socrata()` / `get_metadata()` / `coerce_socrata_types()`

```r
ls_socrata("data.example.gov", search = "permits", limit = 20)

meta <- get_metadata("https://data.example.gov/resource/abcd-1234")
meta$columns

df <- read_socrata("https://data.example.gov/resource/abcd-1234")
df <- coerce_socrata_types(df, meta)  # or coerce = TRUE on read
```

### `tune_socrata_parallel()`

Benchmarks `max_active` and `page_size` for a dataset (requires
`ggplot2`, `patchwork`, `scales`, `dplyr`).

### Utilities

| Function | Description |
|---|---|
| `is_four_by_four(x)` | Valid `xxxx-xxxx` dataset ID? |
| `posixify(x)` | Parse ISO 8601 / `mm/dd/yyyy` to `POSIXct` |

## Performance notes

On a ~1.2M-row portal dataset (Somerville 311), approximate wall times were:

| Method | Elapsed |
|---|---:|
| RSocrata JSON | ~140 s |
| RSocrata CSV | ~94 s |
| socratr sequential JSON | ~128 s |
| socratr parallel CSV/JSON | ~9–10 s |

Parallel reads shine above ~10k rows. For smaller pulls, sequential is fine.

## Acknowledgements

Inspired by [`RSocrata`](https://github.com/Chicago/RSocrata) (City of
Chicago). Thanks to the SomerStat office in the City of Somerville for
supporting development.

## Issues

[GitHub Issues](https://github.com/dschaadt32/socratr/issues)
