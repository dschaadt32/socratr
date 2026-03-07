library(testthat)
library(httr2)

# Credentials — set these as env vars; never hard-code in tests.
# Sys.setenv(SOCRATA_USER = "...", SOCRATA_SECRET = "...", SOCRATA_TOKEN = "...")
socrata_user <- Sys.getenv("SOCRATA_USER")
socrata_password <- Sys.getenv("SOCRATA_PASSWORD")
app_token <- Sys.getenv("SOCRATA_TOKEN")

# Shared demo dataset (1007 rows, always public)
DEMO_URL <- "https://soda.demo.socrata.com/resource/4334-bgaj"
DEMO_NROWS <- 1007L

###############################################################################
# Group 1 — Date parsing
###############################################################################

test_that("posixify: ISO 8601 (SODA 3)", {
  expect_equal(
    format(posixify("2012-09-14T22:38:01"), "%Y-%m-%d %H:%M:%S"),
    "2012-09-14 22:38:01"
  )
})

test_that("posixify: legacy Socrata CSV format (mm/dd/yyyy hh:mm:ss PM)", {
  expect_equal(
    format(posixify("09/14/2012 10:38:01 PM"), "%Y-%m-%d %H:%M:%S"),
    "2012-09-14 22:38:01"
  )
})

test_that("posixify: date-only string", {
  expect_equal(format(posixify("2012-09-14"), "%Y-%m-%d"), "2012-09-14")
})

test_that("posixify: vectorised input", {
  v <- posixify(c("2024-01-01", "2024-06-15T12:00:00"))
  expect_length(v, 2L)
  expect_s3_class(v, "POSIXct")
})

test_that("posixify: NA passthrough", {
  expect_true(is.na(posixify(NA_character_)))
})

test_that("posixify: empty input returns zero-length POSIXct", {
  result <- posixify(character(0L))
  expect_s3_class(result, "POSIXct")
  expect_length(result, 0L)
})

test_that("posixify: unparseable string warns and returns NA", {
  expect_warning(result <- posixify("not-a-date"), "could not be parsed")
  expect_true(is.na(result))
})

###############################################################################
# Group 2 — Identifier validation
###############################################################################

test_that("is_four_by_four: valid IDs", {
  expect_true(is_four_by_four("4334-bgaj"))
  expect_true(is_four_by_four("ABCD-1234")) # uppercase is valid
  expect_true(all(is_four_by_four(c("aaaa-1111", "zzzz-9999"))))
})

test_that("is_four_by_four: invalid IDs", {
  expect_false(is_four_by_four("4334-bgaj-extra"))
  expect_false(is_four_by_four("4334_bgaj"))
  expect_false(is_four_by_four("tooshort"))
  expect_false(is_four_by_four(""))
})

test_that("is_four_by_four: vectorised", {
  expect_equal(
    is_four_by_four(c("abcd-1234", "bad", "ef01-ab23")),
    c(TRUE, FALSE, TRUE)
  )
})

###############################################################################
# Group 3 — Input validation (no network)
###############################################################################

test_that("read_socrata: empty url errors", {
  expect_error(read_socrata(""), "non-empty")
  expect_error(read_socrata("   "), "non-empty")
})

test_that("read_socrata: url without http scheme errors", {
  expect_error(
    read_socrata("soda.demo.socrata.com/resource/4334-bgaj"),
    "4x4" # resolve_dataset will fail to find a valid ID in a bare domain
  )
})

test_that("read_socrata: url with no valid 4x4 errors", {
  expect_error(
    read_socrata("https://soda.demo.socrata.com/resource/invalid-id-here"),
    "4x4"
  )
})

test_that("read_socrata: bare 4x4 without domain errors", {
  expect_error(read_socrata("4334-bgaj"), "domain")
})

test_that("read_socrata: invalid page_size errors", {
  expect_error(read_socrata(DEMO_URL, page_size = 0L), "page_size")
  expect_error(read_socrata(DEMO_URL, page_size = 100000L), "page_size")
  expect_error(read_socrata(DEMO_URL, page_size = -1L), "page_size")
})

test_that("write_socrata: non-data-frame input errors", {
  expect_error(
    write_socrata(
      "not a df",
      "example.gov",
      "abcd-1234",
      socrata_user = "x",
      password = "y"
    ),
    "data frame"
  )
})

test_that("write_socrata: invalid dataset_id errors", {
  expect_error(
    write_socrata(
      data.frame(x = 1),
      "example.gov",
      "not-valid!!",
      socrata_user = "x",
      password = "y"
    ),
    "4x4"
  )
})

test_that("write_socrata: empty domain errors", {
  expect_error(
    write_socrata(
      data.frame(x = 1),
      "",
      "abcd-1234",
      socrata_user = "x",
      password = "y"
    ),
    "domain"
  )
})

test_that("ls_socrata: invalid limit errors", {
  expect_error(ls_socrata("example.gov", limit = 0L), "limit")
  expect_error(ls_socrata("example.gov", limit = -5L), "limit")
})

###############################################################################
# Group 4 — Live API: basic reads  (skip on CRAN & when creds absent)
###############################################################################

skip_live <- function() {
  skip_on_cran()
  skip_if(
    !nzchar(socrata_user) && !nzchar(app_token),
    "No Socrata credentials available — set SOCRATA_USER/SOCRATA_SECRET or SOCRATA_TOKEN."
  )
}

test_that("read_socrata: returns a tibble from a full URL", {
  skip_live()
  df <- read_socrata(DEMO_URL, app_token = app_token)
  expect_s3_class(df, "tbl_df")
  expect_gt(nrow(df), 0L)
})

test_that("read_socrata: bare 4x4 + domain works identically to full URL", {
  skip_live()
  df_url <- read_socrata(DEMO_URL, app_token = app_token)
  df_id <- read_socrata(
    "4334-bgaj",
    domain = "soda.demo.socrata.com",
    app_token = app_token
  )
  expect_equal(nrow(df_url), nrow(df_id))
  expect_equal(names(df_url), names(df_id))
})

test_that("read_socrata: soql SELECT limits columns returned", {
  skip_live()
  df <- read_socrata(DEMO_URL, soql = "SELECT region", app_token = app_token)
  expect_s3_class(df, "tbl_df")
  expect_equal(ncol(df), 1L)
  expect_named(df, "region")
})

test_that("read_socrata: soql WHERE filters rows", {
  skip_live()
  df_all <- read_socrata(DEMO_URL, app_token = app_token)
  regions <- unique(df_all$region)
  target <- regions[[1L]]
  df_filtered <- read_socrata(
    DEMO_URL,
    soql = sprintf("SELECT * WHERE region = '%s'", target),
    app_token = app_token
  )
  expect_gt(nrow(df_filtered), 0L)
  expect_lt(nrow(df_filtered), nrow(df_all))
  expect_true(all(df_filtered$region == target))
})

test_that("read_socrata: pagination retrieves all rows across multiple pages", {
  skip_live()
  # Force multiple pages by using a small page_size; verify total row count.
  df <- read_socrata(
    DEMO_URL,
    socrata_user = socrata_user,
    password = socrata_password,
    # app_token = app_token,
    page_size = 200L
  )
  expect_equal(nrow(df), DEMO_NROWS)
})

test_that("read_socrata: max_rows caps the result", {
  skip_live()
  df <- read_socrata(DEMO_URL, app_token = app_token, max_rows = 50L)
  expect_lte(nrow(df), 50L)
  expect_gt(nrow(df), 0L)
})

test_that("read_socrata: column names are clean_names'd", {
  skip_live()
  df <- read_socrata(DEMO_URL, app_token = app_token)
  # clean_names produces snake_case; no spaces or leading/trailing punctuation
  expect_true(all(grepl("^[a-z0-9_]+$", names(df))))
})

test_that("read_socrata: all columns are character", {
  skip_live()
  df <- read_socrata(DEMO_URL, app_token = app_token)
  col_classes <- vapply(df, class, character(1L))
  expect_true(all(col_classes == "character"))
})

test_that("read_socrata: ISO 8601 dates parse correctly via posixify", {
  skip_live()
  # NYC 311 — 'created_date' is a clean ISO 8601 timestamp field
  df <- read_socrata(
    "https://data.cityofnewyork.us/resource/erm2-nwe9",
    soql = "SELECT created_date",
    app_token = app_token,
    max_rows = 20L
  )
  expect_false(all(is.na(posixify(df$created_date))))
})

###############################################################################
# Group 5 — Live API: regression tests
###############################################################################

test_that("Regression #19: sparse datasets with missing fields across pages", {
  # rbindlist(fill=TRUE) must handle columns that appear only on some pages.
  skip_live()
  df <- read_socrata(
    "https://data.cityofchicago.org/resource/kn9c-c2s2",
    app_token = app_token
  )
  expect_gt(nrow(df), 0L)
  expect_true("community_area_name" %in% names(df))
})

###############################################################################
# Group 6 — Live API: authentication
###############################################################################

test_that("App token is accepted and does not 401/403", {
  skip_live()
  skip_if(!nzchar(app_token), "SOCRATA_TOKEN not set.")
  df <- read_socrata(DEMO_URL, app_token = app_token)
  expect_s3_class(df, "tbl_df")
})

test_that("Basic auth (API key + secret) is accepted", {
  skip_live()
  skip_if(
    !nzchar(socrata_user) || !nzchar(socrata_password),
    "SOCRATA_USER / SOCRATA_SECRET not set."
  )
  df <- read_socrata(
    DEMO_URL,
    socrata_user = socrata_user,
    password = socrata_password
  )
  expect_s3_class(df, "tbl_df")
})

test_that("Unauthenticated request to a private dataset throws an error", {
  skip_on_cran()
  # This dataset requires auth; should get HTTP 401/403
  expect_error(
    read_socrata("https://data.cityofchicago.org/resource/j8vp-2qpg"),
    regexp = NULL # any error is correct — we just want it to throw
  )
})

###############################################################################
# Group 7 — Live API: dataset discovery
###############################################################################

test_that("ls_socrata: returns a tibble with expected columns", {
  skip_live()
  df <- ls_socrata("soda.demo.socrata.com", limit = 10L)
  expect_s3_class(df, "tbl_df")
  expect_true(all(c("name", "id", "type", "updated", "url") %in% names(df)))
})

test_that("ls_socrata: 'updated' column is POSIXct", {
  skip_live()
  df <- ls_socrata("soda.demo.socrata.com", limit = 5L)
  expect_s3_class(df$updated, "POSIXct")
})

test_that("ls_socrata: search filter reduces results", {
  skip_live()
  df_all <- ls_socrata("data.cityofchicago.org", limit = 100L)
  df_search <- ls_socrata(
    "data.cityofchicago.org",
    limit = 100L,
    search = "permits"
  )
  expect_lte(nrow(df_search), nrow(df_all))
})

test_that("ls_socrata: unknown domain returns empty tibble with a message", {
  skip_on_cran()
  expect_message(
    result <- ls_socrata("this.domain.does.not.exist.socrata.com"),
    "No datasets found"
  )
  expect_equal(nrow(result), 0L)
})

test_that("ls_socrata: leading https:// in domain is stripped gracefully", {
  skip_live()
  df1 <- ls_socrata("soda.demo.socrata.com", limit = 5L)
  df2 <- ls_socrata("https://soda.demo.socrata.com", limit = 5L)
  expect_equal(nrow(df1), nrow(df2))
})

###############################################################################
# Group 8 — get_metadata() input validation (no network)
###############################################################################

test_that("get_metadata: empty url errors", {
  expect_error(get_metadata(""), "non-empty|4x4")
  expect_error(get_metadata("   "), "non-empty|4x4")
})

test_that("get_metadata: bare 4x4 without domain errors", {
  expect_error(get_metadata("4334-bgaj"), "domain")
})

test_that("get_metadata: invalid 4x4 in URL errors", {
  expect_error(
    get_metadata("https://soda.demo.socrata.com/resource/not-valid-here")
  )
})

###############################################################################
# Group 9 — coerce_socrata_types() unit tests (no network)
###############################################################################

test_that("coerce_socrata_types: rejects non-data-frame df", {
  fake_meta <- list(
    columns = data.frame(
      field_name = "x",
      data_type = "number",
      stringsAsFactors = FALSE
    )
  )
  expect_error(coerce_socrata_types("not a df", fake_meta), "data frame")
})

test_that("coerce_socrata_types: rejects meta without columns", {
  expect_error(
    coerce_socrata_types(data.frame(x = "1"), list(id = "abc")),
    "get_metadata"
  )
})

test_that("coerce_socrata_types: number columns become numeric", {
  df <- tibble::tibble(value = c("1.5", "2.0", "3.7"))
  meta <- list(
    columns = data.frame(
      field_name = "value",
      data_type = "number",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_type(result$value, "double")
  expect_equal(result$value, c(1.5, 2.0, 3.7))
})

test_that("coerce_socrata_types: calendar_date columns become POSIXct", {
  df <- tibble::tibble(opened = c("2024-01-15T10:30:00", "2024-06-01T00:00:00"))
  meta <- list(
    columns = data.frame(
      field_name = "opened",
      data_type = "calendar_date",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_s3_class(result$opened, "POSIXct")
})

test_that("coerce_socrata_types: checkbox columns become logical", {
  df <- tibble::tibble(active = c("true", "false", "true"))
  meta <- list(
    columns = data.frame(
      field_name = "active",
      data_type = "checkbox",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_type(result$active, "logical")
  expect_equal(result$active, c(TRUE, FALSE, TRUE))
})

test_that("coerce_socrata_types: text columns stay character", {
  df <- tibble::tibble(name = c("Alice", "Bob"))
  meta <- list(
    columns = data.frame(
      field_name = "name",
      data_type = "text",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_type(result$name, "character")
})

test_that("coerce_socrata_types: unknown type columns stay character", {
  df <- tibble::tibble(geo = c("POINT(1 2)", "POINT(3 4)"))
  meta <- list(
    columns = data.frame(
      field_name = "geo",
      data_type = "multipolygon",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_type(result$geo, "character")
})

test_that("coerce_socrata_types: columns not in metadata are untouched", {
  df <- tibble::tibble(a = c("1", "2"), b = c("x", "y"))
  meta <- list(
    columns = data.frame(
      field_name = "a",
      data_type = "number",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_type(result$a, "double")
  expect_type(result$b, "character") # 'b' not in meta, stays character
})

test_that("coerce_socrata_types: clean_names normalisation matches read_socrata output", {
  # read_socrata applies make_clean_names; metadata field names must be normalised
  # the same way. Test that "My Column" -> "my_column" matches correctly.
  df <- tibble::tibble(my_column = c("1.0", "2.0"))
  meta <- list(
    columns = data.frame(
      field_name = "My Column", # raw name as returned by API
      data_type = "number",
      stringsAsFactors = FALSE
    )
  )
  result <- coerce_socrata_types(df, meta)
  expect_type(result$my_column, "double")
})

###############################################################################
# Group 10 — read_socrata_parallel() input validation (no network)
###############################################################################

test_that("read_socrata_parallel: empty url errors", {
  expect_error(read_socrata_parallel(""), "non-empty")
  expect_error(read_socrata_parallel("   "), "non-empty")
})

test_that("read_socrata_parallel: invalid page_size errors", {
  expect_error(read_socrata_parallel(DEMO_URL, page_size = 0L), "page_size")
  expect_error(
    read_socrata_parallel(DEMO_URL, page_size = 100000L),
    "page_size"
  )
})

test_that("read_socrata_parallel: invalid max_active errors", {
  expect_error(read_socrata_parallel(DEMO_URL, max_active = 0L), "max_active")
  expect_error(read_socrata_parallel(DEMO_URL, max_active = 11L), "max_active")
})

test_that("read_socrata_parallel: bare 4x4 without domain errors", {
  expect_error(read_socrata_parallel("4334-bgaj"), "domain")
})

###############################################################################
# Group 11 — write_socrata_parallel() input validation (no network)
###############################################################################

test_that("write_socrata_parallel: non-data-frame errors", {
  expect_error(
    write_socrata_parallel(
      "not df",
      "example.gov",
      "abcd-1234",
      socrata_user = "x",
      password = "y"
    ),
    "data frame"
  )
})

test_that("write_socrata_parallel: invalid dataset_id errors", {
  expect_error(
    write_socrata_parallel(
      data.frame(x = 1),
      "example.gov",
      "bad!!",
      socrata_user = "x",
      password = "y"
    ),
    "4x4"
  )
})

test_that("write_socrata_parallel: invalid max_active errors", {
  expect_error(
    write_socrata_parallel(
      data.frame(x = 1),
      "example.gov",
      "abcd-1234",
      socrata_user = "x",
      password = "y",
      max_active = 0L
    ),
    "max_active"
  )
  expect_error(
    write_socrata_parallel(
      data.frame(x = 1),
      "example.gov",
      "abcd-1234",
      socrata_user = "x",
      password = "y",
      max_active = 11L
    ),
    "max_active"
  )
})

###############################################################################
# Group 12 — get_metadata() live tests
###############################################################################

test_that("get_metadata: returns a list with expected structure", {
  skip_live()
  meta <- get_metadata(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password
  )
  expect_type(meta, "list")
  expect_true(all(c("id", "name", "description", "columns") %in% names(meta)))
})

test_that("get_metadata: columns tibble has expected columns", {
  skip_live()
  meta <- get_metadata(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password
  )
  expect_s3_class(meta$columns, "tbl_df")
  expect_true(all(
    c("field_name", "display_name", "data_type") %in% names(meta$columns)
  ))
})

test_that("get_metadata: no system columns (starting with ':') in output", {
  skip_live()
  meta <- get_metadata(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password
  )
  expect_false(any(grepl("^:", meta$columns$field_name)))
})

test_that("get_metadata: field_names match column names returned by read_socrata", {
  skip_live()
  meta <- get_metadata(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password
  )
  df <- socrata_read(DEMO_URL, max_rows = 1L)

  # After clean_names, metadata field names should all appear in df
  clean_fields <- janitor::make_clean_names(meta$columns$field_name)
  expect_true(all(clean_fields %in% names(df)))
})

###############################################################################
# Group 13 — coerce_socrata_types() live tests
###############################################################################

test_that("coerce_socrata_types: round-trips correctly against a live dataset", {
  skip_live()
  meta <- get_metadata(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password
  )
  df <- socrata_read(DEMO_URL, max_rows = 50L)

  # All columns should be character before coercion
  expect_true(all(vapply(df, is.character, logical(1L))))

  df_typed <- coerce_socrata_types(df, meta)

  # At minimum, numeric columns should no longer be character
  number_cols <- janitor::make_clean_names(
    meta$columns$field_name[
      meta$columns$data_type %in% c("number", "money", "double")
    ]
  )
  number_cols <- intersect(number_cols, names(df_typed))
  if (length(number_cols) > 0L) {
    expect_true(all(vapply(df_typed[number_cols], is.numeric, logical(1L))))
  }
})

###############################################################################
# Group 14 — read_socrata_parallel() live tests
###############################################################################

test_that("read_socrata_parallel: returns a tibble", {
  skip_live()
  df <- read_socrata_parallel(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password,
    page_size = 500L
  )
  expect_s3_class(df, "tbl_df")
  expect_gt(nrow(df), 0L)
})

test_that("read_socrata_parallel: retrieves same row count as serial read", {
  skip_live()
  df_serial <- read_socrata(
    DEMO_URL,
    socrata_user = socrata_user,
    password = socrata_password,
  )
  df_parallel <- read_socrata_parallel(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password,
    page_size = 200L,
    max_active = 3L
  )
  expect_equal(nrow(df_parallel), nrow(df_serial))
})

test_that("read_socrata_parallel: column names match serial read", {
  skip_live()
  df_serial <- read_socrata(
    DEMO_URL,
    socrata_user = socrata_user,
    password = socrata_password,
    max_rows = 1L
  )
  df_parallel <- read_socrata_parallel(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password,
    max_rows = 1L
  )
  expect_equal(sort(names(df_parallel)), sort(names(df_serial)))
})

test_that("read_socrata_parallel: max_rows caps the result", {
  skip_live()
  df <- read_socrata_parallel(
    DEMO_URL,
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password,
    max_rows = 100L,
    page_size = 50L
  )
  expect_lte(nrow(df), 100L)
  expect_gt(nrow(df), 0L)
})

test_that("read_socrata_parallel: soql filter is applied correctly", {
  skip_live()
  df_all <- read_socrata(
    DEMO_URL,
    socrata_user = socrata_user,
    password = socrata_password,
  )
  target <- unique(df_all$region)[[1L]]

  df_filtered <- read_socrata_parallel(
    DEMO_URL,
    soql = sprintf("SELECT * WHERE region = '%s'", target),
    app_token = app_token,
    socrata_user = socrata_user,
    password = socrata_password
  )

  expect_true(all(df_filtered$region == target))
  expect_lt(nrow(df_filtered), nrow(df_all))
  expect_equal(df_filtered, df_all %>% dplyr::filter(region == target))
})

###############################################################################
# Group 9 — write_socrata / write_socrata_parallel live round-trip
#
# Uses the public writable Socrata demo datasets:
#   xh6g-yugi — small dataset, used for UPSERT tests
#   kc76-ybeq — small dataset, used for REPLACE tests
#
# Each test writes known data, reads it back, and verifies the values match.
# Requires valid credentials (SOCRATA_USER + SOCRATA_SECRET).
###############################################################################

WRITE_UPSERT_URL  <- "https://soda.demo.socrata.com/resource/xh6g-yugi"
WRITE_REPLACE_URL <- "https://soda.demo.socrata.com/resource/kc76-ybeq"
WRITE_ID_UPSERT   <- "xh6g-yugi"
WRITE_ID_REPLACE  <- "kc76-ybeq"
WRITE_DOMAIN      <- "soda.demo.socrata.com"

skip_write <- function() {
  skip_on_cran()
  skip_if(
    !nzchar(socrata_user) || !nzchar(socrata_password),
    "Write tests require SOCRATA_USER + SOCRATA_SECRET."
  )
}

# ── write_socrata ─────────────────────────────────────────────────────────────

test_that("write_socrata: UPSERT returns 200 and summary data frame", {
  skip_write()
  df  <- data.frame(x = sample(-1000:1000, 1L), y = sample(-1000:1000, 1L))
  res <- write_socrata(
    dataframe    = df,
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_UPSERT,
    update_mode  = "UPSERT",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token
  )
  expect_type(res, "list")
  expect_length(res, 1L)  # one chunk
})

test_that("write_socrata: UPSERT round-trip — values read back correctly", {
  skip_write()
  x <- sample(-1000:1000, 1L)
  y <- sample(-1000:1000, 1L)
  df <- data.frame(x = x, y = y)

  write_socrata(
    dataframe    = df,
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_UPSERT,
    update_mode  = "UPSERT",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token
  )

  Sys.sleep(2L)  # allow Socrata to index the write before reading back

  result <- socrata_read(
    WRITE_UPSERT_URL,
    soql = sprintf("SELECT x, y WHERE x = '%d' AND y = '%d'", x, y)
  )

  expect_gt(nrow(result), 0L)
  expect_equal(as.integer(result$x[[1L]]), x)
  expect_equal(as.integer(result$y[[1L]]), y)
})

test_that("write_socrata: REPLACE overwrites dataset with new rows", {
  skip_write()
  x <- sample(-1000:1000, 5L)
  y <- sample(-1000:1000, 5L)
  df <- data.frame(x = x, y = y)

  write_socrata(
    dataframe    = df,
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_REPLACE,
    update_mode  = "REPLACE",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token
  )

  Sys.sleep(2L)

  result <- socrata_read(WRITE_REPLACE_URL)

  # REPLACE should leave exactly the rows we sent
  expect_equal(nrow(result), nrow(df))
})

test_that("write_socrata: chunked UPSERT sends all rows", {
  skip_write()
  n  <- 25L
  df <- data.frame(x = sample(-1000:1000, n), y = sample(-1000:1000, n))

  res <- write_socrata(
    dataframe    = df,
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_UPSERT,
    update_mode  = "UPSERT",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token,
    chunk_size   = 10L   # forces 3 chunks
  )

  # Three chunks sent, three response objects returned
  expect_length(res, 3L)
})

# ── write_socrata_parallel ────────────────────────────────────────────────────

test_that("write_socrata_parallel: UPSERT returns correct structure", {
  skip_write()
  df  <- data.frame(x = sample(-1000:1000, 1L), y = sample(-1000:1000, 1L))
  res <- write_socrata_parallel(
    dataframe    = df,
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_UPSERT,
    update_mode  = "UPSERT",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token,
    chunk_size   = 10L,
    max_active   = 3L
  )
  expect_named(res, c("responses", "failures", "summary"))
  expect_length(res$failures, 0L)
  expect_s3_class(res$summary, "data.frame")
})

test_that("write_socrata_parallel: round-trip — values read back correctly", {
  skip_write()
  x <- sample(-1000:1000, 1L)
  y <- sample(-1000:1000, 1L)

  write_socrata_parallel(
    dataframe    = data.frame(x = x, y = y),
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_UPSERT,
    update_mode  = "UPSERT",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token
  )

  Sys.sleep(2L)

  result <- socrata_read(
    WRITE_UPSERT_URL,
    soql = sprintf("SELECT x, y WHERE x = '%d' AND y = '%d'", x, y)
  )

  expect_gt(nrow(result), 0L)
  expect_equal(as.integer(result$x[[1L]]), x)
  expect_equal(as.integer(result$y[[1L]]), y)
})

test_that("write_socrata_parallel: multi-chunk UPSERT has no failures", {
  skip_write()
  n  <- 25L
  df <- data.frame(x = sample(-1000:1000, n), y = sample(-1000:1000, n))

  res <- write_socrata_parallel(
    dataframe    = df,
    domain       = WRITE_DOMAIN,
    dataset_id   = WRITE_ID_UPSERT,
    update_mode  = "UPSERT",
    socrata_user = socrata_user,
    password     = socrata_password,
    app_token    = app_token,
    chunk_size   = 10L,
    max_active   = 3L
  )

  expect_length(res$failures, 0L)
  expect_equal(length(res$responses), ceiling(n / 10L))
  expect_equal(res$summary$Rows_Created + res$summary$Rows_Updated, n)
})

test_that("write_socrata_parallel: REPLACE falls back and completes", {
  skip_write()
  df <- data.frame(x = sample(-1000:1000, 5L), y = sample(-1000:1000, 5L))

  expect_message(
    write_socrata_parallel(
      dataframe    = df,
      domain       = WRITE_DOMAIN,
      dataset_id   = WRITE_ID_REPLACE,
      update_mode  = "REPLACE",
      socrata_user = socrata_user,
      password     = socrata_password,
      app_token    = app_token
    ),
    "falling back to write_socrata"
  )

  Sys.sleep(2L)
  result <- socrata_read(WRITE_REPLACE_URL)
  expect_equal(nrow(result), nrow(df))
})
