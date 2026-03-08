###############################################################################
# socratr: Interface to Socrata Datasets
#
# Read  — SODA 3  POST /api/v3/views/{id}/query.json
#           Page-number pagination (the only mechanism SODA 3 exposes).
#           Internally managed so callers never touch page numbers.
#
# Write — SODA 2  POST|PUT /resource/{id}.json
#           Chunked upsert / replace with aggregate summary.
#
# List  — Socrata Discovery API v1
#           https://api.us.socrata.com/api/catalog/v1
###############################################################################

# ── Internal helpers ──────────────────────────────────────────────────────────

#' Null-coalescing operator
#' @noRd
`%||%` <- function(a, b) if (!is.null(a)) a else b

#' Standard User-Agent string
#' @noRd
socratr_ua <- function() {
  paste0(
    "RSocrata/2.0.0 (",
    Sys.info()[["sysname"]],
    "; R/",
    paste0(R.version$major, ".", R.version$minor),
    ")"
  )
}

#' Extract a 4x4 dataset ID from a URL path or bare string.
#' Returns NA_character_ silently if nothing matches.
#' @noRd
extract_four_by_four <- function(x) {
  m <- regmatches(x, regexpr("[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}", x))
  if (length(m) == 0L) NA_character_ else m[[1L]]
}

#' Resolve a full URL or bare 4x4+domain pair to a (hostname, four_by_four) list.
#' @noRd
resolve_dataset <- function(url, domain = NULL) {
  four_by_four <- extract_four_by_four(url)
  if (is.na(four_by_four)) {
    stop("Could not find a valid 4x4 dataset ID in: ", url, call. = FALSE)
  }

  if (grepl("^https?://", url)) {
    hostname <- httr2::url_parse(url)$hostname
  } else {
    if (is.null(domain)) {
      stop(
        "A `domain` argument is required when `url` is a bare 4x4 ID.",
        call. = FALSE
      )
    }
    hostname <- gsub("^https?://|/.*$", "", domain)
  }

  list(hostname = hostname, four_by_four = four_by_four)
}

# ── Public utilities ──────────────────────────────────────────────────────────

#' Test whether a string is a valid Socrata 4x4 dataset identifier
#'
#' @param x Character vector to test.
#' @return Logical vector the same length as `x`.
#' @examples
#' is_four_by_four("abcd-1234")   # TRUE
#' is_four_by_four("not-valid!")  # FALSE
#' @export
is_four_by_four <- function(x) {
  grepl("^[[:alnum:]]{4}-[[:alnum:]]{4}$", as.character(x))
}

#' Parse date/time strings returned by the Socrata API
#'
#' Handles ISO 8601 (SODA 3) and legacy `mm/dd/yyyy` (SODA 2) formats.
#' Warns if any non-missing value fails to parse.
#'
#' @param x Character vector of date/time strings.
#' @return A `POSIXct` vector in the system timezone.
#' @examples
#' posixify("2024-06-15T13:45:00.000")
#' posixify("06/15/2024 01:45:00 PM")
#' @export
posixify <- function(x) {
  if (is.null(x) || length(x) == 0L) {
    return(as.POSIXct(character(0L)))
  }

  x <- as.character(x)
  dt <- lubridate::parse_date_time(
    x,
    orders = c("ymd HMS", "ymd HM", "ymd", "mdy HMS p", "mdy HM p", "mdy"),
    truncated = 3L,
    quiet = TRUE
  )

  n_bad <- sum(is.na(dt) & !is.na(x))
  if (n_bad > 0L) {
    warning(
      n_bad,
      " value(s) could not be parsed as dates and were set to NA.",
      call. = FALSE
    )
  }

  as.POSIXct(dt)
}

# ── Read ──────────────────────────────────────────────────────────────────────

#' Read a Socrata dataset via SODA 3
#'
#' Fetches data from a Socrata portal using the SODA 3 POST query API.
#' Pagination is handled automatically; callers never manage page numbers.
#'
#' @section Pagination:
#' SODA 3 exposes only page-number pagination — there is no server-side cursor.
#' This function manages page numbers internally and stops as soon as a page
#' returns fewer rows than `page_size`, which is the API's signal that all
#' data has been retrieved. Socrata's own docs note that performance degrades
#' at very high page numbers, so use `soql` filters to reduce result sets when
#' fetching from large datasets.
#'
#' @param url         Character. A full Socrata dataset URL **or** a bare 4x4
#'   ID (e.g. `"abcd-1234"`). If a bare ID, `domain` is also required.
#' @param domain      Character. Required only when `url` is a bare 4x4 ID.
#'   Leading `https://` and trailing paths are stripped automatically.
#' @param soql        Character. A plain SoQL query string (default
#'   `"SELECT *"`). Do **not** include `LIMIT` or `OFFSET`; pagination is
#'   handled internally. Example:
#'   `"SELECT name, value WHERE value > 100 ORDER BY value DESC"`.
#' @param app_token   Character (optional). Socrata Application Token, sent as
#'   the `X-App-Token` header. Strongly recommended — raises the anonymous
#'   rate limit significantly.
#' @param socrata_user Character (optional). Socrata account email or API Key
#'   ID. Required for private datasets.
#' @param password    Character (optional). Socrata password or API Secret Key.
#' @param page_size   Integer. Rows fetched per request (default 2 000,
#'   max 50 000). Reduce if you see timeouts on wide or slow datasets.
#' @param max_rows    Integer or `Inf` (default). Hard cap on total rows
#'   returned. Useful for sampling or testing without pulling a full dataset.
#' @param verbose     Logical. If `TRUE`, prints a one-line progress message
#'   per page fetched (default `FALSE`).
#'
#' @return A [tibble::tibble()] with all columns as character strings.
#'   Convert dates with [posixify()] and numerics with [as.numeric()] as
#'   needed — Socrata returns all values as text in JSON.
#'
#' @examples
#' \dontrun{
#' # Full URL
#' df <- read_socrata("https://data.somervillema.gov/resource/abcd-1234")
#'
#' # Bare 4x4 + domain
#' df <- read_socrata("abcd-1234", domain = "data.somervillema.gov")
#'
#' # With a SoQL filter and app token
#' df <- read_socrata(
#'   url       = "https://data.somervillema.gov/resource/abcd-1234",
#'   soql      = "SELECT name, opened_date WHERE status = 'Open'",
#'   app_token = Sys.getenv("SOCRATA_APP_TOKEN")
#' )
#' }
#'
#' @importFrom httr2 request req_body_json req_user_agent req_headers
#'   req_auth_basic req_retry req_perform resp_body_raw
#' @importFrom yyjsonr read_json_raw
#' @importFrom data.table as.data.table rbindlist
#' @importFrom janitor make_clean_names
#' @importFrom tibble as_tibble
#' @export
read_socrata <- function(
  url,
  domain = NULL,
  soql = "SELECT *",
  app_token = NULL,
  socrata_user = NULL,
  password = NULL,
  page_size = 5000L,
  max_rows = Inf,
  verbose = FALSE
) {
  return(read_socrata_parallel(
    url = url,
    domain = domain,
    soql = soql,
    app_token = app_token,
    socrata_user = socrata_user,
    password = password,
    page_size = page_size,
    max_rows = max_rows,
    max_active = 1, ## sequential is just having one active thread
    verbose = verbose
  ))

  # # ── Validate ─────
  # if (missing(url) || !nzchar(trimws(url))) {
  #   stop("`url` must be a non-empty string.", call. = FALSE)
  # }
  # # page_size <- as.integer(page_size)
  # # if (is.na(page_size) || page_size < 1L || page_size > 50000L) {
  # #   stop("`page_size` must be between 1 and 50 000.", call. = FALSE)
  # # }

  # ds <- resolve_dataset(url, domain)
  # endpoint <- paste0(
  #   "https://",
  #   ds$hostname,
  #   "/api/v3/views/",
  #   ds$four_by_four,
  #   "/query.json"
  # )

  # # ── Build base request ─────────────────────────────────────────────────────
  # # Auth headers are set once and reused for every page request.
  # req_base <- httr2::request(endpoint) |>
  #   httr2::req_user_agent(socratr_ua()) |>
  #   httr2::req_headers(
  #     "Accept" = "application/json",
  #     "Content-Type" = "application/json"
  #   ) |>
  #   httr2::req_retry(max_tries = 3L, backoff = ~ 2^.x)

  # if (!is.null(app_token)) {
  #   req_base <- req_base |> httr2::req_headers("X-App-Token" = app_token)
  # }
  # if (!is.null(socrata_user) && !is.null(password)) {
  #   req_base <- req_base |> httr2::req_auth_basic(socrata_user, password)
  # }

  # # ── Paginate ───────────────────────────────────────────────────────────────
  # # SODA 3 has no server-side cursor. Page numbers are managed here so callers
  # # never see them. We stop when a page returns fewer rows than page_size,
  # # which is the documented signal that there is no more data.

  # all_pages <- vector("list", 64L) # pre-allocated; trimmed before rbindlist
  # page_num <- 1L # page number sent to the API
  # page_index <- 0L # insertion index into all_pages
  # total_rows <- 0L

  # repeat {
  #   rows_remaining <- max_rows - total_rows
  #   if (rows_remaining <= 0L) {
  #     break
  #   }

  #   # Guard: min(integer, Inf) returns a double in R. Serialising a double Inf
  #   # to JSON produces null, which causes SODA 3 to use its own default page
  #   # size (often smaller than requested), making the short-page stop condition
  #   # fire too early and truncating the result.
  #   this_page_size <- if (is.finite(rows_remaining)) {
  #     as.integer(min(page_size, rows_remaining))
  #   } else {
  #     page_size # already a validated integer
  #   }

  #   if (verbose) {
  #     message(sprintf(
  #       "Fetching page %d (up to %d rows) ...",
  #       page_num,
  #       this_page_size
  #     ))
  #   }

  #   body <- list(
  #     query = soql,
  #     page = list(pageNumber = page_num, pageSize = this_page_size),
  #     includeSynthetic = FALSE
  #   )

  #   resp <- tryCatch(
  #     httr2::req_perform(req_base |> httr2::req_body_json(body)),
  #     error = function(e) {
  #       api_msg <- if (!is.null(e$resp)) {
  #         err <- tryCatch(
  #           yyjsonr::read_json_raw(httr2::resp_body_raw(e$resp)),
  #           error = function(...) list()
  #         )
  #         err$message %||% paste("HTTP", httr2::resp_status(e$resp))
  #       } else {
  #         conditionMessage(e)
  #       }
  #       stop(
  #         sprintf("Request failed on page %d: %s", page_num, api_msg),
  #         call. = FALSE
  #       )
  #     }
  #   )

  #   parsed <- tryCatch(
  #     yyjsonr::read_json_raw(httr2::resp_body_raw(resp)),
  #     error = function(e) {
  #       stop(
  #         "Failed to parse JSON on page ",
  #         page_num,
  #         ": ",
  #         conditionMessage(e),
  #         call. = FALSE
  #       )
  #     }
  #   )

  #   n_rows <- nrow(parsed)
  #   if (n_rows == 0L) {
  #     break
  #   }

  #   batch_dt <- data.table::as.data.table(parsed)

  #   # Convert only list-typed columns (nested JSON) to character.
  #   # Leave atomic columns alone so the caller can coerce types as needed.
  #   list_cols <- names(batch_dt)[vapply(batch_dt, is.list, logical(1L))]
  #   if (length(list_cols) > 0L) {
  #     batch_dt[, (list_cols) := lapply(.SD, as.character), .SDcols = list_cols]
  #   }

  #   page_index <- page_index + 1L
  #   all_pages[[page_index]] <- batch_dt
  #   total_rows <- total_rows + n_rows

  #   # Short page = last page; stop without making a wasted empty request
  #   if (n_rows < this_page_size) {
  #     break
  #   }

  #   page_num <- page_num + 1L
  # }

  # if (total_rows == 0L) {
  #   return(tibble::tibble())
  # }

  # final_dt <- data.table::rbindlist(
  #   all_pages[seq_len(page_index)],
  #   fill = TRUE,
  #   use.names = TRUE
  # )
  # names(final_dt) <- janitor::make_clean_names(names(final_dt))

  # tibble::as_tibble(final_dt)
}

# ── Write ─────────────────────────────────────────────────────────────────────

#' Upload data to a Socrata dataset via SODA 2
#'
#' Serializes a data frame to JSON and sends it to the Socrata SODA 2 API.
#' Large uploads are automatically split into chunks.
#'
#' @section SODA version note:
#' Writing uses SODA 2 (`/resource/{id}.json`) because SODA 3 does not yet
#' expose a write endpoint. SODA 2 write support is stable and is the approach
#' recommended by Socrata / Tyler Technologies.
#'
#' @param dataframe   A data frame or tibble to upload.
#' @param domain      Character. The Socrata domain
#'   (e.g. `"data.somervillema.gov"`).
#' @param dataset_id  Character. The 4x4 dataset identifier.
#' @param update_mode Character. `"UPSERT"` (HTTP POST — add or update rows by
#'   primary key) or `"REPLACE"` (HTTP PUT — overwrite the full dataset).
#' @param socrata_user Character. Socrata account email or API Key ID.
#' @param password    Character. Socrata password or API Secret Key.
#' @param app_token   Character (optional). Socrata Application Token.
#' @param chunk_size  Integer. Rows per request for `UPSERT` (default 10 000).
#'   `REPLACE` always sends the full data frame in a single request.
#'
#' @return Invisibly returns a list of `httr2` response objects (one per
#'   chunk). Prints an aggregate upload summary as a side effect.
#'
#' @examples
#' \dontrun{
#' write_socrata(
#'   dataframe    = my_df,
#'   domain       = "data.somervillema.gov",
#'   dataset_id   = "abcd-1234",
#'   update_mode  = "UPSERT",
#'   socrata_user = Sys.getenv("SOCRATA_USER"),
#'   password     = Sys.getenv("SOCRATA_KEY")
#' )
#' }
#'
#' @importFrom httr2 request req_user_agent req_auth_basic req_body_raw
#'   req_method req_headers req_retry req_perform resp_body_raw resp_status
#' @importFrom yyjsonr write_json_raw read_json_raw
#' @export
write_socrata <- function(
  dataframe,
  domain,
  dataset_id,
  update_mode = c("UPSERT", "REPLACE"),
  socrata_user,
  password,
  app_token = NULL,
  chunk_size = 10000L
) {
  return(write_socrata_parallel(
    dataframe = dataframe,
    domain = domain,
    dataset_id = dataset_id,
    update_mode = update_mode,
    socrata_user = socrata_user,
    password = password,
    app_token = app_token,
    chunk_size = chunk_size,
    max_active = 1 ## sequential write is same as parallel write with 1 thread
  ))
  # update_mode <- match.arg(update_mode)
  # chunk_size <- as.integer(chunk_size)

  # if (!is.data.frame(dataframe)) {
  #   stop("`dataframe` must be a data frame.", call. = FALSE)
  # }
  # if (!nzchar(trimws(domain))) {
  #   stop("`domain` must be a non-empty string.", call. = FALSE)
  # }
  # if (!is_four_by_four(dataset_id)) {
  #   stop("`dataset_id` must be a valid 4x4 identifier.", call. = FALSE)
  # }
  # if (is.na(chunk_size) || chunk_size < 1L) {
  #   stop("`chunk_size` must be a positive integer.", call. = FALSE)
  # }

  # hostname <- gsub("^https?://|/.*$", "", domain)
  # base_url <- paste0("https://", hostname, "/resource/", dataset_id, ".json")

  # # ── Base request ───────────────────────────────────────────────────────────
  # req_base <- httr2::request(base_url) |>
  #   httr2::req_user_agent(socratr_ua()) |>
  #   httr2::req_auth_basic(socrata_user, password) |>
  #   httr2::req_headers("Accept" = "application/json") |>
  #   httr2::req_retry(max_tries = 3L, backoff = ~ 2^.x) |>
  #   httr2::req_method(if (update_mode == "UPSERT") "POST" else "PUT")

  # if (!is.null(app_token)) {
  #   req_base <- req_base |> httr2::req_headers("X-App-Token" = app_token)
  # }

  # # ── Chunk ──────────────────────────────────────────────────────────────────
  # # REPLACE must be atomic (one PUT covering the whole dataset).
  # # UPSERT can be safely chunked because it keys on the primary key.
  # n_rows <- nrow(dataframe)
  # chunks <- if (update_mode == "REPLACE") {
  #   list(dataframe)
  # } else {
  #   starts <- seq(1L, n_rows, by = chunk_size)
  #   lapply(starts, function(i) {
  #     dataframe[i:min(i + chunk_size - 1L, n_rows), , drop = FALSE]
  #   })
  # }

  # n_chunks <- length(chunks)
  # message(sprintf(
  #   "Starting %s of %d row(s) in %d chunk(s) to dataset %s ...",
  #   update_mode,
  #   n_rows,
  #   n_chunks,
  #   dataset_id
  # ))

  # # ── Send ───────────────────────────────────────────────────────────────────
  # responses <- vector("list", n_chunks)
  # totals <- list(
  #   Rows_Created = 0L,
  #   Rows_Updated = 0L,
  #   Rows_Deleted = 0L,
  #   Errors = 0L
  # )

  # for (i in seq_len(n_chunks)) {
  #   raw_payload <- yyjsonr::write_json_raw(chunks[[i]])

  #   responses[[i]] <- tryCatch(
  #     httr2::req_perform(
  #       req_base |> httr2::req_body_raw(raw_payload, "application/json")
  #     ),
  #     error = function(e) {
  #       api_msg <- if (!is.null(e$resp)) {
  #         body <- tryCatch(
  #           yyjsonr::read_json_raw(httr2::resp_body_raw(e$resp)),
  #           error = function(...) list()
  #         )
  #         body$message %||% paste("HTTP", httr2::resp_status(e$resp))
  #       } else {
  #         conditionMessage(e)
  #       }
  #       stop(
  #         sprintf("Chunk %d/%d failed: %s", i, n_chunks, api_msg),
  #         call. = FALSE
  #       )
  #     }
  #   )

  #   summ <- tryCatch(
  #     yyjsonr::read_json_raw(httr2::resp_body_raw(responses[[i]])),
  #     error = function(...) list()
  #   )
  #   totals$Rows_Created <- totals$Rows_Created + (summ$Rows_Created %||% 0L)
  #   totals$Rows_Updated <- totals$Rows_Updated + (summ$Rows_Updated %||% 0L)
  #   totals$Rows_Deleted <- totals$Rows_Deleted + (summ$Rows_Deleted %||% 0L)
  #   totals$Errors <- totals$Errors + (summ$Errors %||% 0L)

  #   message(sprintf("  Chunk %d/%d sent.", i, n_chunks))
  # }

  # cat("\n--- Upload Summary ---\n")
  # print(as.data.frame(totals))

  # invisible(responses)
}

# ── List ──────────────────────────────────────────────────────────────────────

#' List datasets available on a Socrata domain
#'
#' Queries the Socrata Discovery API (catalog v1) to enumerate datasets on a
#' given domain.
#'
#' @param domain  Character. The Socrata domain
#'   (e.g. `"data.somervillema.gov"`). Leading `https://` is stripped.
#' @param limit   Integer. Maximum datasets to return (default 100, max 10 000).
#' @param search  Character (optional). Keyword filter applied server-side.
#' @param types   Character vector. Resource types to include (default
#'   `"dataset"`). Other values: `"map"`, `"chart"`, `"file"`.
#'
#' @return A [tibble::tibble()] with columns `name`, `id`, `type`,
#'   `updated` ([POSIXct]), `description`, and `url`.
#'
#' @examples
#' \dontrun{
#' ls_socrata("data.somervillema.gov")
#' ls_socrata("data.somervillema.gov", search = "permits", limit = 20)
#' }
#'
#' @importFrom httr2 request req_url_query req_user_agent req_perform
#'   resp_body_json
#' @importFrom data.table rbindlist
#' @importFrom tibble as_tibble
#' @export
ls_socrata <- function(
  domain,
  limit = 100L,
  search = NULL,
  types = "dataset"
) {
  hostname <- gsub("^https?://|/.*$", "", domain)
  limit <- as.integer(limit)
  if (is.na(limit) || limit < 1L) {
    stop("`limit` must be a positive integer.", call. = FALSE)
  }

  req <- httr2::request("https://api.us.socrata.com/api/catalog/v1") |>
    httr2::req_user_agent(socratr_ua()) |>
    httr2::req_url_query(
      domains = hostname,
      limit = limit,
      q = search, # correct param: 'q', not 'search'
      only = paste(types, collapse = ",")
    )

  resp <- tryCatch(
    httr2::req_perform(req),
    httr2_http_404 = function(e) NULL, # unknown domain returns 404, not empty results
    error = function(e) {
      stop("Discovery API request failed: ", conditionMessage(e), call. = FALSE)
    }
  )

  content <- if (is.null(resp)) {
    list(results = list())
  } else {
    httr2::resp_body_json(resp, simplifyVector = FALSE)
  }

  if (length(content$results) == 0L) {
    message("No datasets found for domain: ", hostname)
    return(tibble::tibble())
  }

  resources <- lapply(content$results, function(x) {
    res <- x$resource
    list(
      name = res$name %||% NA_character_,
      id = res$id %||% NA_character_,
      type = res$type %||% NA_character_,
      updated = res$updatedAt %||% NA_character_,
      description = res$description %||% NA_character_,
      url = x$link %||% NA_character_ # x$link not res$permalink
    )
  })

  df <- data.table::rbindlist(resources, fill = TRUE)
  df[, updated := posixify(updated)]

  tibble::as_tibble(df)
}

# ── Metadata ──────────────────────────────────────────────────────────────────

#' Fetch schema and metadata for a Socrata dataset
#'
#' Retrieves the full view metadata from `GET /api/views/{4x4}.json`, including
#' column names, API field names, data types, and dataset-level attributes such
#' as description, row count, and last-modified timestamp.
#'
#' This is useful for inspecting a dataset's schema before reading it, building
#' dynamic SoQL `SELECT` clauses, or passing column type information to
#' [coerce_socrata_types()].
#'
#' @param url         Character. A full Socrata dataset URL or a bare 4x4 ID.
#' @param domain      Character. Required only when `url` is a bare 4x4 ID.
#' @param app_token   Character (optional). Socrata Application Token.
#' @param socrata_user Character (optional). Socrata account email or API Key ID.
#' @param password    Character (optional). Socrata password or API Secret Key.
#'
#' @return A named list with elements:
#'   \describe{
#'     \item{`id`}{The 4x4 dataset identifier.}
#'     \item{`name`}{Dataset display name.}
#'     \item{`description`}{Dataset description.}
#'     \item{`row_count`}{Approximate number of rows as reported by the API.}
#'     \item{`updated`}{Last-modified timestamp as [POSIXct].}
#'     \item{`columns`}{A [tibble::tibble()] with one row per column, containing
#'       `field_name` (the API/SoQL name), `display_name`, `data_type`, and
#'       `description`.}
#'   }
#'
#' @examples
#' \dontrun{
#' meta <- get_metadata("https://data.somervillema.gov/resource/abcd-1234")
#' meta$columns
#'
#' # Use field names to build a SELECT clause
#' fields <- paste(meta$columns$field_name, collapse = ", ")
#' df <- read_socrata(
#'   "https://data.somervillema.gov/resource/abcd-1234",
#'   soql = paste("SELECT", fields)
#' )
#' }
#'
#' @importFrom httr2 request req_user_agent req_headers req_auth_basic
#'   req_retry req_perform resp_body_json
#' @importFrom tibble tibble
#' @export
get_metadata <- function(
  url,
  domain = NULL,
  app_token = NULL,
  socrata_user = NULL,
  password = NULL
) {
  ds <- resolve_dataset(url, domain)
  endpoint <- paste0(
    "https://",
    ds$hostname,
    "/api/views/",
    ds$four_by_four,
    ".json"
  )

  req <- httr2::request(endpoint) |>
    httr2::req_user_agent(socratr_ua()) |>
    httr2::req_headers("Accept" = "application/json") |>
    httr2::req_retry(max_tries = 3L, backoff = ~ 2^.x)

  if (!is.null(app_token)) {
    req <- req |> httr2::req_headers("X-App-Token" = app_token)
  }
  if (!is.null(socrata_user) && !is.null(password)) {
    req <- req |> httr2::req_auth_basic(socrata_user, password)
  }

  resp <- tryCatch(
    httr2::req_perform(req),
    error = function(e) {
      stop("Metadata request failed: ", conditionMessage(e), call. = FALSE)
    }
  )

  raw <- httr2::resp_body_json(resp, simplifyVector = FALSE)

  # ── Parse columns ──────────────────────────────────────────────────────────
  cols_raw <- raw$columns %||% list()
  columns_tbl <- tibble::tibble(
    field_name = vapply(
      cols_raw,
      function(c) c$fieldName %||% NA_character_,
      character(1L)
    ),
    display_name = vapply(
      cols_raw,
      function(c) c$name %||% NA_character_,
      character(1L)
    ),
    data_type = vapply(
      cols_raw,
      function(c) c$dataTypeName %||% NA_character_,
      character(1L)
    ),
    description = vapply(
      cols_raw,
      function(c) c$description %||% NA_character_,
      character(1L)
    )
  )

  # Filter out Socrata system columns (field names starting with ":")
  columns_tbl <- columns_tbl[!grepl("^:", columns_tbl$field_name), ]

  list(
    id = raw$id %||% ds$four_by_four,
    name = raw$name %||% NA_character_,
    description = raw$description %||% NA_character_,
    row_count = {
      approx <- as.integer(raw$approxRowCount %||% NA_integer_)
      if (is.na(approx)) {
        counts <- vapply(
          raw$columns %||% list(),
          function(col) {
            as.integer(col$cachedContents$count %||% NA_character_)
          },
          integer(1L)
        )
        counts <- counts[!is.na(counts)]
        if (length(counts) > 0L) max(counts) else NA_integer_
      } else {
        approx
      }
    },
    ,
    updated = posixify(as.character(raw$rowsUpdatedAt %||% NA_character_)),
    columns = columns_tbl
  )
}

# ── Type coercion ─────────────────────────────────────────────────────────────

# Internal mapping from Socrata dataTypeName to an R coercion function.
# All values in a freshly-read tibble are character; we coerce in place.
# @noRd
.socrata_type_map <- list(
  # Numeric types
  "number" = as.numeric,
  "double" = as.numeric,
  "money" = as.numeric,
  "percent" = as.numeric,
  # Boolean
  "checkbox" = function(x) x == "true" | x == "TRUE" | x == "1",
  # Date / time
  "calendar_date" = posixify,
  "date" = posixify,
  "fixed_timestamp" = posixify,
  # Everything else (text, url, location, geo, etc.) stays character
  "text" = identity,
  "url" = identity,
  "html" = identity,
  "email" = identity,
  "phone" = identity
)

#' Coerce columns in a Socrata tibble to their native R types
#'
#' After [read_socrata()] returns a tibble with all-character columns, this
#' function applies type conversions based on the schema returned by
#' [get_metadata()]. Numeric columns become `numeric`, date columns become
#' `POSIXct`, and checkbox columns become `logical`. Everything else stays
#' `character`.
#'
#' @param df      A tibble returned by [read_socrata()].
#' @param meta    The list returned by [get_metadata()] for the same dataset.
#'   Must contain a `columns` tibble with `field_name` and `data_type` columns.
#'
#' @return The same tibble with columns coerced to appropriate types.
#'
#' @details
#' Column matching is done on `field_name` after applying
#' [janitor::make_clean_names()] to both sides, which mirrors the name
#' transformation applied by [read_socrata()]. Columns in `df` that have no
#' corresponding entry in `meta$columns` are left as-is. Columns with
#' unrecognised `data_type` values are also left as character.
#'
#' @examples
#' \dontrun{
#' meta <- get_metadata("https://data.somervillema.gov/resource/abcd-1234")
#' df   <- read_socrata("https://data.somervillema.gov/resource/abcd-1234")
#' df   <- coerce_socrata_types(df, meta)
#' }
#'
#' @importFrom janitor make_clean_names
#' @export
coerce_socrata_types <- function(df, meta) {
  if (!is.data.frame(df)) {
    stop("`df` must be a data frame.", call. = FALSE)
  }
  if (!is.list(meta) || !is.data.frame(meta$columns)) {
    stop("`meta` must be the list returned by get_metadata().", call. = FALSE)
  }

  # Normalise field names the same way read_socrata() does
  clean_fields <- janitor::make_clean_names(meta$columns$field_name)
  type_map <- setNames(as.list(meta$columns$data_type), clean_fields)

  for (col in names(df)) {
    # Use [[ with a default via %||%: missing keys return NULL from a list,
    # which is safe to check with is.null — avoids subscriptOutOfBounds that
    # [[ throws on named *vectors* when a key is absent.
    dtype <- type_map[[col]] %||% NULL
    if (is.null(dtype)) {
      next
    } # column not in metadata — leave untouched

    fn <- .socrata_type_map[[dtype]]
    if (is.null(fn)) {
      next
    } # unknown Socrata type — leave as char
    if (identical(fn, identity)) {
      next
    } # text — nothing to do

    df[[col]] <- tryCatch(
      fn(df[[col]]),
      error = function(e) {
        warning(
          "Could not coerce column '",
          col,
          "' (",
          dtype,
          "): ",
          conditionMessage(e),
          call. = FALSE
        )
        df[[col]] # return original on failure
      }
    )
  }

  return(df)
}

# ── Parallel read ─────────────────────────────────────────────────────────────

#' @noRd
.read_socrata_parallel_batched <- function(
  req_base,
  soql,
  page_size,
  max_rows,
  max_active,
  verbose
) {
  all_pages <- vector("list", 256L)
  page_index <- 0L
  total_rows <- 0L
  page_num <- 1L

  repeat {
    rows_remaining <- max_rows - total_rows

    if (is.finite(rows_remaining) && rows_remaining <= 0L) {
      break
    }

    # ── Build a batch of max_active page requests ──────────────────────────
    batch_size <- if (is.finite(rows_remaining)) {
      min(max_active, ceiling(rows_remaining / page_size))
    } else {
      max_active
    }

    batch_reqs <- lapply(seq_len(batch_size), function(i) {
      p <- page_num + i - 1L

      rows_this_page <- if (is.finite(rows_remaining)) {
        as.integer(min(page_size, rows_remaining - (i - 1L) * page_size))
      } else {
        page_size
      }

      req_base |>
        httr2::req_body_json(list(
          query = soql,
          page = list(pageNumber = p, pageSize = rows_this_page),
          includeSynthetic = FALSE
        ))
    })

    if (verbose) {
      message(sprintf(
        "Fetching pages %d-%d (up to %d rows each) ...",
        page_num,
        page_num + batch_size - 1L,
        page_size
      ))
    }

    # ── Fire batch in parallel ─────────────────────────────────────────────
    resps <- httr2::req_perform_parallel(
      batch_reqs,
      on_error = "continue",
      max_active = max_active,
      progress = verbose
    )

    failed <- httr2::resps_failures(resps)
    if (length(failed) > 0L) {
      warning(
        length(failed),
        " page request(s) in this batch failed and will be ",
        "missing from results.",
        call. = FALSE
      )
    }

    successful <- httr2::resps_successes(resps)

    # ── Parse batch responses ──────────────────────────────────────────────
    done <- FALSE

    for (i in seq_along(successful)) {
      parsed <- tryCatch(
        yyjsonr::read_json_raw(httr2::resp_body_raw(successful[[i]])),
        error = function(e) {
          warning(
            "Failed to parse page ",
            page_num + i - 1L,
            ": ",
            conditionMessage(e),
            call. = FALSE
          )
          NULL
        }
      )

      if (is.null(parsed) || length(parsed) == 0L) {
        done <- TRUE
        break
      }

      n_rows <- nrow(parsed)

      if (n_rows > 0L) {
        batch_dt <- data.table::as.data.table(parsed)
        list_cols <- names(batch_dt)[vapply(batch_dt, is.list, logical(1L))]
        if (length(list_cols) > 0L) {
          batch_dt[,
            (list_cols) := lapply(.SD, as.character),
            .SDcols = list_cols
          ]
        }

        page_index <- page_index + 1L

        if (page_index > length(all_pages)) {
          all_pages <- c(all_pages, vector("list", 256L))
        }

        all_pages[[page_index]] <- batch_dt
        total_rows <- total_rows + n_rows
      }

      # Short page = last page
      if (n_rows < page_size) {
        done <- TRUE
        break
      }
    }

    if (done) {
      break
    }

    page_num <- page_num + batch_size
  }

  if (page_index == 0L) {
    return(tibble::tibble())
  }

  final_dt <- data.table::rbindlist(
    all_pages[seq_len(page_index)],
    fill = TRUE,
    use.names = TRUE
  )
  names(final_dt) <- janitor::make_clean_names(names(final_dt))

  tibble::as_tibble(final_dt)
}


#' Read a Socrata dataset in parallel (faster for large datasets)
#'
#' A drop-in replacement for [read_socrata()] that fetches pages concurrently
#' instead of sequentially. Attempts a metadata preflight to determine the
#' total row count and pre-build all page requests. If the row count is
#' unavailable, falls back to a batched parallel strategy that fires
#' \code{max_active} pages at a time and stops on a short page.
#'
#' @inheritParams read_socrata
#' @param max_active Integer. Maximum concurrent requests (default 5).
#'
#' @return A [tibble::tibble()] with all columns as character strings.
#'
#' @importFrom httr2 request req_body_json req_user_agent req_headers
#'   req_auth_basic req_throttle req_perform req_perform_parallel
#'   resp_body_raw resps_successes resps_failures
#' @importFrom yyjsonr read_json_raw
#' @importFrom data.table as.data.table rbindlist
#' @importFrom janitor make_clean_names
#' @importFrom tibble as_tibble
#' @export
read_socrata_parallel <- function(
  url,
  domain = NULL,
  soql = "SELECT *",
  app_token = NULL,
  socrata_user = NULL,
  password = NULL,
  page_size = 5000L,
  max_rows = Inf,
  max_active = 20L,
  verbose = FALSE
) {
  # ── Validate ───────────────────────────────────────────────────────────────
  if (missing(url) || !nzchar(trimws(url))) {
    stop("`url` must be a non-empty string.", call. = FALSE)
  }
  page_size <- as.integer(page_size)
  max_active <- as.integer(max_active)

  ds <- resolve_dataset(url, domain)
  endpoint <- paste0(
    "https://",
    ds$hostname,
    "/api/v3/views/",
    ds$four_by_four,
    "/query.json"
  )

  # ── Build authenticated base request ──────────────────────────────────────
  req_base <- httr2::request(endpoint) |>
    httr2::req_user_agent(socratr_ua()) |>
    httr2::req_headers(
      "Accept" = "application/json",
      "Content-Type" = "application/json"
    ) |>
    httr2::req_throttle(rate = 10 / 1)

  if (!is.null(app_token)) {
    req_base <- req_base |> httr2::req_headers("X-App-Token" = app_token)
  }
  if (!is.null(socrata_user) && !is.null(password)) {
    req_base <- req_base |> httr2::req_auth_basic(socrata_user, password)
  }

  # ── Preflight: try metadata for row count ──────────────────────────────────
  total_rows_api <- tryCatch(
    {
      if (verbose) {
        message("Running metadata preflight to get row count ...")
      }
      meta <- get_metadata(
        url = url,
        domain = domain,
        app_token = app_token,
        socrata_user = socrata_user,
        password = password
      )
      as.integer(meta$row_count)
    },
    error = function(e) NA_integer_
  )

  # Metadata failed or returned NA — try COUNT(*) via SoQL
  if (is.na(total_rows_api) || total_rows_api <= 0L) {
    if (verbose) {
      message("Metadata unavailable. Trying COUNT(*) query ...")
    }
    total_rows_api <- tryCatch(
      {
        count_resp <- httr2::req_perform(
          req_base |>
            httr2::req_body_json(list(
              query = "SELECT COUNT(*)",
              page = list(pageNumber = 1L, pageSize = 1L),
              includeSynthetic = FALSE
            ))
        )
        parsed <- yyjsonr::read_json_raw(httr2::resp_body_raw(count_resp))
        as.integer(parsed[[1]][[1]])
      },
      error = function(e) NA_integer_
    )
  }

  # Both failed — fall back to batched parallel
  if (is.na(total_rows_api) || total_rows_api <= 0L) {
    if (verbose) {
      message("Row count unavailable. Using batched parallel fetch ...")
    }
    return(.read_socrata_parallel_batched(
      req_base = req_base,
      soql = soql,
      page_size = page_size,
      max_rows = max_rows,
      max_active = max_active,
      verbose = verbose
    ))
  }

  # ── Preflight succeeded: pre-build all page requests ──────────────────────
  total_rows_wanted <- if (is.finite(max_rows)) {
    as.integer(min(total_rows_api, max_rows))
  } else {
    total_rows_api
  }

  n_pages <- ceiling(total_rows_wanted / page_size)

  if (verbose) {
    message(sprintf(
      "COUNT = %d rows. Fetching %d page(s) of %d rows (max_active = %d) ...",
      total_rows_api,
      n_pages,
      page_size,
      max_active
    ))
  }

  reqs <- lapply(seq_len(n_pages), function(p) {
    rows_this_page <- if (is.finite(max_rows)) {
      as.integer(min(page_size, total_rows_wanted - (p - 1L) * page_size))
    } else {
      page_size
    }

    req_base |>
      httr2::req_body_json(list(
        query = soql,
        page = list(pageNumber = p, pageSize = rows_this_page),
        includeSynthetic = FALSE
      ))
  })

  resps <- httr2::req_perform_parallel(
    reqs,
    on_error = "continue",
    max_active = max_active,
    progress = verbose
  )

  failed <- httr2::resps_failures(resps)
  if (length(failed) > 0L) {
    warning(
      length(failed),
      " page request(s) failed and will be missing from results.",
      call. = FALSE
    )
  }

  successful <- httr2::resps_successes(resps)
  if (length(successful) == 0L) {
    stop("All page requests failed.", call. = FALSE)
  }

  all_pages <- vector("list", length(successful))
  for (i in seq_along(successful)) {
    parsed <- tryCatch(
      yyjsonr::read_json_raw(httr2::resp_body_raw(successful[[i]])),
      error = function(e) {
        warning(
          "Failed to parse page ",
          i,
          ": ",
          conditionMessage(e),
          call. = FALSE
        )
        NULL
      }
    )
    if (is.null(parsed) || length(parsed) == 0L) {
      next
    }

    batch_dt <- data.table::as.data.table(parsed)
    list_cols <- names(batch_dt)[vapply(batch_dt, is.list, logical(1L))]
    if (length(list_cols) > 0L) {
      batch_dt[, (list_cols) := lapply(.SD, as.character), .SDcols = list_cols]
    }
    all_pages[[i]] <- batch_dt
  }

  all_pages <- Filter(Negate(is.null), all_pages)
  if (length(all_pages) == 0L) {
    return(tibble::tibble())
  }

  final_dt <- data.table::rbindlist(all_pages, fill = TRUE, use.names = TRUE)
  names(final_dt) <- janitor::make_clean_names(names(final_dt))

  tibble::as_tibble(final_dt)
}

# ── Parallel write ────────────────────────────────────────────────────────────

#' Upload data to Socrata in parallel (faster for large datasets)
#'
#' A drop-in replacement for [write_socrata()] that sends chunks concurrently
#' via [httr2::req_perform_parallel()] instead of a sequential `for` loop.
#' Only `"UPSERT"` supports parallel sending; `"REPLACE"` is always a single
#' atomic PUT and falls back to [write_socrata()] automatically.
#'
#' @section When to use this:
#' For large `UPSERT` operations (>50 000 rows), parallel sending can reduce
#' wall-clock time significantly. For `REPLACE`, or for smaller uploads, use
#' [write_socrata()] — the parallelism overhead is not worth it below ~5 chunks.
#'
#' @section Limitations:
#' `req_perform_parallel()` does not support `req_retry()`. Each chunk is sent
#' once; failed chunks are reported in the return value but not retried.
#' Use [write_socrata()] if retry-on-failure is required.
#'
#' @inheritParams write_socrata
#' @param max_active Integer. Maximum concurrent upload requests (default 5).
#'   Do not exceed 10 without explicit approval from your Socrata account team.
#'
#' @return Invisibly returns a named list with elements:
#'   \describe{
#'     \item{`responses`}{List of `httr2` response objects for successful chunks.}
#'     \item{`failures`}{List of error objects for failed chunks (empty on full success).}
#'     \item{`summary`}{A data frame of aggregate row counts.}
#'   }
#'
#' @examples
#' \dontrun{
#' write_socrata_parallel(
#'   dataframe    = my_large_df,
#'   domain       = "data.somervillema.gov",
#'   dataset_id   = "abcd-1234",
#'   update_mode  = "UPSERT",
#'   socrata_user = Sys.getenv("SOCRATA_USER"),
#'   password     = Sys.getenv("SOCRATA_KEY"),
#'   chunk_size   = 10000L,
#'   max_active   = 20L
#' )
#' }
#'
#' @importFrom httr2 request req_user_agent req_auth_basic req_body_raw
#'   req_method req_headers req_throttle req_perform_parallel
#'   resp_body_raw resps_successes resps_failures
#' @importFrom yyjsonr write_json_raw read_json_raw
#' @export
write_socrata_parallel <- function(
  dataframe,
  domain,
  dataset_id,
  update_mode = c("UPSERT", "REPLACE"),
  socrata_user,
  password,
  app_token = NULL,
  chunk_size = 10000L,
  max_active = 20L
) {
  update_mode <- match.arg(update_mode)
  chunk_size <- as.integer(chunk_size)
  max_active <- as.integer(max_active)

  if (!is.data.frame(dataframe)) {
    stop("`dataframe` must be a data frame.", call. = FALSE)
  }
  if (!nzchar(trimws(domain))) {
    stop("`domain` must be a non-empty string.", call. = FALSE)
  }
  if (!is_four_by_four(dataset_id)) {
    stop("`dataset_id` must be a valid 4x4 identifier.", call. = FALSE)
  }
  if (is.na(chunk_size) || chunk_size < 1L) {
    stop("`chunk_size` must be a positive integer.", call. = FALSE)
  }
  # if (is.na(max_active) || max_active < 1L || max_active > 10L) {
  #   stop("`max_active` must be between 1 and 10.", call. = FALSE)
  # }

  # REPLACE is a single atomic PUT — parallelism doesn't apply.
  # Fall back to the serial write_socrata() which handles it correctly.
  if (update_mode == "REPLACE") {
    message(
      "REPLACE mode is always a single atomic PUT; falling back to write_socrata()."
    )
    write_socrata(
      dataframe = dataframe,
      domain = domain,
      dataset_id = dataset_id,
      update_mode = "REPLACE",
      socrata_user = socrata_user,
      password = password,
      app_token = app_token,
      chunk_size = chunk_size
    )
    return(invisible(NULL))
  }

  hostname <- gsub("^https?://|/.*$", "", domain)
  base_url <- paste0("https://", hostname, "/resource/", dataset_id, ".json")

  # ── Base request ───────────────────────────────────────────────────────────
  # req_throttle instead of req_retry: req_perform_parallel does not support
  # req_retry. We throttle to respect Socrata's rate limits.
  req_base <- httr2::request(base_url) |>
    httr2::req_user_agent(socratr_ua()) |>
    httr2::req_auth_basic(socrata_user, password) |>
    httr2::req_headers("Accept" = "application/json") |>
    httr2::req_throttle(rate = 10 / 1) |>
    httr2::req_method("POST")

  if (!is.null(app_token)) {
    req_base <- req_base |> httr2::req_headers("X-App-Token" = app_token)
  }

  # ── Build chunk requests ───────────────────────────────────────────────────
  n_rows <- nrow(dataframe)
  starts <- seq(1L, n_rows, by = chunk_size)
  n_chunks <- length(starts)

  message(sprintf(
    "Starting parallel UPSERT of %d row(s) in %d chunk(s) (max_active = %d) to %s ...",
    n_rows,
    n_chunks,
    max_active,
    dataset_id
  ))

  reqs <- lapply(starts, function(i) {
    chunk <- dataframe[i:min(i + chunk_size - 1L, n_rows), , drop = FALSE]
    req_base |>
      httr2::req_body_raw(yyjsonr::write_json_raw(chunk), "application/json")
  })

  # ── Fire in parallel ───────────────────────────────────────────────────────
  resps <- httr2::req_perform_parallel(
    reqs,
    on_error = "continue",
    max_active = max_active,
    progress = TRUE
  )

  successful <- httr2::resps_successes(resps)
  failed <- httr2::resps_failures(resps)

  # ── Aggregate summary from successful responses ────────────────────────────
  totals <- list(
    Rows_Created = 0L,
    Rows_Updated = 0L,
    Rows_Deleted = 0L,
    Errors = 0L
  )

  for (r in successful) {
    summ <- tryCatch(
      yyjsonr::read_json_raw(httr2::resp_body_raw(r)),
      error = function(...) list()
    )
    totals$Rows_Created <- totals$Rows_Created + (summ$Rows_Created %||% 0L)
    totals$Rows_Updated <- totals$Rows_Updated + (summ$Rows_Updated %||% 0L)
    totals$Rows_Deleted <- totals$Rows_Deleted + (summ$Rows_Deleted %||% 0L)
    totals$Errors <- totals$Errors + (summ$Errors %||% 0L)
  }

  if (length(failed) > 0L) {
    warning(
      length(failed),
      " of ",
      n_chunks,
      " chunk(s) failed. ",
      "Re-run with write_socrata() for automatic retry.",
      call. = FALSE
    )
  }

  cat(sprintf(
    "\n--- Upload Summary (%d/%d chunks succeeded) ---\n",
    length(successful),
    n_chunks
  ))
  print(as.data.frame(totals))

  invisible(list(
    responses = successful,
    failures = failed,
    summary = as.data.frame(totals)
  ))
}

#' Tune max_active and page_size for read_socrata_parallel
#'
#' Runs two sequential benchmarks:
#'   1. Sweeps `max_active` with a fixed `page_size` to find the optimal
#'      concurrency level.
#'   2. Fixes `max_active` at the optimal value and sweeps `page_size`.
#'
#' @param url          Character. A full Socrata dataset URL or bare 4x4 ID.
#' @param domain       Character. Required only when `url` is a bare 4x4 ID.
#' @param app_token    Character (optional). Socrata Application Token.
#' @param socrata_user Character (optional). Socrata account email or API Key ID.
#' @param password     Character (optional). Socrata password or API Secret Key.
#' @param soql         Character. SoQL query (default `"SELECT *"`).
#' @param max_active_values Integer vector. Concurrency levels to test
#'   (default `c(1, 5, 10, 15, 20, 25)`).
#' @param page_size_values  Integer vector. Page sizes to test
#'   (default `c(1000, 5000, 10000, 25000, 50000)`).
#' @param fixed_page_size   Integer. Page size held constant during the
#'   `max_active` sweep (default `5000L`).
#' @param save_plot    Logical. If `TRUE`, saves the plot to `path` (default `TRUE`).
#' @param path         Character. Output file path for the saved plot
#'   (default `"socrata_tune.png"`).
#'
#' @return Invisibly returns a list with elements:
#'   \describe{
#'     \item{`max_active_results`}{data.frame of max_active sweep timings.}
#'     \item{`page_size_results`}{data.frame of page_size sweep timings.}
#'     \item{`optimal_max_active`}{Integer. Best max_active value found.}
#'     \item{`optimal_page_size`}{Integer. Best page_size value found.}
#'   }
#'
#' @examples
#' \dontrun{
#' tune <- tune_socrata_parallel(
#'   url          = "https://data.somervillema.gov/resource/abcd-1234",
#'   app_token    = Sys.getenv("SOCRATA_APP_TOKEN"),
#'   socrata_user = Sys.getenv("SOCRATA_USER"),
#'   password     = Sys.getenv("SOCRATA_PASSWORD")
#' )
#' tune$optimal_max_active
#' tune$optimal_page_size
#' }
#'
#' @importFrom ggplot2 ggplot aes geom_area geom_line geom_point geom_hline
#'   annotate scale_x_continuous labs theme_minimal theme element_rect
#'   element_line element_text
#' @importFrom dplyr bind_rows
#' @importFrom scales comma
#' @export
tune_socrata_parallel <- function(
  url,
  domain = NULL,
  app_token = NULL,
  socrata_user = NULL,
  password = NULL,
  soql = "SELECT *",
  max_active_values = c(1L, 5L, 10L, 15L, 20L, 25L),
  page_size_values = c(1000L, 5000L, 10000L, 25000L, 50000L),
  fixed_page_size = 5000L,
  save_plot = TRUE,
  path = "socrata_tune.png"
) {
  # ── Shared call args (avoids repetition in both sweeps) ───────────────────
  base_args <- list(
    url = url,
    domain = domain,
    soql = soql,
    app_token = app_token,
    socrata_user = socrata_user,
    password = password
  )

  # ── Dark theme (shared by both panels) ────────────────────────────────────
  dark_theme <- ggplot2::theme_minimal(base_family = "mono") +
    ggplot2::theme(
      plot.background = ggplot2::element_rect(fill = "#0b0e14", color = NA),
      panel.background = ggplot2::element_rect(fill = "#111520", color = NA),
      panel.grid.major = ggplot2::element_line(color = "#1e2535"),
      panel.grid.minor = ggplot2::element_line(color = "#161c2a"),
      plot.title = ggplot2::element_text(
        color = "#e8f0ff",
        size = 13,
        face = "bold"
      ),
      plot.subtitle = ggplot2::element_text(color = "#556080", size = 9),
      plot.caption = ggplot2::element_text(color = "#556080", size = 7),
      axis.text = ggplot2::element_text(color = "#556080"),
      axis.title = ggplot2::element_text(color = "#8899bb"),
      axis.text.x = ggplot2::element_text(angle = 45, hjust = 1)
    )

  .make_panel <- function(
    df,
    x_col,
    x_label,
    x_breaks,
    x_fmt_fn,
    title,
    subtitle
  ) {
    optimal <- df[which.min(df$elapsed), ]
    baseline_y <- df$elapsed[which.min(df[[x_col]])]

    ggplot2::ggplot(df, ggplot2::aes(x = .data[[x_col]], y = elapsed)) +
      ggplot2::geom_area(fill = "#00e5ff", alpha = 0.08) +
      ggplot2::geom_line(color = "#00e5ff", linewidth = 1.1) +
      ggplot2::geom_point(color = "#00e5ff", size = 2.5) +
      ggplot2::geom_point(
        data = optimal,
        color = "#a8ff78",
        size = 5,
        shape = 18
      ) +
      ggplot2::annotate(
        "text",
        x = optimal[[x_col]],
        y = optimal$elapsed * 1.10,
        label = sprintf(
          "optimal\n%s = %s\n%.1fs",
          x_col,
          x_fmt_fn(optimal[[x_col]]),
          optimal$elapsed
        ),
        color = "#a8ff78",
        size = 3.1,
        hjust = 0.5,
        family = "mono"
      ) +
      ggplot2::geom_hline(
        yintercept = baseline_y,
        linetype = "dashed",
        color = "#ff6b35",
        linewidth = 0.6,
        alpha = 0.6
      ) +
      ggplot2::annotate(
        "text",
        x = max(df[[x_col]]) * 0.97,
        y = baseline_y * 1.05,
        label = sprintf("baseline (%.1fs)", baseline_y),
        color = "#ff6b35",
        size = 2.9,
        hjust = 1,
        family = "mono"
      ) +
      ggplot2::scale_x_continuous(
        breaks = x_breaks,
        labels = vapply(x_breaks, x_fmt_fn, character(1L))
      ) +
      ggplot2::labs(
        title = title,
        subtitle = subtitle,
        x = x_label,
        y = "Elapsed time (seconds)",
        caption = "green diamond = optimal  |  orange dashed = baseline"
      ) +
      dark_theme
  }

  # ── Sweep 1: max_active ────────────────────────────────────────────────────
  message("\n── Sweep 1/2: max_active (page_size = ", fixed_page_size, ") ──")

  ma_results <- dplyr::bind_rows(lapply(max_active_values, function(n) {
    message(sprintf("  Testing max_active = %d ...", n))
    t <- system.time(do.call(
      read_socrata_parallel,
      c(
        base_args,
        list(
          page_size = fixed_page_size,
          max_active = n
        )
      )
    ))
    data.frame(
      max_active = n,
      elapsed = t[["elapsed"]],
      user = t[["user.self"]],
      sys = t[["sys.self"]]
    )
  }))

  optimal_max_active <- ma_results$max_active[which.min(ma_results$elapsed)]
  message(sprintf(
    "  → optimal max_active = %d (%.1fs)",
    optimal_max_active,
    min(ma_results$elapsed)
  ))

  p1 <- .make_panel(
    df = ma_results,
    x_col = "max_active",
    x_label = "max_active (parallel connections)",
    x_breaks = max_active_values,
    x_fmt_fn = as.character,
    title = "Sweep 1 — max_active vs Elapsed Time",
    subtitle = sprintf("page_size = %s (fixed)", scales::comma(fixed_page_size))
  )

  # ── Sweep 2: page_size ────────────────────────────────────────────────────
  message(sprintf(
    "\n── Sweep 2/2: page_size (max_active = %d) ──",
    optimal_max_active
  ))

  ps_results <- dplyr::bind_rows(lapply(page_size_values, function(ps) {
    message(sprintf("  Testing page_size = %d ...", ps))
    t <- system.time(do.call(
      read_socrata_parallel,
      c(
        base_args,
        list(
          page_size = ps,
          max_active = optimal_max_active
        )
      )
    ))
    data.frame(
      page_size = ps,
      elapsed = t[["elapsed"]],
      user = t[["user.self"]],
      sys = t[["sys.self"]]
    )
  }))

  optimal_page_size <- ps_results$page_size[which.min(ps_results$elapsed)]
  message(sprintf(
    "  → optimal page_size = %s (%.1fs)",
    scales::comma(optimal_page_size),
    min(ps_results$elapsed)
  ))

  p2 <- .make_panel(
    df = ps_results,
    x_col = "page_size",
    x_label = "page_size (rows per request)",
    x_breaks = page_size_values,
    x_fmt_fn = function(x) scales::comma(x),
    title = "Sweep 2 — page_size vs Elapsed Time",
    subtitle = sprintf(
      "max_active = %d (optimal from Sweep 1)",
      optimal_max_active
    )
  )

  # ── Combine panels & print ─────────────────────────────────────────────────
  combined <- patchwork::wrap_plots(p1, p2, ncol = 2) +
    patchwork::plot_annotation(
      title = "read_socrata_parallel — Parallel Tuning",
      subtitle = url,
      theme = ggplot2::theme(
        plot.background = ggplot2::element_rect(fill = "#0b0e14", color = NA),
        plot.title = ggplot2::element_text(
          color = "#e8f0ff",
          size = 15,
          face = "bold",
          family = "mono"
        ),
        plot.subtitle = ggplot2::element_text(
          color = "#556080",
          size = 9,
          family = "mono"
        )
      )
    )

  print(combined)

  if (save_plot) {
    ggplot2::ggsave(
      path,
      plot = combined,
      width = 14,
      height = 6,
      dpi = 150,
      bg = "#0b0e14"
    )
    message("\nPlot saved to: ", path)
  }

  # ── Final recommendation ───────────────────────────────────────────────────
  message(sprintf(
    "\n── Recommendation ──────────────────────────────────────────\n  max_active = %d\n  page_size  = %s\n────────────────────────────────────────────────────────────",
    optimal_max_active,
    scales::comma(optimal_page_size)
  ))

  invisible(list(
    max_active_results = ma_results,
    page_size_results = ps_results,
    optimal_max_active = optimal_max_active,
    optimal_page_size = optimal_page_size
  ))
}
