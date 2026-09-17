# Contributing to socratr

Bug fixes and new features are welcome.

## Submitting a bug

[Open an issue on GitHub](https://github.com/dschaadt/socratr/issues) and include:

* What you did
* What happened
* What you expected to happen
* A reproducible example when possible

## Making changes

* Branch off `main` for bug fixes and features.
* Make commits of logical units with clear messages.
* Add `testthat` tests for any new functionality.
* Document new functions and arguments with roxygen2 (`#'` comments in `R/`).
* Update the version in `DESCRIPTION` following [semantic versioning](https://semver.org/).
* Update `NEWS.md` under the appropriate version heading.
* Update `DESCRIPTION` if you add or remove a dependency.
* Run `devtools::document()`, `devtools::test()`, and preferably
  `devtools::check()` before opening a pull request.

## Code style

* Use `httr2` for all HTTP — do not introduce `httr` or `curl` directly.
* Prefer `data.table` for aggregating large page results.
* Keep internal helpers marked with `#' @noRd`.
* `write_socrata()` is the serial write path with retries;
  `write_socrata_parallel()` calls into it for `REPLACE` mode.
* Live API tests should skip when credentials are absent
  (`SOCRATA_USER` / `SOCRATA_PASSWORD` / `SOCRATA_TOKEN`).

## Local secrets

Never commit credentials. Use environment variables or a gitignored
`config.R` / `.Renviron` for local work.
