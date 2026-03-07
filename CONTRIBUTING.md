# Contributing to socratr

Bug fixes and new features are welcome. Please read the following before submitting.

## Submitting a bug

[Open an issue on GitHub](https://github.com/your-org/socratr/issues) and include:

* What you did
* What happened
* What you expected to happen
* Where you think the error is occurring, if known

One issue per report, please. We'll respond with a label or follow-up questions.

## Making changes

* Branch off `dev` (not `main`) for all bug fixes and features.
* Make commits of logical units with clear messages.
* Add `testthat` tests for any new functionality.
* Document new functions and any new arguments with roxygen2.
* Update the version in `DESCRIPTION` following [semantic versioning](https://semver.org/spec/v2.0.0.html) (`x.y.z`).
* Update `NEWS.md` with a brief description of your change under the appropriate version heading.
* Update `DESCRIPTION` if your change adds or removes a package dependency, or raises the minimum R version required.
* Run the full test suite with `devtools::test()` before opening a pull request.
* Open a pull request against the `dev` branch with a clear description of what changed and why, or a reference to the relevant issue.

## Code style

* Use `httr2` for all HTTP — do not introduce `httr` or `curl` directly.
* Prefer `data.table` for in-memory aggregation of large page results.
* Keep internal helpers in the `# ── Internal helpers ──` section and mark them `#' @noRd`.
* Sequential functions (`read_socrata`, `write_socrata`) delegate to their parallel counterparts with `max_active = 1` — keep that pattern consistent.