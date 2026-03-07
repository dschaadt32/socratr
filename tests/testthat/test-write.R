context("write Socrata datasets")

# 1. Parameter Setup
# Use the SomerStat-style environment variables for safety
socrata_user <- Sys.getenv("SOCRATA_USER")
socrata_password <- Sys.getenv("SOCRATA_PASSWORD")

# Helper to check if credentials exist before running
skip_if_no_auth <- function() {
  if (identical(socrata_user, "") || identical(socrata_password, "")) {
    skip("No Socrata credentials found in environment variables")
  }
}

## RUN TESTS

test_that("add a row to a dataset (UPSERT)", {
  skip_if_no_auth()

  dataset_endpoint <- "https://soda.demo.socrata.com/resource/xh6g-yugi.json"
  domain <- "soda.demo.socrata.com"
  dataset_id <- "xh6g-yugi"

  # Generate random test data
  df_in <- data.frame(
    x = sample(-1000:1000, 1),
    y = sample(-1000:1000, 1)
  )

  res <- write_socrata(
    data = df_in,
    domain = domain,
    dataset_id = dataset_id,
    update_mode = "UPSERT",
    socrata_user = socrata_user,
    password = socrata_password
  )

  # SODA 3 often returns 200 or 201 for successful writes
  expect_true(res$status_code %in% c(200L, 201L))
})


test_that("fully replace a dataset (REPLACE)", {
  skip_if_no_auth()

  # Modern SODA 3 View Endpoint
  domain <- "soda.demo.socrata.com"
  dataset_id < "kc76-ybeq"

  # Generate 5 rows of random data
  df_in <- data.frame(
    x = sample(-1000:1000, 5),
    y = sample(-1000:1000, 5)
  )

  # write.socrata should handle the PUT/POST to the replacement endpoint
  res <- write_socrata(
    data = df_in,
    domain = domain,
    dataset_id = dataset_id,
    update_mode = "REPLACE",
    socrata_user = socrata_user,
    password = socrata_password
  )

  expect_true(res$status_code %in% c(200L, 201L))
})


test_that("fully replace a dataset", {
  skip('See Issue #174')
  datasetToReplaceUrl <- "https://soda.demo.socrata.com/resource/kc76-ybeq.json"

  # populate df_in with two columns of random numbers
  x <- sample(-1000:1000, 5)
  y <- sample(-1000:1000, 5)
  df_in <- data.frame(x, y)

  # write to dataset
  res <- write.socrata(
    df_in,
    datasetToReplaceUrl,
    "REPLACE",
    socrataEmail,
    socrataPassword
  )

  # Check that the dataset was written without error
  expect_equal(res$status_code, 200L)
})
