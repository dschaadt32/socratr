context("write Socrata datasets")

# Credentials — set as env vars; never hard-code.
socrata_user <- Sys.getenv("SOCRATA_USER")
socrata_password <- Sys.getenv("SOCRATA_PASSWORD")

skip_if_no_auth <- function() {
  if (identical(socrata_user, "") || identical(socrata_password, "")) {
    skip("No Socrata credentials found in environment variables")
  }
}

test_that("add a row to a dataset (UPSERT)", {
  skip_if_no_auth()

  domain <- "soda.demo.socrata.com"
  dataset_id <- "xh6g-yugi"

  df_in <- data.frame(
    x = sample(-1000:1000, 1),
    y = sample(-1000:1000, 1)
  )

  res <- write_socrata(
    dataframe = df_in,
    domain = domain,
    dataset_id = dataset_id,
    update_mode = "UPSERT",
    socrata_user = socrata_user,
    password = socrata_password
  )

  expect_type(res, "list")
  expect_length(res, 1L)
  expect_true(httr2::resp_status(res[[1L]]) %in% c(200L, 201L))
})

test_that("fully replace a dataset (REPLACE)", {
  skip_if_no_auth()

  domain <- "soda.demo.socrata.com"
  dataset_id <- "kc76-ybeq"

  df_in <- data.frame(
    x = sample(-1000:1000, 5),
    y = sample(-1000:1000, 5)
  )

  res <- write_socrata(
    dataframe = df_in,
    domain = domain,
    dataset_id = dataset_id,
    update_mode = "REPLACE",
    socrata_user = socrata_user,
    password = socrata_password
  )

  expect_type(res, "list")
  expect_length(res, 1L)
  expect_true(httr2::resp_status(res[[1L]]) %in% c(200L, 201L))
})
