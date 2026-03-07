# if (!require("pacman")) {
#   install.packages("pacman")
# }
# pacman::p_load(tidyverse, jsonlite, yyjsonr, httr2, httr, plyr)
# source("R/RSocrata.R")
# source("R/socratr.R")
# source("config.R")

# domain <- "data.somervillema.gov"
# external_311_dataset_id <- "4pyi-uqq6"
# legacy_external_311_dataset_id_endpoint_json <- "https://data.somervillema.gov/resource/4pyi-uqq6.json"
# legacy_external_311_dataset_id_endpoint_csv <- "https://data.somervillema.gov/resource/4pyi-uqq6.csv"
# test_311_write_dataset_id <- "vfpd-hcqd"

# # ── READING ───────────────────────────────────────────────────────────────────

# ## RSocrata (creds included for fairness)
# cat("--- RSocrata: read.socrata JSON ---\n")
# t_legacy_json <- system.time(
#   res_legacy_json <- read.socrata(
#     legacy_external_311_dataset_id_endpoint_json,
#     email = socrata_user,
#     password = socrata_password,
#     app_token = socrata_token
#   )
# )
# print(t_legacy_json)

# cat("--- RSocrata: read.socrata CSV ---\n")
# t_legacy_csv <- system.time(
#   res_legacy_csv <- read.socrata(
#     legacy_external_311_dataset_id_endpoint_csv,
#     email = socrata_user,
#     password = socrata_password,
#     app_token = socrata_token
#   )
# )
# print(t_legacy_csv)

# ## socratr
# cat("--- socratr: read_socrata (sequential) ---\n")
# t_sequential <- system.time(
#   res_sequential <- read_socrata(
#     url = external_311_dataset_id,
#     domain = domain,
#     app_token = socrata_token,
#     socrata_user = socrata_user,
#     password = socrata_password
#   )
# )
# print(t_sequential)

# cat("--- socratr: read_socrata_parallel ---\n")
# t_parallel <- system.time(
#   res_parallel <- read_socrata_parallel(
#     url = external_311_dataset_id,
#     domain = domain,
#     app_token = socrata_token,
#     socrata_user = socrata_user,
#     password = socrata_password,
#     max_active = 15,
#     page_size = 25000
#   )
# )
# print(t_parallel)

# # ── Summary table ─────────────────────────────────────────────────────────────
# times <- c(
#   legacy_json = t_legacy_json[["elapsed"]],
#   legacy_csv = t_legacy_csv[["elapsed"]],
#   socratr_seq = t_sequential[["elapsed"]],
#   socratr_parallel = t_parallel[["elapsed"]]
# )

# tbl <- data.frame(
#   method = names(times),
#   elapsed_sec = round(times, 2L),
#   vs_legacy_json = round(times / times[["legacy_json"]], 2L),
#   row.names = NULL
# )

# cat("\n=== Benchmark Summary ===\n")
# print(tbl)

# write_socrata(
#   res_legacy_json,
#   "data.somervillema.gov",
#   test_311_write_dataset_id,
#   update_mode = "REPLACE",
#   socrata_user = socrata_user,
#   password = socrata_password,
#   app_token = app_token
# )

# write_socrata_parallel(
#   res_legacy_json,
#   "data.somervillema.gov",
#   test_311_write_dataset_id,
#   update_mode = "REPLACE",
#   socrata_user = socrata_user,
#   password = socrata_password,
#   app_token = app_token,
#   max_active = 15,
#   chunk_size = 25,
#   000
# )

# tune <- tune_socrata_parallel(
#   url = external_311_dataset_id,
#   domain = domain,
#   app_token = socrata_token,
#   socrata_user = socrata_user,
#   password = socrata_password
# )
