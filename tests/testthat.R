## LOAD REQUIRED LIBRARIES

library(testthat)
library(RSocrata)
library(httr)
library(jsonlite)
library(mime)
library(plyr)

Sys.setenv(
  SOCRATA_USER = "mark.silverberg+soda.demo@socrata.com",
  SOCRATA_PASSWORD = "7vFDsGFDUG"
)

## RUN TESTS
## This command will run all files matching test*.R in `./tests`, unless
## the tests are skipped globally or individually.
## Note: Run locally with `devtools::check()`
## Note: Run this to test as CRAN: Sys.setenv(NOT_CRAN=FALSE)

test_check("RSocrata")
