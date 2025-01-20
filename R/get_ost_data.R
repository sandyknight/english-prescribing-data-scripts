# Currently only buprenorphine data
# 1. Getting the API working --------------------------------------------------
# This is all based on the instructional script here:
# https://raw.githubusercontent.com/nhsbsa-data-analytics/open-data-portal-api/refs/heads/master/open-data-portal-api.R
# which is on the very helpful NHSBSA Data Analytics github.com/nhsbsa-data-analytics

# Define the url for the API call
base_endpoint <- "https://opendata.nhsbsa.net/api/3/action/"
package_list_method <- "package_list"     # List of data-sets in the portal
package_show_method <- "package_show?id=" # List all resources of a data-set
action_method <- "datastore_search_sql?" # SQL action method

# Send API call to get list of data-sets
datasets_response <- jsonlite::fromJSON(paste0(
  base_endpoint,
  package_list_method
))

metadata_repsonse <- jsonlite::fromJSON(paste0(
  base_endpoint,
  package_show_method,
  dataset_id
))

# We're interested in the English Prescribing Dataset (EPD).
dataset_id <- "english-prescribing-data-epd"

resource_name <- "EPD_202001" # All EPD data is monthly and uses YYYYMM
bnf_chemical_substance <- "0410030A0" # Buprenorphine hydrochloride

# Resource names and IDs are kept within the resources table returned from the
# package_show_method call.
resources_table <- metadata_repsonse$result$resources

# I want data from 2020-present
resource_name_list <- resources_table$name[grepl("202[0-4]", resources_table$name)]

# Construct the SQL query as a function
async_query <- function(resource_name) {
  paste0(
    "
    SELECT
        *
    FROM `",
        resource_name, "`
    WHERE
        1=1
    AND bnf_chemical_substance = '", bnf_chemical_substance, "'
    "
  )
}

# Create the API calls
async_api_calls <- lapply(
  X = resource_name_list,
  FUN = function(x)
    paste0(
      base_endpoint,
      action_method,
      "resource_id=",
      x,
      "&",
      "sql=",
      URLencode(async_query(x)) # Encode spaces in the url
    )
)

# Use crul::Async to get the results
# I got rate limited at 5 asynchronous calls for one substance (burprenorphine) across all geographies.
# It seems likely that this defeats the point of using async at all but I'm comitted now

get_x_calls <-
  function(calls, from, to) {
    dd <- crul::Async$new(urls = calls[from:to])
    res <- dd$get()
    # Check that everything is a success
    test <- all(vapply(res, function(z) z$success(), logical(1)))
    message(test)
    return(res)
  }




parse_api_responses <- function(res) {
  # Parse the output into a list of dataframes
  async_dfs <- lapply(
    X = res,
    FUN = function(x) {
      # Parse the response
      tmp_response <- x$parse("UTF-8")
      # Extract the records
      tmp_df <- jsonlite::fromJSON(tmp_response)$result$result$records
    }
  )
  df <-
    data.table::rbindlist(async_dfs)
  return(df)
}


get_data_from_api <- function(calls, step = 3, interval = 60) {

  froms <- seq(1, length(calls), step)
  tos <- froms + (step - 1)

  for (i in seq_along(froms)) {
    res <-  get_x_calls(calls = calls, from = froms[i], to = tos[i])
    df <- parse_api_responses(res)
    data.table::fwrite(df, paste0("data/epd_", paste0(Sys.time(), ".csv")))
    Sys.sleep(interval)
  }
}


## list_results <- get_data_from_api(calls = async_api_calls, interval = 300)

## df <- data.table::rbindlist(l = list_results)



## data.table::fwrite(df, "bupe-prescribing-202001-202410.csv")
