# 1. Script details ------------------------------------------------------------

# Name of script: OpenDataAPIQuery
# Description:  Using R to query the NHSBSA open data portal API. 
# Created by: Matthew Wilson (NHSBSA)
# Created on: 26-03-2020
# Latest update by: Adam Ivison (NHSBSA)
# Latest update on: 24-06-2021
# Update notes: Updated endpoint in the script, refactored code and added async

# R version: created in 3.5.3

# 2. Load packages -------------------------------------------------------------

# List packages we will use
packages <- c(
  "jsonlite", # 1.6
  "dplyr",    # 0.8.3
  "crul"      # 1.1.0
)

# Install packages if they aren't already
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
  install.packages(setdiff(packages, rownames(installed.packages())))  
}

# 3. Define variables ----------------------------------------------------------

# Define the url for the API call
base_endpoint <- "https://opendata.nhsbsa.net/api/3/action/"
package_list_method <- "package_list"     # List of data-sets in the portal
package_show_method <- "package_show?id=" # List all resources of a data-set
action_method <- "datastore_search_sql?"  # SQL action method

# Send API call to get list of data-sets
datasets_response <- jsonlite::fromJSON(paste0(
  base_endpoint, 
  package_list_method
))

# Now lets have a look at the data-sets currently available
datasets_response$result

# For this example we're interested in the English Prescribing Dataset (EPD).
# We know the name of this data-set so can set this manually, or access it 
# from datasets_response.
dataset_id <- "english-prescribing-data-epd"

# 4. API calls for single month ------------------------------------------------

# Define the parameters for the SQL query
resource_name <- "EPD_202001" # For EPD resources are named EPD_YYYYMM
pco_code <- "13T00" # Newcastle Gateshead CCG
bnf_chemical_substance <- "040702%" # Paracetamol

# Build SQL query (WHERE criteria should be enclosed in single quotes)  
single_month_query <- paste0(
  "
  SELECT 
      * 
  FROM `", 
  resource_name, "` 
  WHERE 
      1=1 
  AND pco_code = '", pco_code, "' 
  AND bnf_chemical_substance LIKE '", bnf_chemical_substance,"' OR bnf_chemical_substance LIKE '41003%'
  "
)

# Build API call
single_month_api_call <- paste0(
  base_endpoint,
  action_method,
  "resource_id=",
  resource_name, 
  "&",
  "sql=",
  URLencode(single_month_query) # Encode spaces in the url
)

# Grab the response JSON as a list
single_month_response <- jsonlite::fromJSON(single_month_api_call)

# Extract records in the response to a dataframe
single_month_df <- single_month_response$result$result$records

# Lets have a quick look at the data
str(single_month_df)
head(single_month_df)

single_month_df |> 
  dplyr::select(CHEMICAL_SUBSTANCE_BNF_DESCR) |> unique()
# 5.2. Async -- ----------------------------------------------------------------

# We can call the API asynchronously and this will result in an approx 10x speed 
# increase over a for loop for large resource_names by vectorising our approach.

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
    AND pco_code = '", pco_code, "' 
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
dd <- crul::Async$new(urls = async_api_calls)
res <- dd$get()

# Check that everything is a success
all(vapply(res, function(z) z$success(), logical(1)))

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

# Concatenate the results 
aysnc_df <- do.call(dplyr::bind_rows, async_dfs)

# 6. Export the data -----------------------------------------------------------

# Use write.csv for ease
write.csv(single_month_df, "single_month.csv")
write.csv(for_loop_df, "for_loop.csv")
write.csv(aysnc_df, "aysnc.csv")