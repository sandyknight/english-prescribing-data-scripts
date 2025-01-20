library(data.table)

# Load BNF codes and names
bnf_info <-
  data.table::fread("data/20241101_1730476037387_BNF_Code_Information.csv")

# Turns out "Nicotine dependence" has a BNF paragraph its own
bnf_info <-
  bnf_info[grep("nicotine", `BNF Paragraph`, ignore.case = TRUE, perl = TRUE)]

# Grab the paragraph code
nicotine_dependence_code <-
  unique(bnf_info[["BNF Paragraph Code"]])

# EPD API
# This is all based on the instructional script here:

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

dataset_id <- "english-prescribing-data-epd"

# 4. API calls for single month ------------------------------------------------

# Define the parameters for the SQL query
resource_name <- "EPD_202001" # All EPD data is monthly and uses YYYYMM

# Build SQL query (WHERE criteria should be enclosed in single quotes)
single_month_query <- paste0(
  "
  SELECT
      *
  FROM `",
  resource_name, "`
  WHERE
      1=1
  AND bnf_paragraph_code = '", nicotine_dependence_code,"'
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
