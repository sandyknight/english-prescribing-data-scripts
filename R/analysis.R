library(arrow)
library(tidyverse)

df <- arrow::read_parquet("data/epd_data.parquet") |> 
  as_arrow_table()

df |> 
  group_by(YEAR_MONTH, CHEMICAL_SUBSTANCE_BNF_DESCR) |> 
  #Osummarise(TOTAL_QUANTITY = sum(TOTAL_QUANTITY, na.rm = TRUE)) |> 
  mutate(date = paste0(YEAR_MONTH, "01")) |> 
  #filter(TOTAL_QUANTITY > 100) |> 
  collect() |> 
  mutate(date = as.Date(date, "%Y%m%d")) |> 
  ggplot(aes(x = date, y = TOTAL_QUANTITY)) + 
  geom_line(aes(colour = CHEMICAL_SUBSTANCE_BNF_DESCR))  + 
  scale_y_log10()


