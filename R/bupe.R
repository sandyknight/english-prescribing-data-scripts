library(data.table)

df <- data.table::fread("bupe-prescribing-202001-202410.csv")

df[, .N, by = c("BNF_DESCRIPTION", "YEAR_MONTH")]

df <- df[,.(YEAR_MONTH, BNF_DESCRIPTION, BNF_CODE, TOTAL_QUANTITY, POSTCODE, ITEMS, ACTUAL_COST)]

df <-
  df[, .(ITEMS = sum(as.numeric(ITEMS))), by = c("YEAR_MONTH", "BNF_DESCRIPTION")]

df[, description := gsub("\\d\\dmg|\\dmg|\\d.|\\d\\.\\d\\dml|\\/",
                         "",
                         x = BNF_DESCRIPTION)]

df[, description := stringr::str_squish(description)]

df <-
  df[grep("^Q", description, invert = TRUE, perl = TRUE), ][description != "-"]

df |>
  ggplot2::ggplot(ggplot2::aes(x = YEAR_MONTH, y = ITEMS)) +
  ggplot2::geom_col(ggplot2::aes(fill = description),
                    colour = "black",
                    ) +
  tinythemes::theme_ipsum_rc(base_size = 40) +
  ggsci::scale_fill_lancet()
