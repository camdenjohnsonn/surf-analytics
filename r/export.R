.libPaths(c("~/R/library", .libPaths()))

library(DBI)
library(RPostgres)

con <- dbConnect(
  RPostgres::Postgres(),
  host     = "localhost",
  port     = 5432,
  dbname   = "surf_analytics",
  user     = "camdenjohnson",
  password = "surfanalytics"
)

on.exit(dbDisconnect(con), add = TRUE)

run_query <- function(filepath) {
  sql <- paste(readLines(filepath), collapse = "\n")
  dbGetQuery(con, sql)
}

message("Exporting query 1: break conditions...")
df1 <- run_query("~/surf-analytics/sql/01_break_conditions.sql")
df1$`Forecast Time` <- format(as.POSIXct(df1$`Forecast Time`), "%Y-%m-%d %H:%M:%S")
write.csv(df1, "~/surf-analytics/exports/01_break_conditions.csv", row.names = FALSE)
message(paste0("  ✓ ", nrow(df1), " rows"))

message("Exporting query 2: surfability windows...")
df2 <- run_query("~/surf-analytics/sql/02_surfability_windows.sql")
write.csv(df2, "~/surf-analytics/exports/02_surfability_windows.csv", row.names = FALSE)
message(paste0("  ✓ ", nrow(df2), " rows"))

message("Exporting query 3: break rankings...")
df3 <- run_query("~/surf-analytics/sql/03_break_rankings.sql")
write.csv(df3, "~/surf-analytics/exports/03_break_rankings.csv", row.names = FALSE)
message(paste0("  ✓ ", nrow(df3), " rows"))

message("Exporting query 4: swell analysis...")
df4 <- run_query("~/surf-analytics/sql/04_swell_analysis.sql")
write.csv(df4, "~/surf-analytics/exports/04_swell_analysis.csv", row.names = FALSE)
message(paste0("  ✓ ", nrow(df4), " rows"))

message("Exporting query 5: accuracy decay...")
df5 <- run_query("~/surf-analytics/sql/05_accuracy_decay.sql")
write.csv(df5, "~/surf-analytics/exports/05_accuracy_decay.csv", row.names = FALSE)
message(paste0("  ✓ ", nrow(df5), " rows"))

message("All exports complete. Files saved to ~/surf-analytics/exports/")

system("cp ~/surf-analytics/exports/*.csv /mnt/c/Users/camde/Desktop/surfanalytics/")
message("CSVs copied to Desktop/surfanalytics")

message("Exporting query 5b: accuracy decay aggregated...")
df5b <- run_query("~/surf-analytics/sql/05b_accuracy_decay_agg.sql")
write.csv(df5b, "~/surf-analytics/exports/05b_accuracy_decay_agg.csv", row.names = FALSE)
message(paste0("  ✓ ", nrow(df5b), " rows"))

message("Exporting query 5c: accuracy decay overall...")
df5c <- run_query("~/surf-analytics/sql/05c_accuracy_decay_overall.sql")
write.csv(df5c, "~/surf-analytics/exports/05c_accuracy_decay_overall.csv", row.names = FALSE)