.libPaths(c("~/R/library", .libPaths()))

library(DBI)
library(RPostgres)
library(httr2)
library(jsonlite)
library(dplyr)
library(lubridate)



breaks <- tibble(
  break_id = c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17),
  name = c(
    "Pipes", "Swamis", "Grandview", "Beacons",
    "Cardiff Reef", "Seaside Reef", "Oceanside Pier",
    "Oceanside Harbor", "Tamarack", "Blacks",
    "Scripps Pier", "Windansea", "Tourmaline",
    "15th Street", "Ocean Beach", "Terramar"
  ),
  url_name = c(
    "pipes", "swamis", "grandview", "beacons",
    "cardiff%20reef", "seaside%20reef", "oceanside%20pier",
    "oceanside%20harbor", "tamarack", "blacks",
    "scripps%20pier", "windansea", "tourmaline",
    "15th%20street", "ocean%20beach", "terramar"
  )
)


base_url <- "https://api.foamo.io/forecast"


con <- dbConnect(
  RPostgres::Postgres(),
  host     = "localhost",
  port     = 5432,
  dbname   = "surf_analytics",
  user     = "camdenjohnson",
  password = "surfanalytics"
)


on.exit(dbDisconnect(con), add = TRUE)

fetch_forecast <- function(url_name) {
  
  url <- paste0(base_url, "/", url_name)

  response <- request(url) |>
    req_timeout(30) |>
    req_retry(max_tries = 3) |>
    req_perform()

  raw <- response |>
    resp_body_string() |>
    fromJSON(flatten = TRUE)
  
  return(raw)
}

insert_forecast <- function(con, break_id, raw) {
  
collected <- floor_date(now(tzone = "UTC"), unit = "hour")



  for (i in 1:nrow(raw)) {
    row <- raw[i, ]
    
    forecast_id <- dbGetQuery(con, "
      INSERT INTO forecasts (
        break_id, forecast_for, collected_at,
        combined_wave_height_m,
        wind_speed_10m_mps, wind_dir_10m_deg, wind_gusts_mps,
        tide_height_ft, tide_direction, tide_rate_ft_per_hr,
        water_temp_c, air_temp_c, uv_index,
        cloud_cover_pct, weather_code, precip_probability_pct,
        predicted_crowd, sunrise_utc, sunset_utc,
        buoy_period_raw_s, buoy_period_used
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
      )
      ON CONFLICT (break_id, forecast_for, collected_at) DO NOTHING
      RETURNING forecast_id
    ",
    params = list(
      break_id,
      row$forecast_for,
      collected,
      row$combined_wave_height_m,
      row$wind_speed_10m_mps, row$wind_dir_10m_deg, row$wind_gusts_mps,
      row$tide_height_ft, row$tide_direction, row$tide_rate_ft_per_hr,
      row$water_temp_c, row$air_temp_c, row$uv_index,
      row$cloud_cover_pct, row$weather_code, row$precip_probability_pct,
      row$predicted_crowd, row$sunrise_utc, row$sunset_utc,
      row$buoy_period_raw_s, row$buoy_period_used
    ))
    
    if (nrow(forecast_id) == 0) next
    fid <- forecast_id$forecast_id[1]
    
    for (s in 1:3) {
      dbExecute(con, "
        INSERT INTO swells (forecast_id, swell_number, height_m, period_s, dir_deg)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (forecast_id, swell_number) DO NOTHING
      ",
      params = list(
        fid, s,
        row[[paste0("swell", s, "_height_m")]],
        row[[paste0("swell", s, "_period_s")]],
        row[[paste0("swell", s, "_dir_deg")]]
      ))
    }
    
    dbExecute(con, "
      INSERT INTO scores (
        forecast_id,
        score_swell_height, score_swell_period, score_swell_direction,
        score_wind, score_tide,
        predicted_rating, ml_predicted_rating, blended_predicted_rating,
        predicted_wave_height_ft_min, predicted_wave_height_ft_max,
        ml_wave_height_ft, blended_wave_height_ft
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
      )
      ON CONFLICT (forecast_id) DO NOTHING
    ",
    params = list(
      fid,
      row$score_swell_height, row$score_swell_period, row$score_swell_direction,
      row$score_wind, row$score_tide,
      row$predicted_rating, row$ml_predicted_rating, row$blended_predicted_rating,
      row$predicted_wave_height_ft_min, row$predicted_wave_height_ft_max,
      row$ml_wave_height_ft, row$blended_wave_height_ft
    ))
  }
}

existing_breaks <- dbGetQuery(con, "SELECT break_id FROM breaks")

if (nrow(existing_breaks) == 0) {
  message("Populating breaks table...")
  dbWriteTable(con, "breaks", 
    breaks |> select(break_id, name),
    append = TRUE,
    row.names = FALSE
  )
}

for (i in 1:nrow(breaks)) {
  
  break_id <- breaks$break_id[i]
  url_name <- breaks$url_name[i]
  name     <- breaks$name[i]
  
  message(paste0("Fetching: ", name, " (", i, "/", nrow(breaks), ")"))
  
  tryCatch({
    raw <- fetch_forecast(url_name)
    insert_forecast(con, break_id, raw)
    message(paste0("  ✓ ", nrow(raw), " rows inserted"))
  }, error = function(e) {
    message(paste0("  ✗ Failed: ", e$message))
  })
}

message("Done.")