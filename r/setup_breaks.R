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

breaks_data <- data.frame(
  break_id               = c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17),
  name                   = c("Pipes","Swamis","Grandview","Beacons","Cardiff Reef",
                             "Seaside Reef","Oceanside Pier","Oceanside Harbor",
                             "Tamarack","Blacks","Scripps Pier","Windansea",
                             "Tourmaline","15th Street","Ocean Beach","Terramar"),
  country                = c(rep("USA, California, San Diego County", 10),
                             NA,
                             rep("USA, California, San Diego County", 5)),
  break_type             = c("beach","reef","beach","beach","reef","reef","beach",
                             "beach","beach","beach","beach","reef","reef","beach",
                             "beach","reef"),
  lat                    = c(33.027659,33.034528,33.076396,33.064898,33.014422,
                             33.001662,33.193880,33.204796,33.149979,32.887226,
                             32.865851,32.829183,32.804320,32.958512,32.751277,
                             33.128204),
  lng                    = c(-117.288572,-117.294066,-117.310631,-117.306050,
                             -117.282378,-117.279705,-117.387592,-117.397752,
                             -117.349457,-117.256271,-117.256462,-117.282127,
                             -117.265144,-117.269651,-117.254525,-117.334707),
  break_depth_m          = c(6,5,6,6,6,5,6,6,6,6,3.5,4,4,5,5,5),
  height_gate_factor     = c(0.9,0.7,1.0,0.9,0.8,0.8,0.9,0.9,0.9,0.9,0.85,
                             0.85,0.85,0.85,0.9,0.7),
  ideal_tide_min_ft      = c(0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.0,1.5,
                             0.0,0.0,0.0,1.0,0.5),
  ideal_tide_max_ft      = c(2.5,2.5,1.5,2.5,2.5,2.5,2.0,2.0,2.0,1.5,4.0,
                             1.0,2.5,2.0,3.0,2.5),
  ideal_period_min_s     = c(13,12,12,12,12,12,12,12,12,13,10,12,10,10,10,12),
  ideal_wind_dir_min     = c(60,40,40,40,40,40,40,40,60,60,60,60,60,50,60,40),
  ideal_wind_dir_max     = c(100,90,100,100,100,90,90,90,100,100,100,120,120,
                             100,100,90),
  ideal_wind_max_kts     = c(10,12,10,10,12,12,10,10,10,10,12,12,15,12,12,12),
  ideal_swell_dir_min    = c(220,220,210,240,220,220,200,200,200,200,250,220,
                             220,220,220,210),
  ideal_swell_dir_max    = c(320,330,330,320,340,320,340,340,340,320,340,320,
                             320,320,320,330),
  min_wave_height_ft     = c(1.8,1.6,2.0,1.8,1.8,1.8,1.8,1.8,1.8,2.0,1.8,
                             1.8,2.2,2.0,2.0,1.6),
  max_wave_height_ft     = c(8.0,10.0,10.0,8.0,10.0,10.0,8.0,8.0,8.0,9.0,
                             8.0,7.0,8.0,10.0,8.0,10.0),
  beach_orientation_deg  = c(270,250,270,270,260,260,250,255,270,270,270,265,
                             270,270,270,250),
  primary_buoy           = c("46086","46086","46086","46086","46086","46086",
                             "46086","46086","46086","46086","sc","46086",
                             "46086","46086","46086","46086"),
  tide_station           = rep("9410230", 16)
)

# Update each break row with full metadata
for (i in 1:nrow(breaks_data)) {
  dbExecute(con, "
    UPDATE breaks SET
      country               = $1,
      break_type            = $2,
      lat                   = $3,
      lng                   = $4,
      break_depth_m         = $5,
      height_gate_factor    = $6,
      ideal_tide_min_ft     = $7,
      ideal_tide_max_ft     = $8,
      ideal_period_min_s    = $9,
      ideal_wind_dir_min    = $10,
      ideal_wind_dir_max    = $11,
      ideal_wind_max_kts    = $12,
      ideal_swell_dir_min   = $13,
      ideal_swell_dir_max   = $14,
      min_wave_height_ft    = $15,
      max_wave_height_ft    = $16,
      beach_orientation_deg = $17,
      primary_buoy          = $18,
      tide_station          = $19
    WHERE break_id = $20
  ", params = list(
    breaks_data$country[i],
    breaks_data$break_type[i],
    breaks_data$lat[i],
    breaks_data$lng[i],
    breaks_data$break_depth_m[i],
    breaks_data$height_gate_factor[i],
    breaks_data$ideal_tide_min_ft[i],
    breaks_data$ideal_tide_max_ft[i],
    breaks_data$ideal_period_min_s[i],
    breaks_data$ideal_wind_dir_min[i],
    breaks_data$ideal_wind_dir_max[i],
    breaks_data$ideal_wind_max_kts[i],
    breaks_data$ideal_swell_dir_min[i],
    breaks_data$ideal_swell_dir_max[i],
    breaks_data$min_wave_height_ft[i],
    breaks_data$max_wave_height_ft[i],
    breaks_data$beach_orientation_deg[i],
    breaks_data$primary_buoy[i],
    breaks_data$tide_station[i],
    breaks_data$break_id[i]
  ))
}

message("Updated 16 breaks with full metadata")

result <- dbGetQuery(con, "
  SELECT break_id, name, break_type, lat, lng 
  FROM breaks 
  ORDER BY break_id
")
print(result)