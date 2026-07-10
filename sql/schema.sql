CREATE TABLE IF NOT EXISTS breaks (
    break_id              INTEGER PRIMARY KEY,
    name                  TEXT NOT NULL,
    country               TEXT,
    break_type            TEXT,
    lat                   NUMERIC,
    lng                   NUMERIC,
    break_depth_m         NUMERIC,
    height_gate_factor    NUMERIC,
    ideal_tide_min_ft     NUMERIC,
    ideal_tide_max_ft     NUMERIC,
    ideal_period_min_s    NUMERIC,
    ideal_wind_dir_min    INTEGER,
    ideal_wind_dir_max    INTEGER,
    ideal_wind_max_kts    NUMERIC,
    ideal_swell_dir_min   INTEGER,
    ideal_swell_dir_max   INTEGER,
    min_wave_height_ft    NUMERIC,
    max_wave_height_ft    NUMERIC,
    beach_orientation_deg INTEGER,
    primary_buoy          TEXT,
    tide_station          TEXT
);

CREATE TABLE IF NOT EXISTS forecasts (
    forecast_id             BIGSERIAL PRIMARY KEY,
    break_id                INTEGER NOT NULL REFERENCES breaks(break_id),
    forecast_for            TIMESTAMPTZ NOT NULL,
    collected_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    combined_wave_height_m  NUMERIC,
    wind_speed_10m_mps      NUMERIC,
    wind_dir_10m_deg        NUMERIC,
    wind_gusts_mps          NUMERIC,
    tide_height_ft          NUMERIC,
    tide_direction          TEXT,
    tide_rate_ft_per_hr     NUMERIC,
    water_temp_c            NUMERIC,
    air_temp_c              NUMERIC,
    uv_index                NUMERIC,
    cloud_cover_pct         INTEGER,
    weather_code            INTEGER,
    precip_probability_pct  INTEGER,
    predicted_crowd         TEXT,
    sunrise_utc             TIMESTAMPTZ,
    sunset_utc              TIMESTAMPTZ,
    buoy_period_raw_s       NUMERIC,
    buoy_period_used        BOOLEAN,
    UNIQUE (break_id, forecast_for, collected_at)
);

CREATE TABLE IF NOT EXISTS swells (
    swell_id     BIGSERIAL PRIMARY KEY,
    forecast_id  BIGINT NOT NULL REFERENCES forecasts(forecast_id),
    swell_number INTEGER NOT NULL,
    height_m     NUMERIC,
    period_s     NUMERIC,
    dir_deg      NUMERIC,
    UNIQUE (forecast_id, swell_number)
);

CREATE TABLE IF NOT EXISTS scores (
    score_id                    BIGSERIAL PRIMARY KEY,
    forecast_id                 BIGINT NOT NULL REFERENCES forecasts(forecast_id),
    score_swell_height          NUMERIC,
    score_swell_period          NUMERIC,
    score_swell_direction       NUMERIC,
    score_wind                  NUMERIC,
    score_tide                  NUMERIC,
    predicted_rating            NUMERIC,
    ml_predicted_rating         NUMERIC,
    blended_predicted_rating    NUMERIC,
    predicted_wave_height_ft_min NUMERIC,
    predicted_wave_height_ft_max NUMERIC,
    ml_wave_height_ft           NUMERIC,
    blended_wave_height_ft      NUMERIC,
    UNIQUE (forecast_id)
);
