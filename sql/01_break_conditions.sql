SELECT 
    b.name                    AS "Break",
    f.forecast_for            AS "Forecast Time",
    f.combined_wave_height_m  AS "Wave Height (m)",
    f.wind_speed_10m_mps      AS "Wind Speed (mps)",
    f.tide_height_ft          AS "Tide Height (ft)",
    s.blended_predicted_rating AS "Blended Rating",
    f.predicted_crowd         AS "Predicted Crowd",
    f.air_temp_c              AS "Air Temp (C)",
    f.water_temp_c            AS "Water Temp (C)",
    f.cloud_cover_pct         AS "Cloud Cover",
    f.precip_probability_pct  AS "Chance of Rain",
    f.uv_index                AS "UV Index",
CASE 
    WHEN f.tide_height_ft BETWEEN b.ideal_tide_min_ft AND b.ideal_tide_max_ft
    AND f.wind_speed_10m_mps * 1.94384 <= b.ideal_wind_max_kts
    AND f.wind_dir_10m_deg BETWEEN b.ideal_wind_dir_min AND b.ideal_wind_dir_max
    THEN 'Yes'
    ELSE 'No'
END AS "In Ideal Window"
FROM forecasts f
JOIN breaks b ON f.break_id = b.break_id
JOIN scores s ON s.forecast_id = f.forecast_id
WHERE EXTRACT(HOUR FROM f.forecast_for AT TIME ZONE 'America/Los_Angeles') BETWEEN 6 AND 20 AND f.collected_at = (SELECT MAX(collected_at) FROM forecasts)
ORDER BY f.forecast_for, b.name;