SELECT
    b.name                          AS "Break",
    EXTRACT(HOUR FROM f.forecast_for AT TIME ZONE 'America/Los_Angeles') AS "Hour",
    COUNT(*)                        AS "Total Forecasts",
SUM(CASE WHEN f.tide_height_ft BETWEEN b.ideal_tide_min_ft AND b.ideal_tide_max_ft THEN 1 ELSE 0 END) AS "Tide OK Count",
SUM(CASE WHEN f.wind_speed_10m_mps * 1.94384 <= b.ideal_wind_max_kts THEN 1 ELSE 0 END) AS "Wind Speed OK Count",
SUM(CASE WHEN f.wind_dir_10m_deg BETWEEN b.ideal_wind_dir_min AND b.ideal_wind_dir_max THEN 1 ELSE 0 END) AS "Wind Dir OK Count",
ROUND(SUM(CASE WHEN f.tide_height_ft BETWEEN b.ideal_tide_min_ft AND b.ideal_tide_max_ft THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 3) AS "Tide Rate",
ROUND(SUM(CASE WHEN f.wind_speed_10m_mps * 1.94384 <= b.ideal_wind_max_kts THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 3) AS "Wind Speed Rate",
ROUND(SUM(CASE WHEN f.wind_dir_10m_deg BETWEEN b.ideal_wind_dir_min AND b.ideal_wind_dir_max THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 3) AS "Wind Dir Rate"
FROM forecasts f
JOIN breaks b ON f.break_id = b.break_id
JOIN scores s ON s.forecast_id = f.forecast_id
WHERE EXTRACT(HOUR FROM f.forecast_for AT TIME ZONE 'America/Los_Angeles') BETWEEN 6 AND 20
GROUP BY b.name, "Hour"
ORDER BY b.name, "Hour";