SELECT
    b.name                          AS "Break",
    b.break_type                    AS "Break Type",
    ROUND(AVG(s.blended_predicted_rating), 2) AS "Avg Rating",
    ROUND(AVG(s.blended_wave_height_ft), 2)   AS "Avg Wave Height (ft)",
    RANK() OVER (ORDER BY AVG(s.blended_predicted_rating) DESC) AS "Rank"
FROM forecasts f
JOIN breaks b ON f.break_id = b.break_id
JOIN scores s ON s.forecast_id = f.forecast_id
WHERE f.collected_at = (SELECT MAX(collected_at) FROM forecasts)
AND f.forecast_for BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
GROUP BY b.name, b.break_type
ORDER BY "Rank";