SELECT
    b.name                          AS "Break",
    sw.swell_number                 AS "Dominant Swell",
    sw.height_m                     AS "Dominant Height (m)",
    sw.period_s                     AS "Dominant Period (s)",
    sw.dir_deg                      AS "Dominant Direction",
    CASE 
        WHEN sw.period_s >= b.ideal_period_min_s THEN 'Yes' 
        ELSE 'No' 
    END                             AS "Meets Min Period",
    AVG(sw.period_s) OVER (PARTITION BY f.forecast_id) AS "Avg Period All Swells"
FROM forecasts f
JOIN breaks b ON f.break_id = b.break_id
JOIN scores s ON s.forecast_id = f.forecast_id
JOIN swells sw ON sw.forecast_id = f.forecast_id
WHERE f.collected_at = (SELECT MAX(collected_at) FROM forecasts)
AND f.forecast_for BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
AND sw.swell_number = (
    SELECT swell_number 
    FROM swells 
    WHERE forecast_id = f.forecast_id 
    ORDER BY height_m DESC 
    LIMIT 1
)
ORDER BY b.name, f.forecast_for;