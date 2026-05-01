WITH snapshots AS (
    SELECT
        b.name AS break_name,
        f.forecast_for,
        f.collected_at,
        s.blended_predicted_rating AS rating,
        ROUND(EXTRACT(EPOCH FROM (f.forecast_for - f.collected_at)) / 86400, 1) AS days_out,
        LAST_VALUE(s.blended_predicted_rating) OVER (
            PARTITION BY f.break_id, f.forecast_for
            ORDER BY f.collected_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_rating
    FROM forecasts f
    JOIN breaks b ON f.break_id = b.break_id
    JOIN scores s ON s.forecast_id = f.forecast_id
),
errors AS (
    SELECT
        break_name,
        days_out,
        rating,
        ROUND(ABS(rating - latest_rating), 3) AS absolute_error
    FROM snapshots
)
SELECT
    break_name                    AS "Break",
    days_out                      AS "Days Out",
    ROUND(AVG(rating), 2)         AS "Avg Predicted Rating",
    ROUND(AVG(absolute_error), 3) AS "Avg Absolute Error",
    COUNT(*)                      AS "Forecast Count"
FROM errors
GROUP BY break_name, days_out
ORDER BY break_name, days_out DESC;