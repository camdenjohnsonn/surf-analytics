WITH snapshots AS (
    SELECT
        b.name                          AS break_name,
        f.forecast_for,
        f.collected_at,
        s.blended_predicted_rating      AS rating,
        ROUND(EXTRACT(EPOCH FROM (f.forecast_for - f.collected_at)) / 86400, 1) AS days_out,
        LAST_VALUE(s.blended_predicted_rating) OVER (
            PARTITION BY f.break_id, f.forecast_for
            ORDER BY f.collected_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS latest_rating
    FROM forecasts f
    JOIN breaks b ON f.break_id = b.break_id
    JOIN scores s ON s.forecast_id = f.forecast_id
)
SELECT
    break_name                          AS "Break",
    forecast_for                        AS "Forecast For",
    collected_at                        AS "Collected At",
    days_out                            AS "Days Out",
    rating                              AS "Predicted Rating",
    latest_rating                       AS "Latest Rating",
    ROUND(ABS(rating - latest_rating), 3) AS "Absolute Error",
    LAG(rating) OVER (
        PARTITION BY break_name, forecast_for
        ORDER BY collected_at
    )                                   AS "Previous Rating",
    ROUND(rating - LAG(rating) OVER (
        PARTITION BY break_name, forecast_for
        ORDER BY collected_at
    ), 3)                               AS "Rating Change"
FROM snapshots
ORDER BY break_name, forecast_for, collected_at;