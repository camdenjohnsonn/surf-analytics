import os
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/surf-analytics/.env"))

OWNER_API_KEY = os.getenv("OWNER_API_KEY", "")
BASE_URL = "https://api.foamo.io/forecast"

BREAKS = [
    (1,  "Pipes",            "pipes"),
    (2,  "Swamis",           "swamis"),
    (3,  "Grandview",        "grandview"),
    (4,  "Beacons",          "beacons"),
    (5,  "Cardiff Reef",     "cardiff%20reef"),
    (6,  "Seaside Reef",     "seaside%20reef"),
    (7,  "Oceanside Pier",   "oceanside%20pier"),
    (8,  "Oceanside Harbor", "oceanside%20harbor"),
    (9,  "Tamarack",         "tamarack"),
    (10, "Blacks",           "blacks"),
    (11, "Scripps Pier",     "scripps%20pier"),
    (12, "Windansea",        "windansea"),
    (13, "Tourmaline",       "tourmaline"),
    (14, "15th Street",      "15th%20street"),
    (15, "Ocean Beach",      "ocean%20beach"),
    (17, "Terramar",         "terramar"),
]

con = psycopg2.connect(
    host="localhost", port=5432, dbname="surf_analytics",
    user="camdenjohnson", password="surfanalytics"
)

def fetch_forecast(url_name):
    url = f"{BASE_URL}/{url_name}"
    resp = requests.get(url, headers={"x-owner-key": OWNER_API_KEY}, timeout=30)
    resp.raise_for_status()
    return resp.json()

def insert_forecast(cur, break_id, rows, collected):
    for row in rows:
        cur.execute("""
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
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            ON CONFLICT (break_id, forecast_for, collected_at) DO NOTHING
            RETURNING forecast_id
        """, (
            break_id, row["forecast_for"], collected,
            row.get("combined_wave_height_m"),
            row.get("wind_speed_10m_mps"), row.get("wind_dir_10m_deg"), row.get("wind_gusts_mps"),
            row.get("tide_height_ft"), row.get("tide_direction"), row.get("tide_rate_ft_per_hr"),
            row.get("water_temp_c"), row.get("air_temp_c"), row.get("uv_index"),
            row.get("cloud_cover_pct"), row.get("weather_code"), row.get("precip_probability_pct"),
            row.get("predicted_crowd"), row.get("sunrise_utc"), row.get("sunset_utc"),
            row.get("buoy_period_raw_s"), row.get("buoy_period_used"),
        ))

        result = cur.fetchone()
        if result is None:
            continue
        forecast_id = result[0]

        for s in range(1, 4):
            cur.execute("""
                INSERT INTO swells (forecast_id, swell_number, height_m, period_s, dir_deg)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (forecast_id, swell_number) DO NOTHING
            """, (
                forecast_id, s,
                row.get(f"swell{s}_height_m"),
                row.get(f"swell{s}_period_s"),
                row.get(f"swell{s}_dir_deg"),
            ))

        cur.execute("""
            INSERT INTO scores (
                forecast_id,
                score_swell_height, score_swell_period, score_swell_direction,
                score_wind, score_tide,
                predicted_rating, ml_predicted_rating, blended_predicted_rating,
                predicted_wave_height_ft_min, predicted_wave_height_ft_max,
                ml_wave_height_ft, blended_wave_height_ft
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (forecast_id) DO NOTHING
        """, (
            forecast_id,
            row.get("score_swell_height"), row.get("score_swell_period"), row.get("score_swell_direction"),
            row.get("score_wind"), row.get("score_tide"),
            row.get("predicted_rating"), row.get("ml_predicted_rating"), row.get("blended_predicted_rating"),
            row.get("predicted_wave_height_ft_min"), row.get("predicted_wave_height_ft_max"),
            row.get("ml_wave_height_ft"), row.get("blended_wave_height_ft"),
        ))

try:
    with con:
        with con.cursor() as cur:
            now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

            cur.execute("SELECT COUNT(*) FROM breaks")
            if cur.fetchone()[0] == 0:
                print("Populating breaks table...")
                for break_id, name, _ in BREAKS:
                    cur.execute(
                        "INSERT INTO breaks (break_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (break_id, name)
                    )

            for i, (break_id, name, url_name) in enumerate(BREAKS, 1):
                print(f"Fetching: {name} ({i}/{len(BREAKS)})")
                try:
                    rows = fetch_forecast(url_name)
                    insert_forecast(cur, break_id, rows, now_utc)
                    print(f"  ✓ {len(rows)} rows inserted")
                except Exception as e:
                    print(f"  ✗ Failed: {e}")

    print("Done.")
finally:
    con.close()
