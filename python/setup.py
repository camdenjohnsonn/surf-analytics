import os
import psycopg2

SCHEMA_PATH = os.path.expanduser("~/surf-analytics/sql/schema.sql")

BREAKS = [
    {"break_id": 1,  "name": "Pipes",            "country": "USA, California, San Diego County", "break_type": "beach", "lat": 33.027659,  "lng": -117.288572, "break_depth_m": 6,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 13, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 320, "min_wave_height_ft": 1.8, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 2,  "name": "Swamis",           "country": "USA, California, San Diego County", "break_type": "reef",  "lat": 33.034528,  "lng": -117.294066, "break_depth_m": 5,   "height_gate_factor": 0.70, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 90,  "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 330, "min_wave_height_ft": 1.6, "max_wave_height_ft": 10.0, "beach_orientation_deg": 250, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 3,  "name": "Grandview",        "country": "USA, California, San Diego County", "break_type": "beach", "lat": 33.076396,  "lng": -117.310631, "break_depth_m": 6,   "height_gate_factor": 1.00, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 1.5, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 210, "ideal_swell_dir_max": 330, "min_wave_height_ft": 2.0, "max_wave_height_ft": 10.0, "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 4,  "name": "Beacons",          "country": "USA, California, San Diego County", "break_type": "beach", "lat": 33.064898,  "lng": -117.306050, "break_depth_m": 6,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 240, "ideal_swell_dir_max": 320, "min_wave_height_ft": 1.8, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 5,  "name": "Cardiff Reef",     "country": "USA, California, San Diego County", "break_type": "reef",  "lat": 33.014422,  "lng": -117.282378, "break_depth_m": 6,   "height_gate_factor": 0.80, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 340, "min_wave_height_ft": 1.8, "max_wave_height_ft": 10.0, "beach_orientation_deg": 260, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 6,  "name": "Seaside Reef",     "country": "USA, California, San Diego County", "break_type": "reef",  "lat": 33.001662,  "lng": -117.279705, "break_depth_m": 5,   "height_gate_factor": 0.80, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 90,  "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 320, "min_wave_height_ft": 1.8, "max_wave_height_ft": 10.0, "beach_orientation_deg": 260, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 7,  "name": "Oceanside Pier",   "country": "USA, California, San Diego County", "break_type": "beach", "lat": 33.193880,  "lng": -117.387592, "break_depth_m": 6,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.0, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 90,  "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 200, "ideal_swell_dir_max": 340, "min_wave_height_ft": 1.8, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 250, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 8,  "name": "Oceanside Harbor", "country": "USA, California, San Diego County", "break_type": "beach", "lat": 33.204796,  "lng": -117.397752, "break_depth_m": 6,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.0, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 90,  "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 200, "ideal_swell_dir_max": 340, "min_wave_height_ft": 1.8, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 255, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 9,  "name": "Tamarack",         "country": "USA, California, San Diego County", "break_type": "beach", "lat": 33.149979,  "lng": -117.349457, "break_depth_m": 6,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.0, "ideal_period_min_s": 12, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 200, "ideal_swell_dir_max": 340, "min_wave_height_ft": 1.8, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 10, "name": "Blacks",           "country": "USA, California, San Diego County", "break_type": "beach", "lat": 32.887226,  "lng": -117.256271, "break_depth_m": 6,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 0.0, "ideal_tide_max_ft": 1.5, "ideal_period_min_s": 13, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 10, "ideal_swell_dir_min": 200, "ideal_swell_dir_max": 320, "min_wave_height_ft": 2.0, "max_wave_height_ft": 9.0,  "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 11, "name": "Scripps Pier",     "country": None,                                "break_type": "beach", "lat": 32.865851,  "lng": -117.256462, "break_depth_m": 3.5, "height_gate_factor": 0.85, "ideal_tide_min_ft": 1.5, "ideal_tide_max_ft": 4.0, "ideal_period_min_s": 10, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 250, "ideal_swell_dir_max": 340, "min_wave_height_ft": 1.8, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 270, "primary_buoy": "sc",    "tide_station": "9410230"},
    {"break_id": 12, "name": "Windansea",        "country": "USA, California, San Diego County", "break_type": "reef",  "lat": 32.829183,  "lng": -117.282127, "break_depth_m": 4,   "height_gate_factor": 0.85, "ideal_tide_min_ft": 0.0, "ideal_tide_max_ft": 1.0, "ideal_period_min_s": 12, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 120, "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 320, "min_wave_height_ft": 1.8, "max_wave_height_ft": 7.0,  "beach_orientation_deg": 265, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 13, "name": "Tourmaline",       "country": "USA, California, San Diego County", "break_type": "reef",  "lat": 32.804320,  "lng": -117.265144, "break_depth_m": 4,   "height_gate_factor": 0.85, "ideal_tide_min_ft": 0.0, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 10, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 120, "ideal_wind_max_kts": 15, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 320, "min_wave_height_ft": 2.2, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 14, "name": "15th Street",      "country": "USA, California, San Diego County", "break_type": "beach", "lat": 32.958512,  "lng": -117.269651, "break_depth_m": 5,   "height_gate_factor": 0.85, "ideal_tide_min_ft": 0.0, "ideal_tide_max_ft": 2.0, "ideal_period_min_s": 10, "ideal_wind_dir_min": 50,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 320, "min_wave_height_ft": 2.0, "max_wave_height_ft": 10.0, "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 15, "name": "Ocean Beach",      "country": "USA, California, San Diego County", "break_type": "beach", "lat": 32.751277,  "lng": -117.254525, "break_depth_m": 5,   "height_gate_factor": 0.90, "ideal_tide_min_ft": 1.0, "ideal_tide_max_ft": 3.0, "ideal_period_min_s": 10, "ideal_wind_dir_min": 60,  "ideal_wind_dir_max": 100, "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 220, "ideal_swell_dir_max": 320, "min_wave_height_ft": 2.0, "max_wave_height_ft": 8.0,  "beach_orientation_deg": 270, "primary_buoy": "46086", "tide_station": "9410230"},
    {"break_id": 17, "name": "Terramar",         "country": "USA, California, San Diego County", "break_type": "reef",  "lat": 33.128204,  "lng": -117.334707, "break_depth_m": 5,   "height_gate_factor": 0.70, "ideal_tide_min_ft": 0.5, "ideal_tide_max_ft": 2.5, "ideal_period_min_s": 12, "ideal_wind_dir_min": 40,  "ideal_wind_dir_max": 90,  "ideal_wind_max_kts": 12, "ideal_swell_dir_min": 210, "ideal_swell_dir_max": 330, "min_wave_height_ft": 1.6, "max_wave_height_ft": 10.0, "beach_orientation_deg": 250, "primary_buoy": "46086", "tide_station": "9410230"},
]

con = psycopg2.connect(
    host="localhost", port=5432, dbname="surf_analytics",
    user="camdenjohnson", password="surfanalytics"
)

try:
    with con:
        with con.cursor() as cur:
            with open(SCHEMA_PATH) as f:
                cur.execute(f.read())
            print("Tables created (or already exist)")

            for b in BREAKS:
                cur.execute("""
                    INSERT INTO breaks (break_id, name)
                    VALUES (%(break_id)s, %(name)s)
                    ON CONFLICT (break_id) DO NOTHING
                """, b)
                cur.execute("""
                    UPDATE breaks SET
                        country               = %(country)s,
                        break_type            = %(break_type)s,
                        lat                   = %(lat)s,
                        lng                   = %(lng)s,
                        break_depth_m         = %(break_depth_m)s,
                        height_gate_factor    = %(height_gate_factor)s,
                        ideal_tide_min_ft     = %(ideal_tide_min_ft)s,
                        ideal_tide_max_ft     = %(ideal_tide_max_ft)s,
                        ideal_period_min_s    = %(ideal_period_min_s)s,
                        ideal_wind_dir_min    = %(ideal_wind_dir_min)s,
                        ideal_wind_dir_max    = %(ideal_wind_dir_max)s,
                        ideal_wind_max_kts    = %(ideal_wind_max_kts)s,
                        ideal_swell_dir_min   = %(ideal_swell_dir_min)s,
                        ideal_swell_dir_max   = %(ideal_swell_dir_max)s,
                        min_wave_height_ft    = %(min_wave_height_ft)s,
                        max_wave_height_ft    = %(max_wave_height_ft)s,
                        beach_orientation_deg = %(beach_orientation_deg)s,
                        primary_buoy          = %(primary_buoy)s,
                        tide_station          = %(tide_station)s
                    WHERE break_id = %(break_id)s
                """, b)

            print("Breaks populated/updated")

    with con.cursor() as cur:
        cur.execute("SELECT break_id, name, break_type, lat, lng FROM breaks ORDER BY break_id")
        rows = cur.fetchall()
        print(f"\n{'ID':<5} {'Name':<20} {'Type':<8} {'Lat':<12} Lng")
        for r in rows:
            print(f"{r[0]:<5} {r[1]:<20} {r[2]:<8} {r[3]:<12} {r[4]}")
finally:
    con.close()
