import os
import shutil
import psycopg2
import pandas as pd

SQL_DIR = os.path.expanduser("~/surf-analytics/sql")
EXPORT_DIR = os.path.expanduser("~/surf-analytics/exports")
DESKTOP_DIR = "/mnt/c/Users/camde/Desktop/surfanalytics"

con = psycopg2.connect(
    host="localhost", port=5432, dbname="surf_analytics",
    user="camdenjohnson", password="surfanalytics"
)

def run_query(filename):
    path = os.path.join(SQL_DIR, filename)
    with open(path) as f:
        sql = f.read()
    return pd.read_sql(sql, con)

try:
    print("Exporting query 1: break conditions...")
    df1 = run_query("01_break_conditions.sql")
    df1["Forecast Time"] = pd.to_datetime(df1["Forecast Time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df1.to_csv(f"{EXPORT_DIR}/01_break_conditions.csv", index=False)
    print(f"  ✓ {len(df1)} rows")

    print("Exporting query 2: surfability windows...")
    df2 = run_query("02_surfability_windows.sql")
    df2.to_csv(f"{EXPORT_DIR}/02_surfability_windows.csv", index=False)
    print(f"  ✓ {len(df2)} rows")

    print("Exporting query 3: break rankings...")
    df3 = run_query("03_break_rankings.sql")
    df3.to_csv(f"{EXPORT_DIR}/03_break_rankings.csv", index=False)
    print(f"  ✓ {len(df3)} rows")

    print("Exporting query 4: swell analysis...")
    df4 = run_query("04_swell_analysis.sql")
    df4.to_csv(f"{EXPORT_DIR}/04_swell_analysis.csv", index=False)
    print(f"  ✓ {len(df4)} rows")

    print("Exporting query 5: accuracy decay...")
    df5 = run_query("05_accuracy_decay.sql")
    df5.to_csv(f"{EXPORT_DIR}/05_accuracy_decay.csv", index=False)
    print(f"  ✓ {len(df5)} rows")

    print("Exporting query 5b: accuracy decay aggregated...")
    df5b = run_query("05b_accuracy_decay_agg.sql")
    df5b.to_csv(f"{EXPORT_DIR}/05b_accuracy_decay_agg.csv", index=False)
    print(f"  ✓ {len(df5b)} rows")

    print("Exporting query 5c: accuracy decay overall...")
    df5c = run_query("05c_accuracy_decay_overall.sql")
    df5c.to_csv(f"{EXPORT_DIR}/05c_accuracy_decay_overall.csv", index=False)
    print(f"  ✓ {len(df5c)} rows")

    print("All exports complete. Files saved to ~/surf-analytics/exports/")

    for f in os.listdir(EXPORT_DIR):
        if f.endswith(".csv"):
            shutil.copy(os.path.join(EXPORT_DIR, f), DESKTOP_DIR)
    print("CSVs copied to Desktop/surfanalytics")

finally:
    con.close()
