# Surf Forecast Analytics — San Diego County

A data engineering and analytics project analyzing surf forecast accuracy 
and conditions across 16 breaks in San Diego County.

## Live Dashboard
[View on Tableau Public](https://public.tableau.com/app/profile/camden.johnson2731/viz/surf-analytics/SurfForecastAnalytics-SanDiegoCounty)

## Stack
- **R** — API ingestion pipeline (httr2, RPostgres)
- **PostgreSQL** (Docker) — time-series forecast storage
- **SQL** — analytical queries (CTEs, window functions)
- **Tableau** — dashboard and visualization

## Data Source
Live forecast data from [Foamo](https://foamo.io) — a surf forecasting 
platform covering 16 San Diego County breaks. Data collected daily via 
automated cron job.

## Database Schema
Four normalized tables: `breaks`, `forecasts`, `swells`, `scores`
- 16 surf breaks from Oceanside to Ocean Beach
- Hourly forecast snapshots collected daily
- 3,000+ rows ingested per collection

## Key Findings
- Wind direction is the primary limiting factor for ideal surf conditions 
  across all 16 breaks during late April/May — persistent SW onshore wind 
  averages 230-260° against ideal offshore range of 40-100°
- Cardiff Reef ranks highest by blended predicted rating for the current 
  forecast window
- Forecast accuracy decay analysis shows prediction error decreasing as 
  the forecast horizon approaches — collection ongoing to build full decay curve

## Project Structure

```
surf-analytics/
├── r/
│   ├── ingest.R          # Daily API ingestion script
│   ├── export.R          # CSV export for Tableau
│   └── setup_breaks.R    # One-time breaks table setup
├── sql/
│   ├── 01_break_conditions.sql
│   ├── 02_surfability_windows.sql
│   ├── 03_break_rankings.sql
│   ├── 04_swell_analysis.sql
│   ├── 05_accuracy_decay.sql
│   ├── 05b_accuracy_decay_agg.sql
│   └── 05c_accuracy_decay_overall.sql
├── exports/              # CSV outputs for Tableau (gitignored)
└── data/                 # Logs (gitignored)
```

## Status
🟢 Data collection active — daily snapshots since April 26, 2026