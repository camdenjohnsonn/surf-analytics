# Surf Forecast Accuracy Analysis

A data engineering and analytics project analyzing surf forecast accuracy 
across 16 breaks in San Diego County.

## Stack
- **R** — API ingestion, data pipeline
- **PostgreSQL** (Docker) — time-series forecast storage
- **SQL** — analytical queries
- **Tableau** — dashboard and visualization

## Data Source
Live forecast data from [Foamo](https://foamo.io) — a surf forecasting 
application covering San Diego County breaks.

## Project Structure
surf-analytics/
├── r/          # R ingestion and analysis scripts
├── sql/        # SQL analysis queries
├── exports/    # CSV exports for Tableau
└── data/       # Raw data (gitignored)


## Status
🟢 Data collection active — daily snapshots since April 26, 2026