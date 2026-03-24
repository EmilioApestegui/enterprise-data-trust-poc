# Enterprise Data Trust Workbench

A static Streamlit PoC designed to demonstrate the three pain points discussed for a Director-level enterprise data / analytics / integration role:

1. **Data trust** — competing KPI definitions and no single source of truth.
2. **Integration reliability** — late / failed feeds and incident visibility.
3. **Analytics adoption** — trusted KPIs and measurable manual work reduction.

## What is included

- `app.py` — Streamlit app
- `data/enterprise_data_trust_poc_data.xlsx` — downloadable static sample workbook
- `scripts/bootstrap_postgres.py` — creates the database and loads all tables into Postgres
- `scripts/create_static_data.py` — confirms where the packaged static data lives
- `notebooks/01_data_setup.ipynb` — lightweight notebook walkthrough
- `enterprise_data_trust_poc/` — shared config and database logic

## Target Postgres setup

The app and scripts are wired for the credentials you provided by default:

- host: `localhost`
- port: `5432`
- admin database: `postgres`
- user: `postgres`
- password: `postgres`
- target database: `enterprise_data_trust_poc`

## Quick start

```bash
pip install -r requirements.txt
python scripts/bootstrap_postgres.py
python -m streamlit run app.py
```

## What the app shows

### 1) Problem / Before State
A deliberately conflicting KPI submission table where Finance, Merchandising, and Supply Chain report different revenue values.

### 2) Governance & Certification
A KPI registry that shows owner, steward, definition, source object, consumer group, and certification status.

### 3) Integration & Lineage
Feed health, incidents, and a simple lineage graph from source systems to certified executive KPIs.

### 4) Data Quality
Rule results with intentional issues:
- one missing plan value
- one late / missing LATAM feed
- one returns outlier
- one KPI with no owner

### 5) Executive View
A trusted pipeline that merges actuals and plans, calculates variance, and exports a certified summary.

### 6) Adoption & Value
Static monthly adoption metrics to show business value, not just technical delivery.

## Default desktop exports

When the app runs locally on the intended Windows machine, the executive summary export defaults to:

- `C:\Users\emili\OneDrive\Desktop\certified_sales_summary.xlsx`
- `C:\Users\emili\OneDrive\Desktop\certified_sales_summary.csv`

If that path does not exist, the app falls back to a local `desktop_exports` folder.
