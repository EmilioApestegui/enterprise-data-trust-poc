
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from .config import TARGET_DB, WORKBOOK_PATH, export_paths

TABLE_LOAD_ORDER = [
    "sales_actuals",
    "sales_plan",
    "kpi_submissions",
    "kpi_registry",
    "feed_monitor",
    "integration_incidents",
    "data_quality_results",
    "issue_log",
    "analytics_adoption",
]


def admin_engine(host: str, port: str, admin_db: str, user: str, password: str) -> Engine:
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{admin_db}",
        isolation_level="AUTOCOMMIT",
        future=True,
    )


def app_engine(host: str, port: str, user: str, password: str, db_name: str = TARGET_DB) -> Engine:
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}",
        future=True,
    )


def test_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def ensure_database(host: str, port: str, admin_db: str, user: str, password: str, target_db: str = TARGET_DB) -> str:
    engine = admin_engine(host, port, admin_db, user, password)
    test_connection(engine)
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {"db_name": target_db},
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{target_db}"'))
            return f'Created database "{target_db}".'
    return f'Database "{target_db}" already exists.'


def load_workbook_to_postgres(engine: Engine, workbook_path: Path = WORKBOOK_PATH) -> Dict[str, int]:
    workbook = pd.read_excel(workbook_path, sheet_name=None)
    loaded = {}
    for table_name in TABLE_LOAD_ORDER:
        df = workbook[table_name].copy()
        # normalize datelike columns
        for col in df.columns:
            if "date" in col or "month_start" in col or col.endswith("_at") or col.endswith("_time") or "timestamp" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="ignore")
                except Exception:
                    pass
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        loaded[table_name] = len(df)
    return loaded


def run_certified_pipeline(
    engine: Engine,
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    region: str = "All",
    product_category: str = "All",
) -> pd.DataFrame:
    actuals = pd.read_sql("SELECT * FROM sales_actuals", engine, parse_dates=["month_start", "business_date", "refresh_timestamp"])
    plan = pd.read_sql("SELECT * FROM sales_plan", engine, parse_dates=["month_start"])

    summary = (
        actuals.groupby(["month_start", "region", "product_category"], as_index=False)
        .agg(
            actual_revenue=("net_revenue", "sum"),
            gross_sales=("gross_sales", "sum"),
            returns_amount=("returns_amount", "sum"),
            units_sold=("units_sold", "sum"),
            inventory_on_hand=("inventory_on_hand", "sum"),
            missing_feeds=("feed_received_flag", lambda s: int((s != "Y").sum())),
        )
        .merge(
            plan[["month_start", "region", "product_category", "plan_revenue", "plan_units", "plan_version"]],
            on=["month_start", "region", "product_category"],
            how="left",
        )
        .sort_values(["month_start", "region", "product_category"])
        .reset_index(drop=True)
    )

    summary["variance"] = summary["actual_revenue"] - summary["plan_revenue"]
    summary["variance_pct"] = np.where(summary["plan_revenue"].fillna(0) != 0, summary["variance"] / summary["plan_revenue"], np.nan)
    summary["kpi_status"] = np.where(
        summary["plan_revenue"].isna() | (summary["missing_feeds"] > 0),
        "Pending Review",
        "Certified",
    )
    summary["owner"] = "VP Finance"
    summary["definition"] = "Net revenue after returns and approved adjustments"
    summary["last_refresh"] = pd.Timestamp("2026-01-05 08:15:00")

    if start_month:
        summary = summary[summary["month_start"] >= pd.to_datetime(start_month)]
    if end_month:
        summary = summary[summary["month_start"] <= pd.to_datetime(end_month)]
    if region != "All":
        summary = summary[summary["region"] == region]
    if product_category != "All":
        summary = summary[summary["product_category"] == product_category]

    summary.to_sql("certified_sales_summary", engine, if_exists="replace", index=False)
    return summary


def get_before_state(engine: Engine) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM kpi_submissions", engine, parse_dates=["report_month", "refresh_timestamp"])
    if df.empty:
        return df
    pivot = (
        df[df["kpi_name"] == "Revenue"]
        .pivot_table(index="report_month", columns="function_name", values="reported_value", aggfunc="first")
        .reset_index()
    )
    pivot["spread"] = pivot[[c for c in pivot.columns if c != "report_month"]].max(axis=1) - pivot[[c for c in pivot.columns if c != "report_month"]].min(axis=1)
    return pivot


def get_kpi_registry(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM kpi_registry", engine, parse_dates=["last_validated_date"])


def get_feed_monitor(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM feed_monitor", engine, parse_dates=["snapshot_time"])


def get_incidents(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM integration_incidents", engine, parse_dates=["opened_at", "resolved_at"])


def get_dq_results(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM data_quality_results", engine, parse_dates=["run_date"])


def get_issue_log(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM issue_log", engine, parse_dates=["opened_date"])


def get_adoption(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM analytics_adoption", engine, parse_dates=["month_start"])


def export_summary_to_desktop(df: pd.DataFrame) -> Dict[str, str]:
    paths = export_paths()
    paths["xlsx"].parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(paths["xlsx"], index=False)
    df.to_csv(paths["csv"], index=False)
    return {k: str(v) for k, v in paths.items()}
