from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import WORKBOOK_PATH, export_paths

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

# Railway auto connection
def get_engine() -> Engine:
    return create_engine(os.getenv("DATABASE_URL"), future=True)

def test_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

def load_workbook_to_postgres(engine: Engine, workbook_path: Path = WORKBOOK_PATH) -> Dict[str, int]:
    workbook = pd.read_excel(workbook_path, sheet_name=None)
    loaded = {}

    for table_name in TABLE_LOAD_ORDER:
        df = workbook[table_name].copy()

        for col in df.columns:
            if (
                "date" in col
                or "month_start" in col
                or col.endswith("_at")
                or col.endswith("_time")
                or "timestamp" in col
            ):
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
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

    actuals = pd.read_sql("SELECT * FROM sales_actuals", engine)
    plan = pd.read_sql("SELECT * FROM sales_plan", engine)

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
            plan[["month_start","region","product_category","plan_revenue","plan_units","plan_version"]],
            on=["month_start","region","product_category"],
            how="left",
        )
    )

    summary["variance"] = summary["actual_revenue"] - summary["plan_revenue"]
    summary["variance_pct"] = np.where(
        summary["plan_revenue"].fillna(0) != 0,
        summary["variance"] / summary["plan_revenue"],
        np.nan,
    )

    summary["kpi_status"] = np.where(
        summary["plan_revenue"].isna() | (summary["missing_feeds"] > 0),
        "Pending Review",
        "Certified",
    )

    summary["owner"] = "VP Finance"
    summary["definition"] = "Net revenue after returns and approved adjustments"

    if start_month:
        summary = summary[summary["month_start"] >= start_month]

    if end_month:
        summary = summary[summary["month_start"] <= end_month]

    if region != "All":
        summary = summary[summary["region"] == region]

    if product_category != "All":
        summary = summary[summary["product_category"] == product_category]

    summary.to_sql("certified_sales_summary", engine, if_exists="replace", index=False)

    return summary


def get_before_state(engine: Engine) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM kpi_submissions", engine)

    pivot = (
        df[df["kpi_name"]=="Revenue"]
        .pivot_table(
            index="report_month",
            columns="function_name",
            values="reported_value",
            aggfunc="first",
        )
        .reset_index()
    )

    pivot["spread"] = (
        pivot.drop(columns=["report_month"]).max(axis=1)
        - pivot.drop(columns=["report_month"]).min(axis=1)
    )

    return pivot


def get_kpi_registry(engine: Engine):
    return pd.read_sql("SELECT * FROM kpi_registry", engine)

def get_feed_monitor(engine: Engine):
    return pd.read_sql("SELECT * FROM feed_monitor", engine)

def get_incidents(engine: Engine):
    return pd.read_sql("SELECT * FROM integration_incidents", engine)

def get_dq_results(engine: Engine):
    return pd.read_sql("SELECT * FROM data_quality_results", engine)

def get_issue_log(engine: Engine):
    return pd.read_sql("SELECT * FROM issue_log", engine)

def get_adoption(engine: Engine):
    return pd.read_sql("SELECT * FROM analytics_adoption", engine)


def export_summary_to_desktop(df: pd.DataFrame):
    paths = export_paths()

    df.to_excel(paths["xlsx"], index=False)
    df.to_csv(paths["csv"], index=False)

    return {k: str(v) for k,v in paths.items()}