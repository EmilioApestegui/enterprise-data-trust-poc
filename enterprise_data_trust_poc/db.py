from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import TARGET_DB, WORKBOOK_PATH, export_paths


def app_engine():
    database_url = os.getenv("DATABASE_URL")

    if database_url:
        return create_engine(database_url)

    # fallback for local testing
    return create_engine(
        "postgresql+psycopg2://postgres:postgres@localhost:5432/enterprise_data_trust_poc"
    )


def test_connection(engine: Engine):
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def load_workbook_to_postgres(engine: Engine):
    workbook = pd.read_excel(WORKBOOK_PATH, sheet_name=None)

    loaded = {}

    for table_name, df in workbook.items():
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        loaded[table_name] = len(df)

    return loaded


def run_certified_pipeline(engine: Engine):

    actuals = pd.read_sql("sales_actuals", engine)
    plan = pd.read_sql("sales_plan", engine)

    summary = (
        actuals.groupby(["month_start","region","product_category"])
        .agg(actual_revenue=("net_revenue","sum"))
        .reset_index()
    )

    summary.to_sql(
        "certified_sales_summary",
        engine,
        if_exists="replace",
        index=False
    )

    return summary


def get_before_state(engine):
    return pd.read_sql("kpi_submissions",engine)


def get_kpi_registry(engine):
    return pd.read_sql("kpi_registry",engine)


def get_feed_monitor(engine):
    return pd.read_sql("feed_monitor",engine)


def get_incidents(engine):
    return pd.read_sql("integration_incidents",engine)


def get_dq_results(engine):
    return pd.read_sql("data_quality_results",engine)


def get_issue_log(engine):
    return pd.read_sql("issue_log",engine)


def get_adoption(engine):
    return pd.read_sql("analytics_adoption",engine)


def export_summary_to_desktop(df):

    paths = export_paths()

    df.to_excel(paths["xlsx"],index=False)
    df.to_csv(paths["csv"],index=False)

    return paths