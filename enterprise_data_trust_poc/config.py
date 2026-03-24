from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
WORKBOOK_PATH = DATA_DIR / "enterprise_data_trust_poc_data.xlsx"
CSV_DIR = DATA_DIR / "csv"

# Use Railway database if available, otherwise fallback to local DB name
DATABASE_URL = os.getenv("DATABASE_URL")

TARGET_DB = os.getenv("PGDATABASE", "enterprise_data_trust_poc")
SCHEMA = "public"

def resolve_desktop_dir() -> Path:
    # Railway has no desktop, so use project folder instead
    desktop = PROJECT_ROOT / "exports"
    desktop.mkdir(exist_ok=True)
    return desktop

def export_paths():
    desktop = resolve_desktop_dir()
    return {
        "xlsx": desktop / "certified_sales_summary.xlsx",
        "csv": desktop / "certified_sales_summary.csv",
    }