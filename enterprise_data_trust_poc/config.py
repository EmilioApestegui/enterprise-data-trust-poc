
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
WORKBOOK_PATH = DATA_DIR / "enterprise_data_trust_poc_data.xlsx"
CSV_DIR = DATA_DIR / "csv"
TARGET_DB = "enterprise_data_trust_poc"
SCHEMA = "public"

WINDOWS_DESKTOP = Path(r"C:\Users\emili\OneDrive\Desktop")

def resolve_desktop_dir() -> Path:
    if WINDOWS_DESKTOP.exists():
        return WINDOWS_DESKTOP
    fallback = PROJECT_ROOT / "desktop_exports"
    fallback.mkdir(exist_ok=True)
    return fallback

def export_paths():
    desktop = resolve_desktop_dir()
    return {
        "xlsx": desktop / "certified_sales_summary.xlsx",
        "csv": desktop / "certified_sales_summary.csv",
    }
