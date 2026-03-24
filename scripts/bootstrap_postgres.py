
from enterprise_data_trust_poc.config import TARGET_DB, WORKBOOK_PATH
from enterprise_data_trust_poc.db import ensure_database, app_engine, load_workbook_to_postgres
import argparse

parser = argparse.ArgumentParser(description="Create the Postgres database and load the static sample workbook.")
parser.add_argument("--host", default="localhost")
parser.add_argument("--port", default="5432")
parser.add_argument("--admin-db", default="postgres")
parser.add_argument("--user", default="postgres")
parser.add_argument("--password", default="postgres")
args = parser.parse_args()

print(ensure_database(args.host, args.port, args.admin_db, args.user, args.password, TARGET_DB))
engine = app_engine(args.host, args.port, args.user, args.password, TARGET_DB)
loaded = load_workbook_to_postgres(engine, WORKBOOK_PATH)
print("Loaded tables:")
for name, rows in loaded.items():
    print(f"- {name}: {rows} rows")
