import os
import io
import pandas as pd
from minio import Minio
import psycopg2
from datetime import date
run_date = date.today().isoformat()
object_name = f"{CURATED_PREFIX}/run_date={run_date}/{table.replace('.', '_')}.csv"

# No hardcoded defaults; rely entirely on environment variables
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")

PG_HOST = os.environ["PG_HOST"]
PG_DB = os.environ["PG_DB"]
PG_USER = os.environ["PG_USER"]
PG_PASS = os.environ["PG_PASS"]

TABLES = ["analytics.dim_product", "analytics.dim_customer", "analytics.fct_sales"]

def export_table_to_minio(table, client, conn):
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    object_name = f"{CURATED_PREFIX}/{table.replace('.', '_')}.csv"
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    print(f"✅ Uploaded {table} → s3://{MINIO_BUCKET}/{object_name} ({len(df)} rows)")

def main():
    print("🚀 Starting curated export job...")
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS
    )
    for table in TABLES:
        export_table_to_minio(table, client, conn)
    conn.close()
    print("🎉 Export completed successfully.")

if __name__ == "__main__":
    main()
