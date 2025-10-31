# exporter/export_curated.py
import os, io
import pandas as pd
from minio import Minio
import psycopg2
from datetime import date

# --- env ---
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")

PG_HOST = os.environ["PG_HOST"]
PG_DB   = os.environ["PG_DB"]
PG_USER = os.environ["PG_USER"]
PG_PASS = os.environ["PG_PASS"]

TABLES = ["analytics.dim_product", "analytics.dim_customer", "analytics.fct_sales"]

def main():
    print("🚀 Starting curated export job...")
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    try:
        run_date = date.today().isoformat()
        for table in TABLES:
            # query
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            # buffer
            buf = io.BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            # construct S3 object name *here*, where we have table + run_date
            object_name = f"{CURATED_PREFIX}/run_date={run_date}/{table.replace('.', '_')}.csv"
            # upload
            client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                data=buf,
                length=buf.getbuffer().nbytes,
                content_type="text/csv",
            )
            print(f"✅ Uploaded {table} → s3://{MINIO_BUCKET}/{object_name} ({len(df)} rows)")
        print("🎉 Export completed successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
