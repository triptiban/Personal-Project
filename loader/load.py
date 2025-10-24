import os, json, datetime as dt
from minio import Minio
import psycopg2
from psycopg2.extras import execute_values

# ----------------------------
# MinIO configuration
# ----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")  # e.g. minio.ecommerce.svc.cluster.local:9000
ACCESS = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY")
SECRET = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY")
SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET = os.getenv("MINIO_BUCKET", "ecommerce")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw")
PARTITION_DATE = os.getenv("PARTITION_DATE") or dt.date.today().isoformat()

# ----------------------------
# Postgres configuration
# ----------------------------
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "warehouse")
PG_USER = os.getenv("POSTGRES_USER", "warehouse")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "warehouse")

def read_json(client, key):
    """Fetch a JSON file from MinIO."""
    resp = client.get_object(BUCKET, key)
    data = json.loads(resp.read().decode("utf-8"))
    resp.close()
    resp.release_conn()
    return data

def main():
    prefix = f"{RAW_PREFIX}/date={PARTITION_DATE}/"
    m = Minio(MINIO_ENDPOINT, ACCESS, SECRET, secure=SECURE)

    print("Downloading data from MinIO...")
    products = read_json(m, prefix + "products.json")
    users = read_json(m, prefix + "users.json")
    carts = read_json(m, prefix + "carts.json")

    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # ----------------------------
    # Create and load raw.products
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.products (
            product_id INT PRIMARY KEY,
            title TEXT,
            price NUMERIC(10,2),
            category TEXT,
            description TEXT,
            _src JSONB,
            _ingested_at TIMESTAMPTZ DEFAULT now()
        );
    """)
    prod_rows = [
        (p["id"], p["title"], p["price"], p["category"], p.get("description", ""), json.dumps(p))
        for p in products
    ]
    execute_values(
        cur,
        """
        INSERT INTO raw.products (product_id, title, price, category, description, _src)
        VALUES %s
        ON CONFLICT (product_id)
        DO UPDATE SET
            title=excluded.title,
            price=excluded.price,
            category=excluded.category,
            description=excluded.description,
            _src=excluded._src,
            _ingested_at=now();
        """,
        prod_rows,
    )

    # ----------------------------
    # Create and load raw.users
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.users (
            user_id INT PRIMARY KEY,
            email TEXT,
            username TEXT,
            full_name TEXT,
            city TEXT,
            phone TEXT,
            _src JSONB,
            _ingested_at TIMESTAMPTZ DEFAULT now()
        );
    """)
    urows = []
    for u in users:
        name = f"{u.get('name', {}).get('firstname', '')} {u.get('name', {}).get('lastname', '')}".strip()
        city = u.get("address", {}).get("city")
        urows.append((u["id"], u.get("email"), u.get("username"), name, city, u.get("phone"), json.dumps(u)))
    execute_values(
        cur,
        """
        INSERT INTO raw.users (user_id, email, username, full_name, city, phone, _src)
        VALUES %s
        ON CONFLICT (user_id)
        DO UPDATE SET
            email=excluded.email,
            username=excluded.username,
            full_name=excluded.full_name,
            city=excluded.city,
            phone=excluded.phone,
            _src=excluded._src,
            _ingested_at=now();
        """,
        urows,
    )

    # ----------------------------
    # Create and load raw.carts + cart_items
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.carts (
            cart_id INT PRIMARY KEY,
            user_id INT,
            date DATE,
            _src JSONB,
            _ingested_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS raw.cart_items (
            cart_id INT,
            product_id INT,
            quantity INT,
            PRIMARY KEY (cart_id, product_id)
        );
    """)
    crows, irows = [], []
    for c in carts:
        crows.append((c["id"], c["userId"], (c.get("date") or "")[:10] or None, json.dumps(c)))
        for it in c.get("products", []):
            irows.append((c["id"], it["productId"], it["quantity"]))
    execute_values(
        cur,
        """
        INSERT INTO raw.carts (cart_id, user_id, date, _src)
        VALUES %s
        ON CONFLICT (cart_id)
        DO UPDATE SET
            user_id=excluded.user_id,
            date=excluded.date,
            _src=excluded._src,
            _ingested_at=now();
        """,
        crows,
    )
    execute_values(
        cur,
        """
        INSERT INTO raw.cart_items (cart_id, product_id, quantity)
        VALUES %s
        ON CONFLICT (cart_id, product_id)
        DO UPDATE SET quantity=excluded.quantity;
        """,
        irows,
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Data loaded successfully for {PARTITION_DATE}")

if __name__ == "__main__":
    main()
