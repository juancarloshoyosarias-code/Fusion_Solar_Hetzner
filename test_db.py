import os, psycopg2
from dotenv import load_dotenv

load_dotenv(".env")

cfg = {k: os.getenv(k) for k in ["PGHOST","PGPORT","PGDATABASE","PGUSER","PGPASSWORD","PGSSLMODE"]}
print("Conectando a:", cfg["PGHOST"], cfg["PGPORT"], cfg["PGDATABASE"])

try:
    conn = psycopg2.connect(
        host=cfg["PGHOST"],
        port=cfg["PGPORT"],
        dbname=cfg["PGDATABASE"],
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg.get("PGSSLMODE","require"),
        connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        print("✅ Versión:", cur.fetchone()[0])
    conn.close()
except Exception as e:
    print("❌ Error de conexión:", e)

