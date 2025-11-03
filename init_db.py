import os, psycopg2, pathlib
from dotenv import load_dotenv

load_dotenv(".env")

sql_path = pathlib.Path("db_init.sql")
assert sql_path.exists(), "No encuentro db_init.sql"

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    sslmode=os.getenv("PGSSLMODE","require"),
)
conn.autocommit = True
with conn, conn.cursor() as cur:
    cur.execute(sql_path.read_text())
    print("✅ Esquema creado / actualizado")

# Verificación rápida
with conn, conn.cursor() as cur:
    cur.execute("SELECT extname, extversion FROM pg_extension WHERE extname='timescaledb';")
    print("Extensión timescaledb:", cur.fetchone())
conn.close()

