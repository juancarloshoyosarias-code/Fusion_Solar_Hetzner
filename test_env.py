from dotenv import load_dotenv
import os
import psycopg2

# Cargar variables desde .env
load_dotenv(".env")

print("🔍 Variables cargadas desde .env:")
print("Host:", os.getenv("PGHOST"))
print("Puerto:", os.getenv("PGPORT"))
print("Usuario:", os.getenv("PGUSER"))

# Crear cadena de conexión DSN
dsn = os.getenv("PG_DSN")
if not dsn:
    dsn = (
        f"host={os.getenv('PGHOST')} "
        f"port={os.getenv('PGPORT')} "
        f"dbname={os.getenv('PGDATABASE')} "
        f"user={os.getenv('PGUSER')} "
        f"password={os.getenv('PGPASSWORD')} "
        f"sslmode={os.getenv('PGSSLMODE', 'disable')}"
    )

print("\n🧩 Probando conexión con DSN:")
print(dsn)

try:
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("SELECT now(), current_database(), current_user;")
    row = cur.fetchone()
    print("\n✅ Conexión exitosa a PostgreSQL:")
    print(f"Fecha/Hora: {row[0]}\nBase de datos: {row[1]}\nUsuario: {row[2]}")
except Exception as e:
    print("\n❌ Error al conectar a la base de datos:")
    print(e)
finally:
    if 'conn' in locals():
        conn.close()
        print("\n🔌 Conexión cerrada correctamente.")
        