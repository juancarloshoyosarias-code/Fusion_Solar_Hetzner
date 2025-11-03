# backfill_history.py
import os, json, time, random, requests, psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

# ---- Huawei (.env) ----
DOMAIN   = os.getenv("FUSION_DOMAIN")              # p.ej. la5.fusionsolar.huawei.com
USER     = os.getenv("FS_USER")
SYSCODE  = os.getenv("FS_SYSCODE")

# ---- Postgres (.env) ----
PGHOST     = os.getenv("PGHOST")
PGPORT     = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER     = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGSSLMODE  = os.getenv("PGSSLMODE", "require")

# ---- Opcionales de control ----
DRY_RUN                 = os.getenv("DRY_RUN", "false").strip().lower() in {"1","true","yes","y"}
UNTIL_DATE_STR          = os.getenv("UNTIL_DATE", "").strip()   # p.ej. "2025-10-01" (inclusive)
HTTP_TIMEOUT_SECONDS    = float(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))
NET_RETRY_TOTAL         = int(os.getenv("NET_RETRY_TOTAL", "5"))
NET_RETRY_BACKOFF       = float(os.getenv("NET_RETRY_BACKOFF", "0.5"))

# ---- Control por ENV (ajustables sin tocar código) ----
ONLY_PLANT                 = os.getenv("ONLY_PLANT", "").strip()       # p.ej. "NE=33801790"
PER_MONTH_DELAY_SECONDS    = float(os.getenv("PER_MONTH_DELAY_SECONDS", "2.5"))
BACKOFF_SECONDS_407        = int(os.getenv("BACKOFF_SECONDS_407", "480"))  # 8 min por defecto
MAX_RETRIES_407_PER_MONTH  = int(os.getenv("MAX_RETRIES_407_PER_MONTH", "4"))

# ---- Fechas de inicio por planta ----
START_DATES = {
    "NE=33723010": "2023-01-01",  # Pozo 1
    "NE=33758743": "2023-08-01",  # Pozo 2
    "NE=33745211": "2023-06-01",  # Porvenir
    "NE=33876570": "2024-04-01",  # Cabañita
    "NE=33788377": "2023-11-01",  # Casa Trejo
    "NE=33801790": "2023-12-01",  # Maracaibo
}

# ---- Validación de ENV ----
REQUIRED_ENV = {
    "FUSION_DOMAIN": DOMAIN,
    "FS_USER": USER,
    "FS_SYSCODE": SYSCODE,
    "PGHOST": PGHOST,
    "PGPORT": PGPORT,
    "PGDATABASE": PGDATABASE,
    "PGUSER": PGUSER,
    "PGPASSWORD": PGPASSWORD,
}

def _fail_missing_env():
    missing = [k for k,v in REQUIRED_ENV.items() if not v]
    if missing:
        raise RuntimeError(f"Faltan variables de entorno requeridas: {', '.join(missing)}")

# ---- Sesión Huawei ----
session = None

def _new_session():
    s = requests.Session()
    # Retries de red para errores transitorios (no sustituyen la lógica 305/407)
    retry = Retry(
        total=NET_RETRY_TOTAL,
        backoff_factor=NET_RETRY_BACKOFF,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET","POST"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
        "Origin": f"https://{DOMAIN}",
        "Referer": f"https://{DOMAIN}/",
    })
    return s

def _preflight():
    try:
        session.get(
            f"https://{DOMAIN}/thirdData/",
            headers={"Accept": "text/html,application/json"},
            timeout=15
        )
    except Exception:
        pass

def login():
    global session
    session = _new_session()
    _preflight()
    url = f"https://{DOMAIN}/thirdData/login"
    r = session.post(
        url,
        json={"userName": USER, "systemCode": SYSCODE},
        timeout=HTTP_TIMEOUT_SECONDS
    )
    r.raise_for_status()
    xsrf = (
        session.cookies.get("XSRF-TOKEN") or session.cookies.get("xsrf-token")
        or r.headers.get("XSRF-TOKEN") or r.headers.get("xsrf-token")
    )
    if not xsrf:
        raise RuntimeError("❌ No se obtuvo XSRF-TOKEN en login")
    session.headers.update({"XSRF-TOKEN": xsrf})
    print("✅ Login Huawei (thirdData) OK")

def _post_json(url, payload, timeout=HTTP_TIMEOUT_SECONDS):
    """
    POST con manejo de relogin (305) y rate-limit (407) con backoff exponencial + jitter.
    """
    attempt_407 = 0
    while True:
        r = session.post(url, json=payload, timeout=timeout)
        # Si no es JSON, valida HTTP y retorna
        try:
            data = r.json()
        except Exception:
            r.raise_for_status()
            return {}

        # 305: relogin
        if data.get("failCode") == 305 or data.get("message") == "USER_MUST_RELOGIN":
            print("↻ 305 USER_MUST_RELOGIN → relogin…")
            login()
            continue

        # 407: ACCESS_FREQUENCY_IS_TOO_HIGH → backoff y reintento limitado
        if data.get("failCode") == 407 or data.get("data") == "ACCESS_FREQUENCY_IS_TOO_HIGH":
            attempt_407 += 1
            if attempt_407 > MAX_RETRIES_407_PER_MONTH:
                print(f"⏭️  407 persistente, salto este mes (intentos={attempt_407}) → {data}")
                return data
            sleep_s = BACKOFF_SECONDS_407 * (1.2 ** (attempt_407 - 1))
            # jitter ±10%
            jitter = sleep_s * random.uniform(-0.1, 0.1)
            sleep_s = max(60.0, sleep_s + jitter)
            mins = int(round(sleep_s / 60))
            print(f"⏳ 407 rate-limit: esperando ~{mins} min antes de reintentar (intento {attempt_407}/{MAX_RETRIES_407_PER_MONTH})…")
            time.sleep(sleep_s)
            # tras backoff, relogin para sesión fresca
            login()
            continue

        # si no hay 305/407, valida HTTP y retorna data
        r.raise_for_status()
        return data

# ---- API mensual: getKpiStationDay (devuelve todo el mes) ----
def get_kpi_station_day(station_code, any_day_in_month):
    first = any_day_in_month.replace(day=1)
    ts_ms = int(datetime(first.year, first.month, 1).timestamp() * 1000)
    url = f"https://{DOMAIN}/thirdData/getKpiStationDay"
    payload = {"stationCodes": station_code, "collectTime": ts_ms}
    return _post_json(url, payload, timeout=HTTP_TIMEOUT_SECONDS)

# ---- DB ----
def _conn():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE,
        user=PGUSER, password=PGPASSWORD, sslmode=PGSSLMODE,
    )

# Última fecha almacenada para reanudar (fs_history_power)
def _get_last_ddate(plant_code):
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT max(ddate)
                    FROM raw.fs_history_power
                    WHERE plant_code = %s
                    """,
                    (plant_code,)
                )
                row = cur.fetchone()
                return row[0] if row and row[0] else None
    except Exception as e:
        print(f"⚠️ No se pudo consultar última fecha para {plant_code}: {e}")
        return None

def save_generation_rows(rows):
    if not rows:
        return
    if DRY_RUN:
        print(f"(DRY_RUN) omito inserción de {len(rows)} filas en fs_history_power")
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw.fs_history_power
                  (plant_code, ddate, day_kwh, payload_json)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (plant_code, ddate) DO UPDATE SET
                  day_kwh = EXCLUDED.day_kwh,
                  payload_json = EXCLUDED.payload_json
                """,
                rows,
            )

def save_meter_rows(rows):
    if not rows:
        return
    if DRY_RUN:
        print(f"(DRY_RUN) omito inserción de {len(rows)} filas en fs_meter_energy_daily")
        return
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO raw.fs_meter_energy_daily
                      (ts_date, plant_code, day_use_kwh, day_grid_kwh)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (plant_code, ts_date) DO UPDATE SET
                      day_use_kwh = EXCLUDED.day_use_kwh,
                      day_grid_kwh = EXCLUDED.day_grid_kwh
                    """,
                    rows,
                )
    except Exception as e:
        print(f"⚠️ No se pudieron guardar consumo/export: {e}")

# ---- Parseo ----
def nz(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0

def parse_month_payload(plant_code, data):
    gen_rows, meter_rows = [], []
    # Algunas regiones devuelven {"data": [...]} y otras {"datas": [...]} o {"data": {"records": [...]}}
    records = (data.get("data") or data.get("datas") or [])
    if isinstance(records, dict):
        records = records.get("records") or []
    for rec in records:
        ct = rec.get("collectTime")
        if not ct:
            continue
        ddate = datetime.utcfromtimestamp(ct / 1000).date()
        dmap = rec.get("dataItemMap") or {}
        gen_kwh  = nz(dmap.get("inverter_power") or dmap.get("product_power") or
                      dmap.get("day_power") or dmap.get("daily_power_generation"))
        grid_kwh = nz(dmap.get("ongrid_power") or dmap.get("daily_on_grid_energy"))
        use_kwh  = nz(dmap.get("use_power")    or dmap.get("daily_use_energy"))
        payload = {"plant_code": plant_code, "collectTime": ct, "dataItemMap": dmap}
        gen_rows.append((plant_code, ddate, gen_kwh, json.dumps(payload)))
        meter_rows.append((ddate, plant_code, use_kwh, grid_kwh))
    return gen_rows, meter_rows

# ---- Iteración mensual ----
def month_iter(d0, d1):
    y, m = d0.year, d0.month
    while (y < d1.year) or (y == d1.year and m <= d1.month):
        yield date(y, m, 1)
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

# ---- Backfill ----
def run_backfill(start_dates):
    _fail_missing_env()
    login()  # Sesión válida antes de cualquier llamada
    today = datetime.utcnow().date()
    if UNTIL_DATE_STR:
        try:
            until = datetime.strptime(UNTIL_DATE_STR, "%Y-%m-%d").date()
            if until < today:
                today = until
        except Exception:
            print(f"⚠️ UNTIL_DATE inválida: '{UNTIL_DATE_STR}', se ignora")

    # si ONLY_PLANT viene en ENV, limita a esa planta
    items = [(ONLY_PLANT, start_dates.get(ONLY_PLANT))] if ONLY_PLANT else start_dates.items()

    for plant_code, start_str in items:
        if not plant_code or not start_str:
            print("⚠️ ONLY_PLANT no coincide con START_DATES; nada que hacer.")
            return

        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        last = _get_last_ddate(plant_code)
        if last and last >= start_date:
            start_date = last + date.resolution  # día siguiente
            print(f"   ↪ Reanudando desde {start_date} (última fecha almacenada {last})")
        print(f"\n== Planta {plant_code}: {start_date} → {today}")

        for first_of_month in month_iter(start_date, today):
            try:
                resp = get_kpi_station_day(plant_code, first_of_month)
                if not resp.get("success", False):
                    # si fue 407 y agotó retries, resp no trae success → seguimos al siguiente mes
                    print(f"⚠️ {plant_code} {first_of_month:%Y-%m}: {resp}")
                else:
                    gen_rows, meter_rows = parse_month_payload(plant_code, resp)
                    save_generation_rows(gen_rows)
                    save_meter_rows(meter_rows)
                    print(f"   · {plant_code} {first_of_month:%Y-%m}: {len(gen_rows)} días OK")
                # pausa entre meses para no disparar 407
                sleep_s = PER_MONTH_DELAY_SECONDS * random.uniform(0.8, 1.4)
                time.sleep(sleep_s)
            except requests.HTTPError as e:
                print(f"❌ HTTP {plant_code} {first_of_month:%Y-%m}: {e}")
                time.sleep(10)
            except Exception as e:
                print(f"❌ EXC {plant_code} {first_of_month:%Y-%m}: {e}")
                time.sleep(4)

if __name__ == "__main__":
    print(f"ONLY_PLANT={ONLY_PLANT or '*'} | DRY_RUN={DRY_RUN} | PER_MONTH_DELAY_SECONDS={PER_MONTH_DELAY_SECONDS} | MAX_RETRIES_407_PER_MONTH={MAX_RETRIES_407_PER_MONTH}")
    run_backfill(START_DATES)
    