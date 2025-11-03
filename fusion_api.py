import os
import time
import re
import random
import json
import requests
import psycopg2
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────
# Carga .env
# ─────────────────────────────────────────────────────────
load_dotenv(".env")

DOMAIN   = os.getenv("FUSION_DOMAIN")              # p.ej. la5.fusionsolar.huawei.com
USER     = os.getenv("FS_USER")
SYSCODE  = os.getenv("FS_SYSCODE")

# Timings desde .env (editables sin tocar código)
PER_PLANT_DELAY_SECONDS  = float(os.getenv("PER_PLANT_DELAY_SECONDS", "180"))
BACKOFF_SECONDS          = int(os.getenv("BACKOFF_SECONDS", "480"))
STARTUP_COOLDOWN_SECONDS = int(os.getenv("STARTUP_COOLDOWN_SECONDS", "60"))
DEBUG_PAYLOAD            = int(os.getenv("DEBUG_PAYLOAD", "1"))

LAST_CALL_FILE = Path(".last_call_ts")

# ─────────────────────────────────────────────────────────
# Utilidades
# ─────────────────────────────────────────────────────────
def _normalize_codes(raw: str):
    out = []
    for token in (raw or "").split(","):
        t = token.strip()
        if not t:
            continue
        if t.startswith("STATION_CODES="):
            t = t.split("STATION_CODES=", 1)[1].strip()
        if not t.startswith("NE="):
            if t.isdigit():
                t = f"NE={t}"
        out.append(t)
    return out

def nz(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0

RAW_CODES = os.getenv("STATION_CODES", "")
print("RAW STATION_CODES =", repr(RAW_CODES))
STATIONS = _normalize_codes(RAW_CODES)
if not STATIONS:
    raise SystemExit("No hay STATION_CODES en .env")
print("STATIONS normalizados:", STATIONS)

# ─────────────────────────────────────────────────────────
# Sesión HTTP Huawei
# ─────────────────────────────────────────────────────────
session: requests.Session = None
xsrf_token = None

def _new_session():
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0"
    })
    return s

def preflight():
    url = f"https://{DOMAIN}/thirdData/"
    try:
        session.get(
            url,
            headers={"Accept": "text/html,application/json", "Referer": f"https://{DOMAIN}/"},
            timeout=20,
        )
    except Exception:
        pass

def login():
    global xsrf_token
    preflight()
    url = f"https://{DOMAIN}/thirdData/login"
    r1 = session.post(
        url,
        headers={"Origin": f"https://{DOMAIN}", "Referer": f"https://{DOMAIN}/"},
        json={"userName": USER, "systemCode": SYSCODE},
        timeout=20,
    )
    r1.raise_for_status()

    xsrf = (
        r1.headers.get("XSRF-TOKEN")
        or r1.headers.get("xsrf-token")
        or r1.headers.get("X-XSRF-TOKEN")
        or session.cookies.get("XSRF-TOKEN")
        or session.cookies.get("xsrf-token")
    )
    if not xsrf:
        m = re.search(r"XSRF-TOKEN=([^;]+)", r1.headers.get("Set-Cookie", ""))
        if m:
            xsrf = m.group(1)
    if not xsrf:
        xsrf = session.cookies.get("XSRF-TOKEN") or session.cookies.get("xsrf-token")

    if not xsrf:
        raise RuntimeError("No llegó XSRF-TOKEN en el login")

    xsrf_token = xsrf
    session.headers.update({"XSRF-TOKEN": xsrf_token})
    print("✅ Login OK. XSRF-TOKEN obtenido.")

def get_station_kpi(station_code_ne: str):
    url = f"https://{DOMAIN}/thirdData/getStationRealKpi"
    payload = {"stationCodes": station_code_ne}
    r = session.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        raise requests.HTTPError(f"{r.status_code} {r.reason} → {r.text[:600]}")
    return r.json()

def safe_logout():
    try:
        session.close()
    except Exception:
        pass

# ─────────────────────────────────────────────────────────
# Rate-limit helpers
# ─────────────────────────────────────────────────────────
def _now_s(): return int(time.time())
def _load_last_call_ts():
    try: return int(LAST_CALL_FILE.read_text().strip())
    except Exception: return 0
def _save_last_call_ts(ts: int):
    try: LAST_CALL_FILE.write_text(str(ts))
    except: pass
def _respect_rate_limit():
    last = _load_last_call_ts()
    elapsed = _now_s() - last
    wait = PER_PLANT_DELAY_SECONDS - elapsed
    if wait > 0:
        print(f"RATE: esperando {wait:.1f}s para cumplir {PER_PLANT_DELAY_SECONDS:.0f}s…", flush=True)
        time.sleep(wait)
def _ensure_cool_start():
    if not LAST_CALL_FILE.exists():
        print(f"START: esperando {STARTUP_COOLDOWN_SECONDS}s (cooldown de arranque)…", flush=True)
        time.sleep(STARTUP_COOLDOWN_SECONDS)

# ─────────────────────────────────────────────────────────
# Helpers de extracción con aliases y dataItemMap
# ─────────────────────────────────────────────────────────
def _from_map(obj, key):
    if isinstance(obj, dict):
        if key in obj and obj[key] is not None:
            return obj[key]
        dim = obj.get("dataItemMap")
        if isinstance(dim, dict) and key in dim and dim[key] is not None:
            return dim[key]
    return None

def pick_str(obj, *keys):
    for k in keys:
        v = _from_map(obj, k)
        if v: return str(v)
    return None

def pick_float(obj, *keys):
    for k in keys:
        v = _from_map(obj, k)
        if v is None: continue
        try: return float(v)
        except:
            try: return float(re.sub(r"[^0-9.\-]", "", str(v)))
            except: continue
    return 0.0

# ─────────────────────────────────────────────────────────
# PostgreSQL
# ─────────────────────────────────────────────────────────
def save_to_db(rows):
    if not rows:
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            port=os.getenv("PGPORT"),
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
            sslmode=os.getenv("PGSSLMODE", "require"),
        )

        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO raw.fs_realtime_plants
                    (plant_code, ts_utc, plant_name, power_kw, day_power_kwh, month_power_kwh, total_power_kwh, health)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (ts_utc, plant_code) DO NOTHING
                    """,
                    rows,
                )

                # UPSERT último estado
                last_row = rows[-1]
                plant_code, ts_utc, plant_name, power_kw, day_power_kwh, month_power_kwh, total_power_kwh, health = last_row

                cur.execute(
                    """
                    INSERT INTO raw.fs_plants_last
                    (plant_code, updated_utc, plant_name, power_kw, day_power_kwh, month_power_kwh, total_power_kwh, health)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (plant_code) DO UPDATE SET
                      updated_utc      = EXCLUDED.updated_utc,
                      plant_name       = EXCLUDED.plant_name,
                      power_kw         = EXCLUDED.power_kw,
                      day_power_kwh    = EXCLUDED.day_power_kwh,
                      month_power_kwh  = EXCLUDED.month_power_kwh,
                      total_power_kwh  = EXCLUDED.total_power_kwh,
                      health           = EXCLUDED.health
                    """,
                    (plant_code, ts_utc, plant_name, power_kw, day_power_kwh, month_power_kwh, total_power_kwh, health)
                )

    except psycopg2.Error as e:
        print(f"❌ Error al guardar en la base de datos: {e}")
    finally:
        if conn:
            conn.close()

# ─────────────────────────────────────────────────────────
# Recolección por planta
# ─────────────────────────────────────────────────────────
def fetch_one_plant(plant_ne_code: str):
    global session
    _ensure_cool_start()
    _respect_rate_limit()

    session = _new_session()
    try:
        login()
    except Exception as e:
        print(f"❌ Login falló para {plant_ne_code}: {e}")
        safe_logout()
        return False

    try:
        time.sleep(2.0)
        data = get_station_kpi(plant_ne_code)

        if (
            data.get("data") == "ACCESS_FREQUENCY_IS_TOO_HIGH"
            or data.get("failCode") == 407
            or data.get("message") == "ACCESS_FREQUENCY_IS_TOO_HIGH"
        ):
            print(f"⏳ {plant_ne_code}: rate limit → durmiendo {BACKOFF_SECONDS}s")
            time.sleep(BACKOFF_SECONDS)
            return False

        if not data.get("success", False):
            print(f"⚠️ {plant_ne_code}: respuesta no exitosa → {data}")
            return False

        payload = data.get("data")
        if not payload:
            print(f"ℹ️ {plant_ne_code}: sin datos → {data}")
            return False
        first = payload[0] if isinstance(payload, list) else payload

        if DEBUG_PAYLOAD and plant_ne_code == STATIONS[0]:
            try: print("DEBUG payload (recortado):", json.dumps(first, ensure_ascii=False)[:1000])
            except: print("DEBUG payload keys:", list(first.keys()))

        station_name = pick_str(first, "stationName", "name", "plantName", "stationCode") or plant_ne_code
        power_kw    = pick_float(first, "realTimePower","realtimePower","activePower","power")
        day_kwh     = pick_float(first, "day_power","day_on_grid_energy")
        month_kwh   = pick_float(first, "month_power")
        total_kwh   = pick_float(first, "total_power")
        health      = _from_map(first, "real_health_state") or 0

        ts_now_utc = datetime.now(timezone.utc)
        row = (plant_ne_code, ts_now_utc, station_name, power_kw, day_kwh, month_kwh, total_kwh, health)
        save_to_db([row])

        print(f"OK {plant_ne_code}: name={station_name} P={power_kw} kW D={day_kwh} kWh M={month_kwh} kWh T={total_kwh} kWh H={health}")
        _save_last_call_ts(_now_s())
        return True

    except Exception as e:
        print(f"❌ Error en {plant_ne_code}: {e}")
        return False
    finally:
        safe_logout()
        time.sleep(1.0)

# ─────────────────────────────────────────────────────────
# Loop principal
# ─────────────────────────────────────────────────────────
def loop():
    while True:
        ok_count = 0
        for st in STATIONS:
            time.sleep(random.uniform(0.0,0.6))
            if fetch_one_plant(st): ok_count += 1
        print(f"✅ Ciclo terminado: {ok_count}/{len(STATIONS)} plantas guardadas @ {datetime.now(timezone.utc).isoformat()}")
        print("🔄 Iniciando nuevo ciclo de inmediato…")

if __name__ == "__main__":
    try: loop()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO EN EL SCRIPT: {e}")
        time.sleep(3); raise
    
