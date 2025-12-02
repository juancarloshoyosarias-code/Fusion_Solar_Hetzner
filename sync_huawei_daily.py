import requests
import json
import psycopg2
import os
import sys
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
API_USER = os.getenv('HUAWEI_API_USERNAME')
API_PASS = os.getenv('HUAWEI_API_PASSWORD')
BASE_URL = "https://intl.fusionsolar.huawei.com/thirdData"

DB_HOST = os.getenv('PGHOST')
DB_NAME = os.getenv('PGDATABASE')
DB_USER = os.getenv('PGUSER')
DB_PASS = os.getenv('PGPASSWORD')

# MAPEO DEFINITIVO
PLANT_MAP = {
    "NE=33876570": 1, "NE=33801790": 2, "NE=33745211": 3,
    "NE=33758743": 4, "NE=33723010": 5, "NE=33788377": 6
}
PLANT_KWP = {1: 116.0, 2: 116.0, 3: 143.0, 4: 116.0, 5: 119.56, 6: 9.765}

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def login():
    url = f"{BASE_URL}/login"
    payload = {"userName": API_USER, "systemCode": API_PASS}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        data = resp.json()
        if data.get('success'): return data['data']
        else: sys.exit(1)
    except: sys.exit(1)

def sync_data():
    print(f"--- SINCRONIZACIÓN API OFICIAL: {datetime.now()} ---")
    token = login()
    headers = {"XSRF-TOKEN": token}
    
    # Pedimos datos de AYER para asegurar cierre de día
    target_date = datetime.now().date() - timedelta(days=1)
    collect_time = int(datetime(target_date.year, target_date.month, target_date.day).timestamp()) * 1000
    
    print(f"📡 Consultando fecha: {target_date}")
    station_codes = ",".join(PLANT_MAP.keys())
    
    url = f"{BASE_URL}/getKpiStationDay"
    payload = {"stationCodes": station_codes, "collectTime": collect_time}
    
    resp = requests.post(url, json=payload, headers=headers)
    api_data = resp.json()
    
    if not api_data.get('success'):
        print("❌ Error API Huawei")
        sys.exit(1)
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    raw_list = api_data.get('data', [])
    count = 0
    for item in raw_list:
        p_code = item.get('stationCode')
        plant_id = PLANT_MAP.get(p_code)
        if not plant_id: continue
        
        yield_kwh = float(item.get('inverter_power') or 0)
        consumption = float(item.get('use_power') or 0)
        export_kwh = float(item.get('on_grid_power') or 0)
        import_kwh = float(item.get('buy_power') or 0)
        self_use = float(item.get('self_use_power') or 0)
        spec = yield_kwh / PLANT_KWP.get(plant_id, 1)
        
        cursor.execute("""
            INSERT INTO fs.plant_daily_metrics 
            (plant_id, plant_code, date, fv_yield_kwh, consumption_kwh, exported_energy_kwh, imported_energy_kwh, self_consumption_kwh, specific_yield_kwh_kwp, source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'api_official', NOW())
            ON CONFLICT (plant_id, date) DO UPDATE SET
                fv_yield_kwh = EXCLUDED.fv_yield_kwh,
                consumption_kwh = EXCLUDED.consumption_kwh,
                exported_energy_kwh = EXCLUDED.exported_energy_kwh,
                imported_energy_kwh = EXCLUDED.imported_energy_kwh,
                self_consumption_kwh = EXCLUDED.self_consumption_kwh,
                specific_yield_kwh_kwp = EXCLUDED.specific_yield_kwh_kwp,
                source = 'api_official_update',
                updated_at = NOW()
        """, (plant_id, p_code, target_date, yield_kwh, consumption, export_kwh, import_kwh, self_use, spec))
        print(f"✅ {p_code}: Gen={yield_kwh} Cons={consumption}")
        count += 1

    conn.commit()
    conn.close()
    print(f"🏁 {count} plantas actualizadas.")

if __name__ == "__main__":
    sync_data()