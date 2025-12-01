#!/usr/bin/env python3
"""
Consolidación diaria: raw.fs_realtime_plants → fs.plant_daily_metrics
Ejecutar diariamente a las 19:00 COT (00:00 UTC) para consolidar el día actual
"""

import os
import sys
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv(".env")

# Mapeo plant_code → plant_id
PLANT_MAP = {
    "NE=33723010": 1,  # Cabañita
    "NE=33758743": 2,  # Maracaibo
    "NE=33788377": 3,  # Porvenir
    "NE=33876570": 4,  # Pozo 2
    "NE=33801790": 5,  # Pozo 1
    "NE=33745211": 6,  # Casa Trejo
}

# Capacidades kWp
PLANT_KWP = {
    1: 116.0,    # Cabañita
    2: 116.0,    # Maracaibo
    3: 143.0,    # Porvenir
    4: 116.0,    # Pozo 2
    5: 119.56,   # Pozo 1
    6: 9.765,    # Casa Trejo
}

def get_db_connection():
    """Conecta a PostgreSQL usando variables de entorno"""
    return psycopg2.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        sslmode=os.getenv("PGSSLMODE", "disable"),
    )

def consolidate_today():
    """Consolida datos del día actual (ejecutar a las 19:00 COT cuando ya no hay sol)"""
    # Colombia está en UTC-5 (sin cambio de horario)
    bogota_tz = timezone(timedelta(hours=-5))
    today_bogota = datetime.now(bogota_tz).date()
    
    print(f"🚀 Consolidando datos del {today_bogota} (ejecutado a las 19:00 COT)")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Obtener datos agregados del día
        cur.execute("""
            SELECT 
                plant_code,
                MAX(day_power_kwh) AS day_yield,
                MAX(total_power_kwh) AS cumulative
            FROM raw.fs_realtime_plants
            WHERE (ts_utc AT TIME ZONE 'America/Bogota')::date = %s
            GROUP BY plant_code
        """, (today_bogota,))
        
        rows = cur.fetchall()
        
        if not rows:
            print(f"⚠️  Sin datos para {today_bogota}")
            return
        
        # Insertar/actualizar en plant_daily_metrics
        count = 0
        for plant_code, day_yield, cumulative in rows:
            plant_id = PLANT_MAP.get(plant_code)
            if not plant_id:
                print(f"⚠️  Plant code desconocido: {plant_code}")
                continue
            
            kwp = PLANT_KWP[plant_id]
            specific_yield = round(float(day_yield) / kwp, 2) if day_yield else None
            
            cur.execute("""
                INSERT INTO fs.plant_daily_metrics (
                    plant_id,
                    plant_code,
                    date,
                    fv_yield_kwh,
                    inverter_yield_kwh,
                    cumulative_energy_kwh,
                    specific_yield_kwh_kwp,
                    source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (plant_id, date) DO UPDATE SET
                    fv_yield_kwh = EXCLUDED.fv_yield_kwh,
                    inverter_yield_kwh = EXCLUDED.inverter_yield_kwh,
                    cumulative_energy_kwh = EXCLUDED.cumulative_energy_kwh,
                    specific_yield_kwh_kwp = EXCLUDED.specific_yield_kwh_kwp,
                    source = EXCLUDED.source
            """, (
                plant_id,
                plant_code,
                today_bogota,
                day_yield,
                day_yield,  # inverter_yield = fv_yield
                cumulative,
                specific_yield,
                'consolidate_daily_auto'
            ))
            count += 1
        
        conn.commit()
        print(f"✅ Consolidadas {count} plantas para {today_bogota}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    consolidate_today()