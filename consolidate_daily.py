import psycopg2
import os
import sys
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
DB_HOST = os.getenv('PGHOST')
DB_NAME = os.getenv('PGDATABASE')
DB_USER = os.getenv('PGUSER')
DB_PASS = os.getenv('PGPASSWORD')

# MAPEO DEFINITIVO (Corregido 02-Dic-2025)
PLANT_MAP = {
    "NE=33876570": 1, "NE=33801790": 2, "NE=33745211": 3,
    "NE=33758743": 4, "NE=33723010": 5, "NE=33788377": 6
}
PLANT_KWP = {1: 116.0, 2: 116.0, 3: 143.0, 4: 116.0, 5: 119.56, 6: 9.765}

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def main():
    target_date = datetime.now().date()
    print(f"--- CONSOLIDACIÓN MATEMÁTICA: {target_date} ---")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Obtener Acumulado de HOY
        cursor.execute("""
            SELECT plant_code, MAX(total_power_kwh), MAX(day_power_kwh)
            FROM raw.fs_realtime_plants
            WHERE date(ts_utc AT TIME ZONE 'America/Bogota') = %s
            GROUP BY plant_code
        """, (target_date,))
        
        for row in cursor.fetchall():
            p_code, total_today, raw_day = row
            plant_id = PLANT_MAP.get(p_code)
            if not plant_id or total_today is None: continue

            # 2. Obtener Acumulado de AYER
            cursor.execute("""
                SELECT MAX(total_power_kwh) FROM raw.fs_realtime_plants 
                WHERE plant_code = %s AND date(ts_utc AT TIME ZONE 'America/Bogota') = %s
            """, (p_code, target_date - timedelta(days=1)))
            
            row_yesterday = cursor.fetchone()
            final_yield = 0.0
            method = "raw"

            if row_yesterday and row_yesterday[0]:
                diff = float(total_today) - float(row_yesterday[0])
                if 0 <= diff < 1500: 
                    final_yield = diff
                    method = "math"
                else:
                    final_yield = float(raw_day or 0)
            else:
                final_yield = float(raw_day or 0)

            if final_yield >= 0:
                kwp = PLANT_KWP.get(plant_id, 1)
                spec = final_yield / kwp
                
                cursor.execute("""
                    INSERT INTO fs.plant_daily_metrics 
                    (plant_id, plant_code, date, fv_yield_kwh, cumulative_energy_kwh, specific_yield_kwh_kwp, source, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (plant_id, date) DO UPDATE SET
                        fv_yield_kwh = EXCLUDED.fv_yield_kwh,
                        cumulative_energy_kwh = EXCLUDED.cumulative_energy_kwh,
                        specific_yield_kwh_kwp = EXCLUDED.specific_yield_kwh_kwp,
                        source = EXCLUDED.source
                """, (plant_id, p_code, target_date, final_yield, total_today, spec, f'auto_{method}'))
                print(f"✅ Planta {plant_id}: {final_yield:.2f} kWh ({method})")

        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()