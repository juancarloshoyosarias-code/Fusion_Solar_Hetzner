import psycopg2
import os
import sys
import traceback
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
DB_HOST = os.getenv('PGHOST')
DB_NAME = os.getenv('PGDATABASE')
DB_USER = os.getenv('PGUSER')
DB_PASS = os.getenv('PGPASSWORD')

# MAPEO DEFINITIVO (Validado)
PLANT_MAP = {
    "NE=33876570": 1,  # Cabañita
    "NE=33801790": 2,  # Maracaibo
    "NE=33745211": 3,  # Porvenir
    "NE=33758743": 4,  # Pozo 2
    "NE=33723010": 5,  # Pozo 1
    "NE=33788377": 6   # Casa Trejo
}

PLANT_KWP = {
    1: 116.0, 
    2: 116.0, 
    3: 143.0, 
    4: 116.0, 
    5: 119.56, 
    6: 9.765
}

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME, 
        user=DB_USER, 
        password=DB_PASS
    )

def main():
    # CRÍTICO: Procesar día AYER (completo), no HOY (incompleto)
    # Si servidor está en UTC (01:00 AM), procesamos la fecha de ayer.
    target_date = (datetime.now() - timedelta(days=1)).date()
    yesterday = target_date - timedelta(days=1)
    
    print(f"\n{'='*60}")
    print(f"CONSOLIDACIÓN FALLBACK MATEMÁTICO")
    print(f"Fecha objetivo: {target_date}")
    print(f"{'='*60}\n")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener acumulados de HOY desde raw
        # CORRECCIÓN SQL: Usamos ::date en lugar de date()
        cursor.execute("""
            SELECT plant_code, MAX(total_power_kwh), MAX(day_power_kwh)
            FROM raw.fs_realtime_plants
            WHERE (ts_utc AT TIME ZONE 'America/Bogota')::date = %s
            GROUP BY plant_code
        """, (target_date,))
        
        rows_today = cursor.fetchall()
        
        if not rows_today:
            print("⚠️ No hay datos en raw.fs_realtime_plants para", target_date)
            sys.exit(1)
        
        processed = 0
        
        for row in rows_today:
            p_code, total_today, raw_day = row
            plant_id = PLANT_MAP.get(p_code)
            
            if not plant_id or total_today is None:
                continue
            
            final_yield = 0.0
            method = "raw_unknown"
            
            # --- CASO ESPECIAL: CASA TREJO (ID 6) ---
            if plant_id == 6:
                # Buscar el acumulado FINAL OFICIAL de ayer en la tabla consolidada
                cursor.execute("""
                    SELECT cumulative_energy_kwh 
                    FROM fs.plant_daily_metrics 
                    WHERE plant_id = 6 AND date = %s
                """, (yesterday,))
                
                row_prev = cursor.fetchone()
                
                if row_prev and row_prev[0]:
                    prev_accum = float(row_prev[0])
                    diff = float(total_today) - prev_accum
                    
                    # Filtro estricto: Máximo 60 kWh/día
                    if 0 <= diff <= 60:
                        final_yield = diff
                        method = "math_casa_trejo"
                    else:
                        final_yield = float(raw_day or 0)
                        method = "raw_fallback_outlier"
                        print(f"⚠️ Casa Trejo: Diferencia sospechosa ({diff:.2f}), usando raw")
                else:
                    final_yield = float(raw_day or 0)
                    method = "raw_no_history"
            
            # --- PLANTAS NORMALES (ID 1-5) ---
            else:
                # Buscamos el acumulado RAW de ayer
                # CORRECCIÓN SQL: Usamos ::date
                cursor.execute("""
                    SELECT MAX(total_power_kwh) 
                    FROM raw.fs_realtime_plants 
                    WHERE plant_code = %s 
                      AND (ts_utc AT TIME ZONE 'America/Bogota')::date = %s
                """, (p_code, yesterday))
                
                row_raw_prev = cursor.fetchone()
                
                if row_raw_prev and row_raw_prev[0]:
                    prev_accum = float(row_raw_prev[0])
                    diff = float(total_today) - prev_accum
                    
                    # Filtro Dinámico: Capacidad * 6.5 Horas Sol Pico
                    max_expected = PLANT_KWP[plant_id] * 6.5
                    
                    if 0 <= diff <= max_expected:
                        final_yield = diff
                        method = "math_calc"
                    else:
                        final_yield = float(raw_day or 0)
                        method = "raw_fallback_outlier"
                        print(f"⚠️ {p_code}: Diferencia {diff:.2f} > Máx {max_expected:.2f}")
                else:
                    final_yield = float(raw_day or 0)
                    method = "raw_no_history"

            # INSERTAR / ACTUALIZAR
            if final_yield >= 0:
                kwp = PLANT_KWP.get(plant_id, 1)
                spec = round(final_yield / kwp, 3)
                
                cursor.execute("""
                    INSERT INTO fs.plant_daily_metrics 
                    (plant_id, plant_code, date, fv_yield_kwh, inverter_yield_kwh, 
                     cumulative_energy_kwh, specific_yield_kwh_kwp, source, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (plant_id, date) DO UPDATE SET
                        fv_yield_kwh = EXCLUDED.fv_yield_kwh,
                        inverter_yield_kwh = EXCLUDED.inverter_yield_kwh,
                        cumulative_energy_kwh = EXCLUDED.cumulative_energy_kwh,
                        specific_yield_kwh_kwp = EXCLUDED.specific_yield_kwh_kwp,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                """, (plant_id, p_code, target_date, final_yield, final_yield, 
                      total_today, spec, f'fallback_{method}'))
                
                print(f"✅ ID={plant_id} ({p_code}): {final_yield:.2f} kWh [{method}]")
                processed += 1

        conn.commit()
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"✅ Completado: {processed} plantas procesadas")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"❌ Error Crítico: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
