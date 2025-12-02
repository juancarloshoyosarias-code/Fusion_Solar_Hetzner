def sync_data():
    print(f"\n{'='*60}")
    print(f"SINCRONIZACIÓN API OFICIAL: {datetime.now()}")
    print(f"{'='*60}\n")
    
    token = login()
    headers = {"XSRF-TOKEN": token}
    
    target_date = datetime.now().date() - timedelta(days=1)
    collect_time = int(datetime(target_date.year, target_date.month, target_date.day).timestamp()) * 1000
    
    print(f"📅 Fecha objetivo: {target_date}")
    
    station_codes = ",".join(PLANT_MAP.keys())
    url = f"{BASE_URL}/getKpiStationDay"
    payload = {"stationCodes": station_codes, "collectTime": collect_time}
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        api_data = resp.json()
    except Exception as e:
        print(f"❌ Error consultando API: {e}")
        sys.exit(1)
    
    if not api_data.get('success'):
        print(f"❌ Error API Huawei: {api_data}")
        sys.exit(1)
    
    raw_list = api_data.get('data', [])
    
    if not raw_list:
        print("⚠️ API retornó lista vacía")
        sys.exit(1)
    
    # DEBUGGING: Ver estructura completa del primer item
    print("\n🔍 PRIMER ITEM COMPLETO:")
    print(json.dumps(raw_list[0], indent=2, ensure_ascii=False))
    print("-" * 60)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    inserted = 0
    updated = 0
    
    for item in raw_list:
        p_code = item.get('stationCode')
        plant_id = PLANT_MAP.get(p_code)
        
        if not plant_id:
            print(f"⚠️ plant_code desconocido: {p_code}")
            continue
        
        # IMPORTANTE: Ajustar según estructura real
        # Opción A: Datos directos en el item
        data_map = item.get('dataItemMap', {})
        
        # Si los datos están en dataItemMap:
        if data_map:
            yield_kwh = float(data_map.get('inverter_power', 0) or 0)
            consumption = float(data_map.get('use_power', 0) or 0)
            export_kwh = float(data_map.get('on_grid_power', 0) or 0)
            import_kwh = float(data_map.get('buy_power', 0) or 0)
            self_use = float(data_map.get('self_use_power', 0) or 0)
        # Si los datos están directos:
        else:
            yield_kwh = float(item.get('inverter_power') or item.get('daily_energy') or 0)
            consumption = float(item.get('use_power') or 0)
            export_kwh = float(item.get('on_grid_power') or 0)
            import_kwh = float(item.get('buy_power') or 0)
            self_use = float(item.get('self_use_power') or 0)
        
        kwp = PLANT_KWP.get(plant_id, 1)
        spec = round(yield_kwh / kwp, 3) if kwp > 0 else None
        
        # Verificar existencia
        cursor.execute("""
            SELECT 1 FROM fs.plant_daily_metrics 
            WHERE plant_id = %s AND date = %s
        """, (plant_id, target_date))
        exists = cursor.fetchone()
        
        # UPSERT
        cursor.execute("""
            INSERT INTO fs.plant_daily_metrics 
            (plant_id, plant_code, date, fv_yield_kwh, consumption_kwh, 
             exported_energy_kwh, imported_energy_kwh, self_consumption_kwh, 
             specific_yield_kwh_kwp, source, created_at)
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
        """, (plant_id, p_code, target_date, yield_kwh, consumption, 
              export_kwh, import_kwh, self_use, spec))
        
        status = "🔄 UPDATE" if exists else "✅ INSERT"
        print(f"{status} ID={plant_id} | Gen={yield_kwh:.2f} Cons={consumption:.2f} Exp={export_kwh:.2f} Imp={import_kwh:.2f} Self={self_use:.2f}")
        
        if exists:
            updated += 1
        else:
            inserted += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETADO: {inserted} nuevos | {updated} actualizados")
    print(f"{'='*60}\n")
```

---

## 🎯 ACCIÓN INMEDIATA

**Ejecuta el script AHORA** y pégame la salida completa, especialmente la parte de:
```
🔍 PRIMER ITEM COMPLETO:
{...}
