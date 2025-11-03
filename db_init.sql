-- Activa Timescale (tu instancia ya lo soporta)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ===== Dimensiones =====
CREATE SCHEMA IF NOT EXISTS dim;
CREATE TABLE IF NOT EXISTS dim.fs_plants (
  plant_code  text PRIMARY KEY,  -- NE=xxxxx
  plant_name  text,
  capacity_kw numeric
);

-- ===== Datos crudos (planta) =====
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.fs_realtime_plants (
  ts_utc           timestamptz NOT NULL,
  plant_code       text        NOT NULL,
  plant_name       text,
  power_kw         numeric,              -- potencia instantánea
  day_power_kwh    numeric,
  month_power_kwh  numeric,
  total_power_kwh  numeric,
  health           int,                  -- 1=OFF,2=FALLA,3=OK
  PRIMARY KEY (ts_utc, plant_code)
);
SELECT create_hypertable('raw.fs_realtime_plants','ts_utc',
                         if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 day');

CREATE TABLE IF NOT EXISTS raw.fs_plants_last (
  plant_code       text PRIMARY KEY,
  updated_utc      timestamptz NOT NULL,
  plant_name       text,
  power_kw         numeric,
  day_power_kwh    numeric,
  month_power_kwh  numeric,
  total_power_kwh  numeric,
  health           int
);

-- ===== Datos crudos (medidor) =====
CREATE TABLE IF NOT EXISTS raw.fs_meter_realtime (
  ts_utc     timestamptz NOT NULL,
  plant_code text        NOT NULL,
  import_kw  numeric,
  export_kw  numeric,
  load_kw    numeric,
  self_use_kw numeric,
  PRIMARY KEY (ts_utc, plant_code)
);
SELECT create_hypertable('raw.fs_meter_realtime','ts_utc',
                         if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 day');

-- ===== Índices =====
CREATE INDEX IF NOT EXISTS idx_fs_realtime_plants ON raw.fs_realtime_plants(plant_code, ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_fs_meter_realtime   ON raw.fs_meter_realtime(plant_code, ts_utc DESC);

-- ===== Políticas de compresión y retención =====
ALTER TABLE raw.fs_realtime_plants SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'plant_code'
);
ALTER TABLE raw.fs_meter_realtime SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'plant_code'
);
SELECT add_compression_policy('raw.fs_realtime_plants', INTERVAL '7 days');
SELECT add_compression_policy('raw.fs_meter_realtime',  INTERVAL '7 days');
SELECT add_retention_policy('raw.fs_realtime_plants',  INTERVAL '180 days');
SELECT add_retention_policy('raw.fs_meter_realtime',   INTERVAL '180 days');

-- Asegura columna de autoconsumo en despliegues existentes
ALTER TABLE raw.fs_meter_realtime
  ADD COLUMN IF NOT EXISTS self_use_kw numeric;


