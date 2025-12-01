FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todos los scripts
COPY fusion_api.py .
COPY consolidate_daily.py .
COPY backfill_history.py .
COPY db_init.sql .
COPY init_db.py .
COPY test_db.py .
COPY test_env.py .

# El contenedor ejecutará fusion_api.py por defecto (loop continuo)
# consolidate_daily.py se ejecutará mediante cron programado en Coolify
CMD ["python3", "fusion_api.py"]