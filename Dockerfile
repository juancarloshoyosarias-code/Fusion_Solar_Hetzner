# Imagen base
FROM python:3.11-slim

# Evitar bytecode / cache y hacer logs sin buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Bogota

# Dependencias del sistema (psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libpq-dev ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Requisitos primero (mejor cache)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Código
COPY . /app

# Usuario no root (opcional, seguro)
RUN useradd -m worker && chown -R worker:worker /app
USER worker

# No exponemos puertos: es un worker
# HEALTHCHECK mínimo: verifica que el proceso siga vivo (si se desea)
# HEALTHCHECK NONE

# Comando de arranque
CMD ["python", "fusion_api.py"]
