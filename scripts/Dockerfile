# pipeline/Dockerfile
FROM python:3.11-slim

# variables d'environnement (par sécurité)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# dépendances
COPY requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc libpq-dev --no-install-recommends \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && apt-get remove -y gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# copier le code
COPY . /app

# point d'entrée par défaut (on appellera les scripts depuis docker-compose ou docker exec)
CMD ["python", "app.py"]
