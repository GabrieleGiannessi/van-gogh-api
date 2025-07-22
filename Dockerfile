FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y poppler-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Installa le dipendenze
COPY requirements.txt .

RUN pip install -U pip && pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY . .

