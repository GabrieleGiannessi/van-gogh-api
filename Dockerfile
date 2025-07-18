FROM python:3.11-slim

WORKDIR /app

# Installa le dipendenze
COPY requirements.txt .

RUN pip install -U pip && pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY . .

