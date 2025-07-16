# app/clients.py
from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient
from app.models import Settings

settings = Settings()

# Funzione factory per creare ES client
def create_es_client():
    return AsyncElasticsearch(settings.es_host)

# Funzione factory per creare MongoClient
def create_mongo_client():
    return AsyncMongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
