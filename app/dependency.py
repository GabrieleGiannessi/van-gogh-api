from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from typing import Annotated

from gridfs import AsyncGridFSBucket
from pymongo import AsyncMongoClient
from app.models import Settings
from app.service import DocumentService

settings = Settings()

def get_es_client():
    return AsyncElasticsearch(settings.es_host)


def get_mongo_client():
    client = AsyncMongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    return client


def get_db() -> AsyncGridFSBucket:
    mongo_client = get_mongo_client()
    documents_db = mongo_client.documents_db
    return AsyncGridFSBucket(documents_db, "documents")


def get_document_service():
    return DocumentService()

#Dependency
document_service_dependency = Annotated[DocumentService, Depends(get_document_service)]
elasticSearch_dependency = Annotated[AsyncElasticsearch, Depends(get_es_client)]
fs_dependency =  Annotated[AsyncGridFSBucket, Depends(get_db)]

