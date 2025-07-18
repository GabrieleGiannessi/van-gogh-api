from elasticsearch import AsyncElasticsearch
from fastapi import Request, Depends
from typing import Annotated
from gridfs import AsyncGridFSBucket
from app.services.docs import DocumentService

def get_es_client(request: Request) -> AsyncElasticsearch:
    return request.app.state.es 

def get_document_service():
    return DocumentService()

document_service_dependency = Annotated[DocumentService, Depends(get_document_service)]
elasticSearch_dependency = Annotated[AsyncElasticsearch, Depends(get_es_client)]