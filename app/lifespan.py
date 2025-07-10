from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from elasticsearch import AsyncElasticsearch
from app.dependency import settings

logger = logging.getLogger(__name__)

DOCS_INDEX = "docs"
PAGES_INDEX = "pages"


async def init_indices(es: AsyncElasticsearch):
    index_configs = {
        DOCS_INDEX: {
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "sub": {"type": "keyword"},
                    "title": {"type": "text"},
                    "author": {"type": "text"},
                    "filename": {"type": "text"},
                    "num_pages": {"type": "integer"},
                    "metadata": {"properties": {"created_at": {"type": "date"}}},
                }
            }
        },
        PAGES_INDEX: {
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "page": {"type": "integer"},
                    "text": {"type": "text"},
                    "metadata": {"properties": {"created_at": {"type": "date"}}},
                }
            }
        },
    }

    for index_name, body in index_configs.items():
        exists = await es.indices.exists(index=index_name)
        if not exists:
            await es.indices.create(index=index_name, body=body)
            logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index already exists: {index_name}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    es = AsyncElasticsearch(settings.es_host)
    await init_indices(es)
    yield
    await es.close()
