import asyncio
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI
from elasticsearch import AsyncElasticsearch
from gridfs import AsyncGridFSBucket
from elastic_transport import ConnectionError
from app.clients import create_es_client, create_mongo_client

logger = logging.getLogger(__name__)

DOCS_INDEX = "docs"
PAGES_INDEX = "pages"

async def wait_for_elasticsearch(es: AsyncElasticsearch, retries=10, delay=1):
    for attempt in range(1, retries + 1):
        try:
            if await es.ping():
                logger.info("✅ Elasticsearch è pronto.")
                return
        except ConnectionError:
            logger.info(f"🔄 Tentativo {attempt}: Elasticsearch non ancora pronto, ritento tra {delay} secondi...")
        await asyncio.sleep(delay)
        delay *= 2  # backoff esponenziale
    raise RuntimeError("❌ Elasticsearch non è disponibile dopo diversi tentativi.")

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
    es = create_es_client()
    app.state.es = es

    mongo_client = create_mongo_client()
    app.state.mongo_client = mongo_client
    app.state.fs = AsyncGridFSBucket(mongo_client.documents_db, "documents")

    try:
        await wait_for_elasticsearch(es)
        await init_indices(es)
        yield
    except RuntimeError as e:
        logger.error(str(e))
        raise
    finally:
        await es.close()
        await mongo_client.close()
