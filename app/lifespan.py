import asyncio
from contextlib import asynccontextmanager
import logging
import os
from fastapi import FastAPI
from elasticsearch import AsyncElasticsearch
from aiobotocore.session import get_session
from elastic_transport import ConnectionError
from app.clients import create_es_client
from dotenv import load_dotenv

load_dotenv()


DOCS_INDEX = os.environ["DOCS_INDEX"]
PAGES_INDEX = os.environ["PAGES_INDEX"]
logger = logging.getLogger(__name__)


async def wait_for_elasticsearch(es: AsyncElasticsearch, retries=10, delay=1):
    for attempt in range(1, retries + 1):
        try:
            if await es.ping():
                logger.info("✅ Elasticsearch è pronto.")
                return
        except ConnectionError:
            logger.info(
                f"🔄 Tentativo {attempt}: Elasticsearch non ancora pronto, ritento tra {delay} secondi..."
            )
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


async def create_minio_bucket():
    session = get_session()

    async with session.create_client(
        "s3",
        region_name="us-east-1",
        endpoint_url=os.environ["MINIO_CLIENT_URL"],
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    ) as s3_client:
        bucket_name = os.environ["BUCKET"]
        try:
            await s3_client.head_bucket(Bucket=bucket_name)
        except s3_client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ["404", "NoSuchBucket"]:
                await s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"✅ Bucket '{bucket_name}' creato.")
            else:
                logger.error(f"❌ Errore durante la verifica del bucket: {e}")
                raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    es = create_es_client()
    app.state.es = es

    try:
        await wait_for_elasticsearch(es)
        await init_indices(es)
        await create_minio_bucket()
        yield
    except RuntimeError as e:
        logger.error(str(e))
        raise
    finally:
        await es.close()
