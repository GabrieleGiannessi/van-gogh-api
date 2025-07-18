# app/clients.py
from contextlib import AsyncExitStack
from elasticsearch import AsyncElasticsearch
from app.models import Settings
from aiobotocore.session import AioSession
from dotenv import load_dotenv
import os

load_dotenv()

settings = Settings()


# Factory per S3
async def create_minio_s3_client(session: AioSession, exit_stack: AsyncExitStack):
    client = await exit_stack.enter_async_context(
        session.create_client(
            "s3",
            region_name="us-east-1",
            endpoint_url=os.environ["MINIO_CLIENT_URL"],
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        )
    )
    return client


# Funzione factory per creare ES client
def create_es_client():
    return AsyncElasticsearch(settings.es_host)
