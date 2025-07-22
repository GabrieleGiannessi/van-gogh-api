# worker.py
import asyncio
import os
from celery import Celery
from elasticsearch import AsyncElasticsearch
import pdfplumber
from app.clients import create_es_client
from elasticsearch.helpers import async_bulk
from app.exceptions import StreamError
from app.models import IndexedDocument

DOCS_INDEX = os.getenv("DOCS_INDEX", "docs")
PAGES_INDEX = os.getenv("PAGES_INDEX", "pages")
broker_url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
celery_app = Celery("pdf_tasks", broker=broker_url)


@celery_app.task
def index_document_task_sync(doc: dict):
    es = create_es_client()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_index_document_task(es, IndexedDocument(**doc)))
    finally:
        loop.run_until_complete(es.close())
        loop.close()


async def _index_document_task(es: AsyncElasticsearch, doc: IndexedDocument):
    # utilizzo la stringa che identifica il file temporaneo salvato per l'indicizzazione
    try:
        with pdfplumber.open(doc.path) as pdf:
            actions = []
            for i, page in enumerate(pdf.pages):
                try:
                    text = (page.extract_text() or "").strip()
                    if text:
                        actions.append(
                            {
                                "_op_type": "index",
                                "_index": PAGES_INDEX,
                                "_id": f"{doc.doc_id}_page_{i+1}",
                                "_source": {
                                    "doc_id": doc.doc_id,
                                    "page": i + 1,
                                    "text": text,
                                    "metadata": {
                                        "created_at": doc.created_at.isoformat()
                                    },
                                },
                            }
                        )
                except Exception as e:
                    raise StreamError(e)
        if actions:
            await async_bulk(es, actions, chunk_size=100, request_timeout=120)

        await es.index(
            index=DOCS_INDEX,
            id=doc.doc_id,
            body={
                "doc_id": doc.doc_id,
                "sub": doc.sub,
                "title": doc.title,
                "author": doc.author,
                "filename": doc.filename,
                "download_link": f"/download/{doc.doc_id}",
                "num_pages": len(pdf.pages),
                "metadata": {"created_at": doc.created_at.isoformat()},
            },
        )

    finally:
        try:
            os.remove(doc.path)  # cleanup file temporaneo
        except Exception as e:
            print(f"Errore nella rimozione del file temporaneo: {e}")
