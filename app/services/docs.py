# Service layer
from collections import defaultdict
from datetime import datetime, timezone
from aiobotocore.session import AioSession
from contextlib import AsyncExitStack
import io
import aiofiles
from uuid import uuid4
from fastapi import UploadFile
from fastapi.responses import StreamingResponse
from gridfs import NoFile
from pdf2image import convert_from_bytes
from app.models import (
    DocumentCreateMetadata,
    DocumentRead,
    DocumentPage,
    IndexedDocument,
    DocumentSearchResult,
    PartialDocument,
)
from elasticsearch import AsyncElasticsearch
from gridfs.asynchronous import AsyncGridFSBucket
from app.exceptions import (
    DocumentCreationError,
    DocumentIndexingError,
    DocumentDeleteError,
    DocumentNotFound,
    PreviewException,
    StreamError,
)
from app.clients import create_minio_s3_client
from dotenv import load_dotenv
import os

load_dotenv()

DOCS_INDEX = os.environ["DOCS_INDEX"]
PAGES_INDEX = os.environ["PAGES_INDEX"]
BUCKET = os.environ["BUCKET"]


class DocumentService:

    async def get_documents(self, es: AsyncElasticsearch) -> list[DocumentRead]:
        try:
            res = await es.search(index=DOCS_INDEX, query={"match_all": {}}, size=1000)
            return [DocumentRead(**hit["_source"]) for hit in res["hits"]["hits"]]
        except Exception as e:
            raise DocumentIndexingError(e)

    async def get_documents_by_sub(
        self, es: AsyncElasticsearch, sub: str
    ) -> list[DocumentRead]:
        try:
            res = await es.search(
                index=DOCS_INDEX, query={"term": {"sub": sub}}, size=1000
            )
            return [DocumentRead(**hit["_source"]) for hit in res["hits"]["hits"]]
        except Exception as e:
            raise DocumentIndexingError(e)

    async def get_document_by_id(
        self, es: AsyncElasticsearch, doc_id: str
    ) -> DocumentRead:
        try:
            res = await es.search(
                index=DOCS_INDEX,
                query={"term": {"doc_id": doc_id}},
                size=1,
            )
            hits = res["hits"]["hits"]
            if not hits:
                return DocumentNotFound
            return DocumentRead(**hits[0]["_source"])
        except Exception as e:
            raise DocumentIndexingError(e)

    async def get_documents_pages_by_id(
        self, es: AsyncElasticsearch, doc_id: str
    ) -> list[DocumentPage]:
        try:
            res = await es.search(
                index=PAGES_INDEX,
                body={"query": {"term": {"doc_id": doc_id}}, "size": 1000},
            )
            return [DocumentPage(**hit["_source"]) for hit in res["hits"]["hits"]]
        except Exception as e:
            raise DocumentIndexingError(e)

    async def delete_document_by_id(
        self,
        es: AsyncElasticsearch,
        doc_id: str,
    ) -> DocumentRead:
        # Step 1: Elimina le pagine associate all'ID
        try:
            await es.delete_by_query(
                index=PAGES_INDEX,
                body={"query": {"term": {"doc_id": doc_id}}},
                refresh=True,
                wait_for_completion=True,
            )
        except Exception as e:
            raise DocumentIndexingError(f"Errore durante delete_by_query: {e}")

        # Step 2: Recupera i metadati per restituirli al chiamante
        try:
            res = await es.get(index=DOCS_INDEX, id=doc_id)
            doc_data = DocumentRead(**res["_source"])
        except Exception as e:
            raise DocumentIndexingError(f"Errore nel recupero del documento: {e}")

        # Step 3: Elimina l'indice del documento
        try:
            await es.delete(index=DOCS_INDEX, id=doc_id, refresh=True)
        except Exception as e:
            raise DocumentIndexingError(
                f"Errore durante l'eliminazione del documento: {e}"
            )

        # Step 4: Elimina il file PDF da MinIO
        try:
            session = AioSession()
            async with AsyncExitStack() as exit_stack:
                s3_client = await create_minio_s3_client(session, exit_stack)
                resp = await s3_client.delete_object(Bucket=BUCKET, Key=doc_id)

        except Exception as e:
            raise StreamError(f"Errore durante la rimozione del file da MinIO: {e}")

        return resp

    async def create_document(
        self,
        metadata: DocumentCreateMetadata,
        file: UploadFile,
    ):

        from app.worker import index_document_task_sync

        doc_id = str(uuid4())
        folder = "/tmp"
        filename = f"/{doc_id}.pdf"
        temp_path = f"{folder}{filename}"
        key = f"/docs/{filename}"

        content = await file.read()
        
        # Salvo in FS (Per indicizzazione)
        try:
            async with aiofiles.open(temp_path, "wb") as out_file:
                await out_file.write(content)
        except Exception as e:
            raise DocumentCreationError(
                f"Errore durante il salvataggio temporaneo: {e}"
            )

        # Salvo in object storage (minIO)
        try:
            session = AioSession()
            async with AsyncExitStack() as exit_stack:
                s3_client = await create_minio_s3_client(session, exit_stack)
                # Salvo il file pdf
                await s3_client.put_object(
                    Bucket=BUCKET, Key=key, Body=content
                )
        except Exception as e:
            raise StreamError(
                f"Errore durante la creazione del documento in MinIO: {e}"
            )

        index_document_task_sync.delay(
            dict(
                IndexedDocument(
                    doc_id=doc_id,
                    path=temp_path,
                    sub=metadata.sub,
                    title=metadata.title,
                    author=metadata.author,
                    filename=metadata.filename,
                    created_at=datetime.now(timezone.utc),
                )
            )
        )

        return {
            "doc_id": doc_id,
            "message": "File uploaded, indexing in progress.",
        }

    async def query_documents(
        self, es: AsyncElasticsearch, q: str
    ) -> list[DocumentSearchResult]:
        page_query = {"match": {"text": {"query": q, "fuzziness": "AUTO"}}}
        highlight = {"fields": {"text": {}, "title": {}}}
        _source_pages = ["doc_id", "page", "text", "metadata"]

        page_res = await es.search(
            index=PAGES_INDEX,
            query=page_query,
            highlight=highlight,
            _source=_source_pages,
            size=100,
        )

        grouped = defaultdict(
            lambda: {
                "doc_id": None,
                "sub": None,
                "filename": None,
                "author": None,
                "title": None,
                "download_link": None,
                "metadata": {},
                "matching_pages": [],
                "title_match": False,
            }
        )

        doc_ids = set()

        for hit in page_res["hits"]["hits"]:
            doc = hit["_source"]
            highlights = hit.get("highlight", {})
            doc_id = doc["doc_id"]

            grouped[doc_id]["doc_id"] = doc_id
            grouped[doc_id]["matching_pages"].append(
                DocumentPage(
                    doc_id=doc_id,
                    page=doc["page"],
                    text=doc["text"],
                    metadata=doc.get("metadata", {}),
                    highlight=highlights,
                )
            )

            doc_ids.add(doc_id)

        if doc_ids:
            doc_query = {"terms": {"doc_id": list(doc_ids)}}

            _source_docs = [
                "doc_id",
                "title",
                "author",
                "filename",
                "download_link",
                "metadata",
                "sub",
            ]

            doc_res = await es.search(
                index=DOCS_INDEX,
                query=doc_query,
                highlight=highlight,
                _source=_source_docs,
                size=100,
            )

            for hit in doc_res["hits"]["hits"]:
                source = hit["_source"]
                doc_id = source["doc_id"]

                grouped[doc_id]["doc_id"] = doc_id
                grouped[doc_id]["title"] = source.get("title")
                grouped[doc_id]["download_link"] = source.get("download_link")
                grouped[doc_id]["metadata"] = source.get("metadata", {})
                grouped[doc_id]["author"] = source.get("author")
                grouped[doc_id]["filename"] = source.get("filename")
                grouped[doc_id]["sub"] = source.get("sub")

                if "highlight" in hit and "title" in hit["highlight"]:
                    grouped[doc_id]["title_match"] = True

        return list(grouped.values())

    async def update_document(
        self, es: AsyncElasticsearch, doc_id: str, metadata: PartialDocument
    ) -> None:
        try:
            res = await es.search(
                index=DOCS_INDEX,
                query={"term": {"doc_id": doc_id}},
                size=1,
            )
            hits = res["hits"]["hits"]
            if not hits:
                raise DocumentNotFound

            es_id = hits[0]["_id"]  # ID interno di Elasticsearch per l'update
            update_fields = metadata.model_dump(exclude_unset=True)

            if not update_fields:
                return  # niente da aggiornare

            await es.update(index=DOCS_INDEX, id=es_id, body={"doc": update_fields})

        except Exception as e:
            raise DocumentIndexingError(e)

    async def get_download_document(
        self, doc_id: str, download: bool
    ) -> StreamingResponse:
        filename = f"/{doc_id}.pdf"
        key = f"/docs/{filename}"
        
        try:
            session = AioSession()
            async with AsyncExitStack() as exit_stack:
                s3_client = await create_minio_s3_client(session, exit_stack)
                # Salvo il file pdf
                file = await s3_client.get_object(Bucket=BUCKET, Key=key)
                
        # except NoFile as e:
        #     raise DocumentNotFound(e)
        except Exception as e:
            raise StreamError(
                f"Errore durante il recupero del documento in MinIO: {e}"
            )
        except Exception as e:
            return Exception(e)
        if download:
            headers = {"Content-Disposition": f'attachment; filename="{file.filename}"'}
        else:
            headers = {"Content-Disposition": f'inline; filename="{file.filename}"'}

        return StreamingResponse(file, media_type="application/pdf", headers=headers)

    async def get_preview_document(
        self, fs: AsyncGridFSBucket, doc_id: str, preview_size: int = 1024 * 1024
    ) -> StreamingResponse:
        
        try:
            file = await fs.open_download_stream(doc_id)
            content = await file.read()
        except NoFile as n:
            raise DocumentNotFound(n)
        except Exception as e:
            return Exception(e)

        pages = convert_from_bytes(content, first_page=1, last_page=1)
        if not pages:
            raise PreviewException

        img = pages[0]
        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

        return StreamingResponse(img_bytes, media_type="image/png")
