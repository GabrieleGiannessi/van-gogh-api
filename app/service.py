# Service layer
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4
from fastapi import HTTPException
import pdfplumber
from app.models import DocumentCreate, DocumentRead, DocumentPage, IndexedDocument
from elasticsearch import AsyncElasticsearch
from gridfs.asynchronous import AsyncGridFSBucket
from elasticsearch.helpers import async_bulk
from app.exceptions import DocumentCreationError, DocumentIndexingError, PDFExtractionError, StreamError
DOCS_INDEX = "docs"
PAGES_INDEX = "pages"


class DocumentService:
    #
    # TODO:
    #       - spostare la logica di business da `routes.py` a questo servizio
    #       - importare tramite dependency injection il servizio nelle route
    #       - logica di modifica del documento
    #
    #

    async def get_documents(self, es: AsyncElasticsearch) -> list[DocumentRead]:
        try:
            res = await es.search(index=DOCS_INDEX, query={"match_all": {}}, size=1000)
            return [DocumentRead(hit["_source"]) for hit in res["hits"]["hits"]]
        except Exception as e:
            raise DocumentIndexingError(message=e)

    async def get_documents_by_sub(self, sub: str) -> DocumentRead | None:
        pass

    async def get_documents_by_id(self, doc_id: str) -> DocumentRead | None:
        pass

    async def get_documents_pages_by_id(self, doc_id: str) -> list[DocumentPage]:
        pass

    async def delete_document_by_id(self, doc_id: str) -> DocumentRead:
        pass

    async def create_document(
        self, fs: AsyncGridFSBucket, es: AsyncElasticsearch, doc: DocumentCreate
    ) -> None:

        doc_id = str(uuid4())

        try:
            async with fs.open_upload_stream_with_id(
                doc_id,
                doc.file.filename,
                chunk_size_bytes=4,
                metadata={"contentType": "application/pdf"},
            ) as grid_in:
                while True:
                    try:
                        chunk = await doc.file.read(1024 * 1024)
                        if not chunk:
                            break
                        await grid_in.write(chunk)
                    except Exception as e:
                        await fs.delete(grid_in._id)
                        raise DocumentCreationError(message=e)
                    finally:
                        await grid_in.close()
        except Exception as e:
            raise Exception(message=e)

        try:
            await self.index_document(
                fs,
                es,
                IndexedDocument(
                    doc_id=doc_id,
                    sub=doc.sub,
                    title=doc.title,
                    filename=doc.file.filename,
                    created_at=datetime.now(timezone.utc),
                ),
            )
        except Exception as e:
            raise DocumentIndexingError(message=e)

        return {
            "doc_id": doc_id,
            "message": "File uploaded and indexed.",
            "download_link": f"/download/{doc_id}",
        }

    async def index_document(
        self, fs: AsyncGridFSBucket, es: AsyncElasticsearch, doc: IndexedDocument
    ) -> None:
        try:
            grid_out = await fs.open_download_stream(
                doc.doc_id
            )  # Prendo i dati dal db riferiti dall' _id del documento
            content = await grid_out.read()
        except Exception as e:
            raise StreamError(message=e)

        with pdfplumber.open(BytesIO(content)) as pdf:
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
                    raise StreamError(message=e)

            if actions:
                await async_bulk(es, actions, chunk_size=100, request_timeout=60)

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
