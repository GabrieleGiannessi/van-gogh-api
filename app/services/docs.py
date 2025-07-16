# Service layer
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
import io
from uuid import uuid4
from fastapi import UploadFile
from fastapi.responses import StreamingResponse
from gridfs import NoFile
from pdf2image import convert_from_bytes
import pdfplumber
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
from elasticsearch.helpers import async_bulk
from app.exceptions import (
    DocumentCreationError,
    DocumentIndexingError,
    DocumentDeleteError,
    DocumentNotFound,
    PreviewException,
    StreamError,
)

DOCS_INDEX = "docs"
PAGES_INDEX = "pages"


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
        self, fs: AsyncGridFSBucket, es: AsyncElasticsearch, doc_id: str
    ) -> DocumentRead:
        try:
            await es.delete_by_query(
                index=PAGES_INDEX,
                body={"query": {"match": {"doc_id": doc_id}}},
                refresh=True,
                wait_for_completion=True,
            )
            try:
                res = await es.get(index=DOCS_INDEX, id=doc_id)
                doc_data = DocumentRead(**res["_source"])
            except Exception as e:
                raise DocumentIndexingError(e)

            await es.delete(index=DOCS_INDEX, id=doc_id, refresh=True)
            try:
                file = await fs.open_download_stream(doc_id)
            except NoFile as e:
                raise StreamError(e)

            await fs.delete(file._id)

            return doc_data

        except Exception as e:
            raise DocumentDeleteError(e)

    async def create_document(
        self,
        fs: AsyncGridFSBucket,
        es: AsyncElasticsearch,
        metadata: DocumentCreateMetadata,
        file: UploadFile,
    ):
        
        from app.worker import index_document_task_sync

        doc_id = str(uuid4())
        temp_path = f"/tmp/{doc_id}.pdf"
        
        #Salvo in FS (Per indicizzazione)
        try:
            with open(temp_path, "wb") as out_file:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    out_file.write(chunk)
        except Exception as e:
            raise DocumentCreationError(f"Errore durante il salvataggio temporaneo: {e}")

        #Salvo in GridFS
        try:
            async with fs.open_upload_stream_with_id(
                doc_id,
                metadata.filename,
                chunk_size_bytes=1024 * 256,
                metadata={"contentType": "application/pdf"},
            ) as grid_in:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    await grid_in.write(chunk)
        except Exception as e:
            await fs.delete(grid_in._id)
            raise DocumentCreationError(e)

        index_document_task_sync.delay(
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
        
        return {
            "doc_id": doc_id,
            "message": "File uploaded, indexing in progress.",
            "download_link": f"/download/{doc_id}",
        }

    # async def index_document(
    #     self, fs: AsyncGridFSBucket, es: AsyncElasticsearch, doc: IndexedDocument
    # ) -> None:
    #     try:
    #         grid_out = await fs.open_download_stream(
    #             doc.doc_id
    #         )  # Prendo i dati dal db riferiti dall' _id del documento
    #         content = await grid_out.read()
    #     except Exception as e:
    #         raise StreamError(message=e)

    #     with pdfplumber.open(BytesIO(content)) as pdf:
    #         actions = []
    #         for i, page in enumerate(pdf.pages):
    #             try:
    #                 text = (page.extract_text() or "").strip()
    #                 if text:
    #                     actions.append(
    #                         {
    #                             "_op_type": "index",
    #                             "_index": PAGES_INDEX,
    #                             "_id": f"{doc.doc_id}_page_{i+1}",
    #                             "_source": {
    #                                 "doc_id": doc.doc_id,
    #                                 "page": i + 1,
    #                                 "text": text,
    #                                 "metadata": {
    #                                     "created_at": doc.created_at.isoformat()
    #                                 },
    #                             },
    #                         }
    #                     )
    #             except Exception as e:
    #                 raise StreamError(e)

    #         if actions:
    #             await async_bulk(es, actions, chunk_size=100, request_timeout=120)

    #         await es.index(
    #             index=DOCS_INDEX,
    #             id=doc.doc_id,
    #             body={
    #                 "doc_id": doc.doc_id,
    #                 "sub": doc.sub,
    #                 "title": doc.title,
    #                 "author": doc.author,
    #                 "filename": doc.filename,
    #                 "download_link": f"/download/{doc.doc_id}",
    #                 "num_pages": len(pdf.pages),
    #                 "metadata": {"created_at": doc.created_at.isoformat()},
    #             },
    #         )

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
        self, fs: AsyncGridFSBucket, doc_id: str, download: bool
    ) -> StreamingResponse:
        try:
            file = await fs.open_download_stream(doc_id)
        except NoFile as e:
            raise DocumentNotFound(e)
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
