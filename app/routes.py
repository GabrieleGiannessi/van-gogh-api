from contextlib import asynccontextmanager
import io
import logging
from uuid import uuid4
from datetime import datetime, timezone
from collections import defaultdict
from io import BytesIO

from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    HTTPException,
    Query,
    UploadFile,
    Form,
)
from fastapi.responses import StreamingResponse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from gridfs import NoFile
from pymongo import AsyncMongoClient
from gridfs.asynchronous import AsyncGridFSBucket
import pdfplumber
from pdf2image import convert_from_bytes
from app.models import Settings, DocumentCreate, DocumentRead
from app.service import DocumentService

logger = logging.getLogger(__name__)

settings = Settings()

fs = None
documents_db = None
es = None

DOCS_INDEX = "docs"
PAGES_INDEX = "pages"


def get_es_client():
    return Elasticsearch(settings.es_host)


async def get_mongo_client():
    client = AsyncMongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    return client

def get_document_service(): 
    return DocumentService()


def init_indices():
    index_configs = {
        DOCS_INDEX: {
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "sub": {"type": "keyword"},
                    "title": {"type": "text"},
                    "author": {"type": "text"},
                    "filename": {"type": "text"},
                    "download_link": {"type": "keyword"},
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
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=body)
            logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index already exists: {index_name}")


@asynccontextmanager
async def lifespan(router: APIRouter):
    global fs, documents_db, es
    mongo_client = await get_mongo_client()
    es = get_es_client()
    documents_db = mongo_client.documents_db
    fs = AsyncGridFSBucket(documents_db, "documents")

    init_indices()

    yield


router = APIRouter(lifespan=lifespan)


async def index_pdf_pages(doc_id, filename, sub, title, author, created_at):
    try:
        grid_out = await fs.open_download_stream(
            doc_id
        )  # Prendo i dati dal db riferiti dall' _id del documento
        content = await grid_out.read()
    except Exception:
        return

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
                            "_id": f"{doc_id}_page_{i+1}",
                            "_source": {
                                "doc_id": doc_id,
                                "page": i + 1,
                                "text": text,
                                "metadata": {"created_at": created_at.isoformat()},
                            },
                        }
                    )
            except Exception:
                continue

        if actions:
            bulk(es, actions, chunk_size=100, request_timeout=60)

        es.index(
            index=DOCS_INDEX,
            id=doc_id,
            body={
                "doc_id": doc_id,
                "sub": sub,
                "title": title,
                "author": author,
                "filename": filename,
                "download_link": f"/download/{doc_id}",
                "num_pages": len(pdf.pages),
                "metadata": {"created_at": created_at.isoformat()},
            },
        )


@router.post("/upload/")
async def upload_pdf(
    sub: str = Form(...),
    title: str = Form(...),
    author: str = Form(...),
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    doc_id = str(uuid4())
    created_at = datetime.now(timezone.utc)

    try:
        async with fs.open_upload_stream_with_id(
            doc_id,
            file.filename,
            chunk_size_bytes=4,
            metadata={"contentType": "application/pdf"},
        ) as grid_in: 
            while True: 
                try:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    await grid_in.write(chunk)
                except Exception as e:
                    await fs.delete(grid_in._id)
                    raise HTTPException(status_code=500, detail=f"Save error: {e}")
                finally:
                    await grid_in.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File creation error: {e}")

    try:
         await index_pdf_pages(doc_id, file.filename, sub, title, author, created_at)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Indexing error: {e}")

    return {
        "doc_id": doc_id,
        "message": "File uploaded and indexed.",
        "download_link": f"/download/{doc_id}",
    }


@router.get("/documents/")
def get_all_documents():
    try:
        res = es.search(index=DOCS_INDEX, query={"match_all": {}}, size=1000)
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        if (
            hasattr(e, "info")
            and isinstance(e.info, dict)
            and e.info.get("error", {}).get("type") == "index_not_found_exception"
        ):
            return []
        raise HTTPException(status_code=500, detail="Error retrieving documents")


@router.get("/documents/{doc_id}/pages")
def get_pages_by_doc_id(doc_id: str):
    try:
        res = es.search(
            index=PAGES_INDEX,
            body={"query": {"match": {"doc_id": doc_id}}, "size": 1000},
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving pages: {e}")


@router.get("/documents/sub/{sub}")
def get_docs_by_sub(sub: str):
    try:
        res = es.search(
            index=DOCS_INDEX, body={"query": {"match": {"sub": sub}}, "size": 1000}
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving documents: {e}")


@router.get("/search")
async def search_text(q: str = Query(..., description="Text to search")):

    page_query = {"match": {"text": {"query": q, "fuzziness": "AUTO"}}}

    highlight = {"fields": {"text": {}, "title": {}}}
    _source_pages = ["doc_id", "page", "text", "metadata"]

    page_res = es.search(
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
            {"page": doc["page"], "highlight": highlights, "text": doc["text"]}
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

        doc_res = es.search(
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


@router.delete("/documents/{doc_id}")
async def delete_document(doc_id: str):
    try:
        es.delete_by_query(
            index=PAGES_INDEX,
            body={"query": {"match": {"doc_id": doc_id}}},
            refresh=True,
            wait_for_completion=True,
        )
        try:
            res = es.get(index=DOCS_INDEX, id=doc_id)
            doc_data = res["_source"]
        except Exception:
            raise HTTPException(status_code=404, detail="Document not found")

        es.delete(index=DOCS_INDEX, id=doc_id, refresh=True)
        try:
            file = await fs.open_download_stream(doc_id)
        except NoFile:
            raise HTTPException(status_code=404, detail="File not found")

        await fs.delete(file._id)
        return {"result": "deleted", "document": doc_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete error: {e}")


@router.get("/download/{doc_id}")
async def download_document(doc_id: str, download: bool = True):
    try:
        file = await fs.open_download_stream(doc_id)
        content = await file.read()
    except NoFile:
        raise HTTPException(status_code=404, detail="File not found")

    disposition = "attachment" if download else "inline"
    headers = {"Content-Disposition": f'{disposition}; filename="{file.filename}"'}
    return StreamingResponse(
        io.BytesIO(content), media_type="application/pdf", headers=headers
    )


@router.get("/preview/{file_id}")
async def preview_pdf(doc_id: str):
    try:
        grid_out = await fs.open_download_stream(doc_id)
        content = await grid_out.read()  # ✅ Serve await per leggere i bytes!
    except NoFile:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing file: {e}")

    try:
        images = convert_from_bytes(content, first_page=1, last_page=1)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PDF conversion error: {e}")

    if not images:
        raise HTTPException(status_code=500, detail="Unable to create preview")

    img_bytes = io.BytesIO()
    images[0].save(img_bytes, format="PNG")
    img_bytes.seek(0)

    return StreamingResponse(img_bytes, media_type="image/png")
