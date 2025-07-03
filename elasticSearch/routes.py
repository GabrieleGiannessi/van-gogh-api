from contextlib import asynccontextmanager
import io
import logging
from uuid import uuid4
from datetime import datetime, timezone
from collections import defaultdict

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pymongo import MongoClient
import gridfs
import pdfplumber
from pdf2image import convert_from_bytes
from elasticSearch.models import Settings

logger = logging.getLogger(__name__)

settings = Settings()

def get_es_client():
    return Elasticsearch(settings.es_host)

def get_mongo_client():
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    client.server_info()
    return client

try:
    es = get_es_client()
    mongo_client = get_mongo_client()
    db = mongo_client["pdf_db"]
    fs = gridfs.GridFS(db)
except Exception as e:
    logger.error(f"Database connection error: {e}")
    raise

DOCS_INDEX = "docs"
PAGES_INDEX = "pages"


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
    init_indices()
    yield  
    

router = APIRouter(lifespan=lifespan)


def index_pdf_pages(doc_id, filename, sub, title, author, created_at):
    try:
        grid_out = fs.get(doc_id)
    except Exception:
        return

    with pdfplumber.open(grid_out) as pdf:
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
    background_tasks: BackgroundTasks,
    sub: str,
    title: str,
    author: str,
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    doc_id = str(uuid4())
    created_at = datetime.now(timezone.utc)

    try:
        grid_in = fs.new_file(_id=doc_id, filename=file.filename)
        try:
            while chunk := await file.read(1024 * 1024):
                grid_in.write(chunk)
        except Exception as e:
            grid_in.close()
            fs.delete(grid_in._id)
            raise HTTPException(status_code=500, detail=f"Save error: {e}")
        finally:
            grid_in.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File creation error: {e}")

    background_tasks.add_task(
        index_pdf_pages, doc_id, file.filename, sub, title, author, created_at
    )

    return {"doc_id": doc_id, "message": "File uploaded. Indexing in progress."}


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
    es_query = {
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["text", "metadata.title"],
                "fuzziness": "AUTO",
            }
        },
        "highlight": {"fields": {"text": {}, "metadata.title": {}}},
        "_source": ["doc_id", "page", "text", "metadata"],
    }

    res = es.search(index=PAGES_INDEX, body=es_query, size=100)

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
        }
    )

    doc_ids = set()
    for hit in res["hits"]["hits"]:
        doc = hit["_source"]
        highlights = hit.get("highlight", {})
        doc_id = doc["doc_id"]

        grouped[doc_id]["doc_id"] = doc_id
        grouped[doc_id]["matching_pages"].append(
            {"page": doc["page"], "highlight": highlights, "text": doc["text"]}
        )
        doc_ids.add(doc_id)

    if doc_ids:
        docs_res = es.mget(index=DOCS_INDEX, body={"ids": list(doc_ids)})
        for doc_hit in docs_res["docs"]:
            if doc_hit.get("found"):
                source = doc_hit["_source"]
                doc_id = source["doc_id"]
                if doc_id in grouped:
                    grouped[doc_id]["title"] = source.get("title")
                    grouped[doc_id]["download_link"] = source.get("download_link")
                    grouped[doc_id]["metadata"] = source.get("metadata", {})

    return list(grouped.values())


@router.delete("/documents/{doc_id}")
def delete_document(doc_id: str):
    try:
        es.delete_by_query(
            index=PAGES_INDEX, body={"query": {"match": {"doc_id": doc_id}}}
        )
        try:
            res = es.get(index=DOCS_INDEX, id=doc_id)
            doc_data = res["_source"]
        except Exception:
            raise HTTPException(status_code=404, detail="Document not found")
        es.delete(index=DOCS_INDEX, id=doc_id)
        file = fs.find_one({"_id": doc_id})
        if file:
            fs.delete(file._id)
        return {"result": "deleted", "document": doc_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete error: {e}")


@router.get("/download/{doc_id}")
def download_document(doc_id: str, download: bool = True):
    file = fs.find_one({"_id": doc_id})
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    disposition = "attachment" if download else "inline"
    headers = {"Content-Disposition": f'{disposition}; filename="{file.filename}"'}
    return StreamingResponse(
        io.BytesIO(file.read()), media_type="application/pdf", headers=headers
    )


@router.get("/preview/{file_id}")
def preview_pdf(file_id: str):
    file = fs.find_one({"_id": file_id})
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    images = convert_from_bytes(file.read(), first_page=1, last_page=1)
    if not images:
        raise HTTPException(status_code=500, detail="Unable to create preview")
    img_bytes = io.BytesIO()
    images[0].save(img_bytes, format="PNG")
    img_bytes.seek(0)
    return StreamingResponse(img_bytes, media_type="image/png")
