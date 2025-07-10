from contextlib import asynccontextmanager
import io
import logging
from collections import defaultdict

from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
    Query,
)

from fastapi.responses import JSONResponse, StreamingResponse
from gridfs import NoFile
from pdf2image import convert_from_bytes
from app.dependency import (
    elasticSearch_dependency,
    fs_dependency,
    document_service_dependency,
)
from app.models import DocumentCreate
from app.exceptions import DocumentCreationError, DocumentIndexingError, StreamError
logger = logging.getLogger(__name__)

fs = None
documents_db = None
es = None

DOCS_INDEX = "docs"
PAGES_INDEX = "pages"

router = APIRouter()


@router.post("/upload/")
async def upload_pdf(
    fs: fs_dependency,
    es: elasticSearch_dependency,
    service: document_service_dependency,
    doc: DocumentCreate,
):
    if not doc.file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    try:
        doc = await service.create_document(fs, es, doc)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing document: {di.message} ")
    except DocumentCreationError as c: 
        raise HTTPException(status_code=500, detail=f"Error creating document in db: {c.message} ")
    except StreamError as s: 
        raise HTTPException(status_code=500, detail=f"Error streaming data: {s.message} ")
    except Exception as e: 
         raise HTTPException(status_code=500, detail=f"Error streaming data: {e} ")

    return JSONResponse(status_code=201, content=doc)


@router.get("/documents/")
async def get_all_documents(
    service: document_service_dependency, es: elasticSearch_dependency
):
    try:
        docs = await service.get_documents(es)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing docs: {di}")        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving docs: {e}")

    return JSONResponse(status_code=200, content=docs)


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
