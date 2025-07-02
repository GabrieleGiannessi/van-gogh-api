from collections import defaultdict
import io
from uuid import uuid4
from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Query, UploadFile
from elasticsearch import Elasticsearch
from fastapi.responses import StreamingResponse
from pdf2image import convert_from_bytes
from pymongo import MongoClient
import gridfs
import pdfplumber
from datetime import datetime, timezone

es = Elasticsearch("http://localhost:9200")
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["pdf_db"]
fs = gridfs.GridFS(db)

router = APIRouter()


def process_and_index_pdf(doc_id, filename, created_at):
    try:
        grid_out = fs.get(doc_id)
    except Exception as e:
        return

    with pdfplumber.open(grid_out) as pdf:
        for i, page in enumerate(pdf.pages):
            try:
                text = page.extract_text() or ""
                text = text.strip()
                if text:
                    page_id = f"{doc_id}_page_{i+1}"
                    es.index(
                        index="pages",
                        id=page_id,
                        body={
                            "doc_id": doc_id,
                            "page": i + 1,
                            "text": text,
                            "metadata": {"created_at": created_at.isoformat()},
                        },
                    )
            except Exception as e:
                return HTTPException(
                    status_code=500,
                    detail={"detail": f"Error processing page number {i}"},
                )

    es.index(
        index="docs",
        id=doc_id,
        body={
            "doc_id": doc_id,
            "title": filename,
            "download_link": f"/download/{doc_id}",
            "num_pages": len(pdf.pages),
            "metadata": {"created_at": created_at.isoformat()},
        },
    )


@router.post("/upload/")
async def upload_pdf(
    file: UploadFile = File(...), background_tasks: BackgroundTasks = None
):
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Il file deve essere un PDF.")

    doc_id = str(uuid4())
    created_at = datetime.now(timezone.utc)

    grid_in = fs.new_file(_id=doc_id, filename=file.filename)
    try:
        while chunk := await file.read(1024 * 1024):
            grid_in.write(chunk)
    finally:
        grid_in.close()

    if background_tasks:
        background_tasks.add_task(
            process_and_index_pdf, doc_id, file.filename, created_at
        )

    return {
        "doc_id": doc_id,
        "message": "File caricato correttamente. Indicizzazione in corso.",
    }


@router.get("/documents/")
def get_all_documents():
    try:
        res = es.search(index="docs", query={"match_all": {}}, size=1000)
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        if (
            hasattr(e, "info")
            and isinstance(e.info, dict)
            and e.info.get("error", {}).get("type") == "index_not_found_exception"
        ):
            return []
        raise HTTPException(status_code=500, detail="Errore nel recupero dei documenti")


@router.get("/documents/{doc_id}")
def get_pages_by_doc_id(doc_id: str):
    try:
        res = es.search(
            index="pages",
            body={
                "query": {
    "match": {
        "doc_id": doc_id
    }
},
                "size": 1000
            }
        )
        return [hit["_source"] for hit in res["hits"]["hits"]]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail={"detail": f"Errore recupero pagine: {e}"}
        )


@router.get("/search")
async def search_text(q: str = Query(..., description="Testo da cercare")):
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

    res = es.search(index="pages", body=es_query, size=100)

    grouped = defaultdict(
        lambda: {
            "doc_id": None,
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
        docs_res = es.mget(index="docs", body={"ids": list(doc_ids)})
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
        res = es.get(index="docs", id=doc_id)
        deleted_doc = res["_source"] | {"id": doc_id}
    except Exception:
        raise HTTPException(status_code=404, detail="Documento non trovato")

    es.delete(index="docs", id=doc_id)
    file = fs.find_one({"_id": doc_id})
    if file:
        fs.delete(file._id)

    return {"result": "deleted", "document": deleted_doc}


@router.get("/download/{file_id}")
def get_pdf(file_id: str, download: bool = True):
    file = fs.find_one({"_id": file_id})
    if not file:
        raise HTTPException(status_code=404, detail="File non trovato")

    headers = {}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="{file.filename}"'
    else:
        headers["Content-Disposition"] = f'inline; filename="{file.filename}"'

    return StreamingResponse(
        io.BytesIO(file.read()), media_type="application/pdf", headers=headers
    )


@router.get("/preview/{file_id}")
def preview_pdf(file_id: str):
    file = fs.find_one({"_id": file_id})
    if not file:
        raise HTTPException(status_code=404, detail="File non trovato")

    images = convert_from_bytes(file.read(), first_page=1, last_page=1)
    if not images:
        raise HTTPException(status_code=500, detail="Impossibile creare anteprima")

    # Salva in memoria come PNG
    img_bytes = io.BytesIO()
    images[0].save(img_bytes, format="PNG")
    img_bytes.seek(0)

    return StreamingResponse(img_bytes, media_type="image/png")
