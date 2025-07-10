from contextlib import asynccontextmanager
import io
import logging
from collections import defaultdict

from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
)

from fastapi.responses import JSONResponse, StreamingResponse
from gridfs import NoFile
from pdf2image import convert_from_bytes
from app.dependency import (
    elasticSearch_dependency,
    fs_dependency,
    document_service_dependency,
)
from app.models import (
    DocumentCreateMetadata,
    DocumentPage,
    DocumentRead,
    DocumentSearchResult,
)
from app.exceptions import (
    DocumentCreationError,
    DocumentDeleteError,
    DocumentIndexingError,
    DocumentNotFound,
    PreviewException,
    StreamError,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload/")
async def upload_pdf(
    fs: fs_dependency,
    es: elasticSearch_dependency,
    service: document_service_dependency,
    sub: str = Form(...),
    title: str = Form(...),
    author: str = Form(...),
    file: UploadFile = File(...),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    doc = DocumentCreateMetadata(
        sub=sub, title=title, author=author, filename=file.filename
    )

    try:
        res = await service.create_document(fs, es, doc, file)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing document: {di} ")
    except DocumentCreationError as c:
        raise HTTPException(
            status_code=500, detail=f"Error creating document in db: {c} "
        )
    except StreamError as s:
        raise HTTPException(status_code=500, detail=f"Error streaming data: {s} ")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error streaming data: {e} ")

    return JSONResponse(status_code=201, content=res)


@router.get("/documents/", response_model=list[DocumentRead])
async def get_all_documents(
    service: document_service_dependency, es: elasticSearch_dependency
):
    try:
        return await service.get_documents(es)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing docs: {di}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving docs: {e}")


@router.get("/documents/{doc_id}/pages", response_model=list[DocumentPage])
async def get_pages_by_doc_id(
    service: document_service_dependency, es: elasticSearch_dependency, doc_id: str
):
    try:
        return await service.get_documents_pages_by_id(es, doc_id=doc_id)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing pages: {di}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving pages: {e}")


@router.get("/documents/sub/{sub}", response_model=list[DocumentRead])
async def get_docs_by_sub(
    service: document_service_dependency, es: elasticSearch_dependency, sub: str
):
    try:
        return await service.get_documents_by_sub(es, sub=sub)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing doc: {di}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving doc: {e}")


@router.get("/search", response_model=list[DocumentSearchResult])
async def search_text(
    service: document_service_dependency,
    es: elasticSearch_dependency,
    q: str = Query(..., description="Text to search"),
):
    return await service.query_documents(es, q)


@router.delete("/documents/{doc_id}", response_model=DocumentRead)
async def delete_document(
    fs: fs_dependency,
    es: elasticSearch_dependency,
    service: document_service_dependency,
    doc_id: str,
):
    try:
        return await service.delete_document_by_id(fs, es, doc_id)
    except DocumentDeleteError as de:
        raise HTTPException(status_code=500, detail=f"Error while removing doc: {de}")
    except DocumentIndexingError as di:
        raise HTTPException(
            status_code=500, detail=f"Error with index while deleting doc: {di}"
        )
    except StreamError as s:
        raise HTTPException(
            status_code=500, detail=f"Stream error while deleting doc: {s}"
        )


@router.get("/download/{doc_id}")
async def download_document(
    service: document_service_dependency,
    fs: fs_dependency,
    doc_id: str,
    download: bool = True,
):
    if download:
        return await service.get_download_document(fs, doc_id)

    raise HTTPException(status_code=400, detail="Download failed")


@router.get("/preview/{doc_id}")
async def preview_pdf(
    service: document_service_dependency,
    fs: fs_dependency,
    doc_id: str,
):
    try:
        return await service.get_preview_document(fs, doc_id)
    except DocumentNotFound: 
        raise HTTPException (status_code=404, detail="Document not found")
    except PreviewException: 
        raise HTTPException (status_code=404, detail="Error loading preview")
    except Exception as e: 
        raise HTTPException (status_code=500, detail=f"Server error: {e}")