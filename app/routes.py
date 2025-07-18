import logging

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile, 
)

from fastapi.responses import JSONResponse

from app.auth.keycloak import require_admin
from app.dependency import (
    elasticSearch_dependency,
    document_service_dependency,
)
from app.models import (
    DocumentCreateMetadata,
    DocumentPage,
    DocumentRead,
    DocumentSearchResult,
    PartialDocument,
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


@router.get("/me/")
async def get_users_me(payload: dict = Depends(require_admin)):
    return {"user": payload}

@router.post("/upload/")
async def upload_pdf(
    service: document_service_dependency,
    sub: str = Form(...),
    title: str = Form(...),
    author: str = Form(...),
    file: UploadFile = File(...),
    token: dict = Depends(require_admin)
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF.")

    doc = DocumentCreateMetadata(
        sub=sub, title=title, author=author, filename=file.filename
    )

    try:
        res = await service.create_document(doc, file)
    except DocumentIndexingError as di:
        raise HTTPException(status_code=500, detail=f"Error indexing document: {di} ")
    except DocumentCreationError as c:
        raise HTTPException(
            status_code=500, detail=f"Document creation error: {c} "
        )
    except StreamError as s:
        raise HTTPException(status_code=500, detail=f"Streaming error: {s} ")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {e} ")

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


@router.get("/documents/{doc_id}", response_model=DocumentRead)
async def get_document_by_id(
    service: document_service_dependency, es: elasticSearch_dependency, doc_id: str
):
    try:
        return await service.get_document_by_id(es, doc_id)
    except DocumentNotFound:
        raise HTTPException(status_code=404, detail=f"Document not found")
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
    es: elasticSearch_dependency,
    service: document_service_dependency,
    doc_id: str,
    token_data: dict = Depends(require_admin)
):
    try:
        return await service.delete_document_by_id(es, doc_id)
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
    doc_id: str,
    download: bool = True,
):
    try:
        return await service.get_download_document(doc_id, download)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Download failed")


@router.get("/preview/{doc_id}")
async def preview_pdf(
    service: document_service_dependency,
    doc_id: str,
):
    try:
        return await service.get_preview_document(doc_id)
    except DocumentNotFound:
        raise HTTPException(status_code=404, detail="Document not found")
    except PreviewException:
        raise HTTPException(status_code=404, detail="Error loading preview")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {e}")


@router.patch("/documents/{doc_id}")
async def modify_document(
    service: document_service_dependency,
    es: elasticSearch_dependency,
    doc_id: str,
    metadata: PartialDocument,
):

    try:
        await service.update_document(es, doc_id, metadata)
    except DocumentIndexingError as di:
        raise HTTPException(
            status_code=500, detail=f"Error while retrieving doc: {di} "
        )
    except DocumentNotFound:
        raise HTTPException(status_code=404, detail="Not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {e} ")

    return Response(status_code=204)
