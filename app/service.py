# Service layer
from app.models import DocumentCreate, DocumentRead


class DocumentService:
    #
    # TODO:
    #       - spostare la logica di business da `routes.py` a questo servizio
    #       - importare tramite dependency injection il servizio nelle route
    #       - logica di modifica del documento
    #
    #

    async def get_documents():
        pass

    async def get_documents_by_sub(sub: str):
        pass

    async def get_documents_by_id(doc_id: str):
        pass

    async def get_documents_pages_by_id(doc_id: str):
        pass

    async def delete_document_by_id(doc_id: str):
        pass

    def create_document(document: DocumentCreate):
        pass
