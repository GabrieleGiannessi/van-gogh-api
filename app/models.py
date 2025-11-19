from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()
class Metadata (BaseModel):
    created_at: Optional[str] = None
    
class DocumentCreateMetadata(BaseModel):
    sub: str 
    title: str 
    author: str 
    filename: str
    
class PartialDocument(BaseModel): 
    title: Optional[str] = None
    author: Optional[str] = None
    
class DocumentRead(BaseModel):
    doc_id: str 
    sub: str 
    title: str 
    author: str 
    filename: str
    num_pages: Optional[int]  
    metadata: Metadata
    
class DocumentPage (BaseModel): 
    doc_id: str
    page: int
    text: str
    metadata: Metadata
    highlight: Optional[dict[str, list[str]]] = None
    
class IndexedDocument (BaseModel): 
    doc_id: str 
    path: str
    sub: str 
    title: str 
    author: str 
    filename: str
    created_at: datetime
    
class DocumentSearchResult(BaseModel):
    doc_id: str
    sub: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    filename: Optional[str] = None
    download_link: Optional[str] = None
    metadata: Optional[Metadata] = None
    title_match: bool = False
    matching_pages: list[DocumentPage] = []
    
class Settings(BaseSettings):
    minio_client: str = os.environ["MINIO_CLIENT_URL"]
    es_host: str = os.environ["ES_URL"]
    es_password: str = os.environ["ELASTIC_PASSWORD"]
    es_user: str = os.environ.get("ES_USER", "elastic")
