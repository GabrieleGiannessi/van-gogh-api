from datetime import datetime
from typing import Optional
from fastapi import Form, UploadFile
from pydantic import BaseModel
from pydantic_settings import BaseSettings
class Metadata (BaseModel):
    created_at: str
    
class DocumentCreate(BaseModel):
    sub: str 
    title: str 
    author: str 
    file: UploadFile 
    
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
    
class IndexedDocument (BaseModel): 
    doc_id: str 
    sub: str 
    title: str 
    author: str 
    filename: str
    created_at: datetime
    
class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017"
    es_host: str = "http://localhost:9200"
