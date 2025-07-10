from fastapi import Form, UploadFile
from pydantic import BaseModel
from pydantic_settings import BaseSettings

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
    num_pages: int  
    
class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017"
    es_host: str = "http://localhost:9200"
