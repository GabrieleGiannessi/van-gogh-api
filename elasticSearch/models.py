from pydantic import BaseModel
from pydantic_settings import BaseSettings

class Document(BaseModel):
    title: str
    payload: str

class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017"
    es_host: str = "http://localhost:9200"
