from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.lifespan import lifespan
from app.routes import router 
import logging

app = FastAPI(lifespan=lifespan)


app.include_router(router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
