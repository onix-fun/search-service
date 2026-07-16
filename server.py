import os
from functools import lru_cache
from typing import Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-small")
app = FastAPI(title="Onix local embeddings", version="1.0")

class EmbeddingRequest(BaseModel):
    input: Union[str, list[str]]
    model: str = MODEL

@lru_cache(maxsize=1)
def model() -> SentenceTransformer:
    return SentenceTransformer(MODEL, device="cpu")

@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL}

@app.post("/v1/embeddings")
def embeddings(request: EmbeddingRequest):
    if request.model != MODEL:
        raise HTTPException(400, f"model {request.model} is not loaded")
    values = [request.input] if isinstance(request.input, str) else request.input
    if not values or len(values) > 64:
        raise HTTPException(400, "input must contain 1 to 64 strings")
    vectors = model().encode(values, normalize_embeddings=True, batch_size=min(32, len(values)))
    return {"object": "list", "model": MODEL, "data": [
        {"object": "embedding", "index": index, "embedding": vector.tolist()}
        for index, vector in enumerate(vectors)
    ], "usage": {"prompt_tokens": 0, "total_tokens": 0}}
