from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI, HTTPException

from .application import EmbeddingService
from .config import Settings
from .model import EmbeddingRequest, EmbeddingResponse
from .sentence_transformer import SentenceTransformerEncoder


@lru_cache(maxsize=1)
def service() -> EmbeddingService:
    settings = Settings.from_env()
    return EmbeddingService(
        model=settings.model,
        max_batch_size=settings.max_batch_size,
        encoder=SentenceTransformerEncoder(settings.model, settings.device),
    )


def create_app() -> FastAPI:
    app = FastAPI(title="Onix Search inference", version="1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "model": Settings.from_env().model}

    @app.post("/v1/embeddings", response_model=EmbeddingResponse)
    def embeddings(request: EmbeddingRequest) -> EmbeddingResponse:
        try:
            return service().embed(request)
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

    return app


app = create_app()
