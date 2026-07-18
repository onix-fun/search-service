from __future__ import annotations

from typing import Protocol

from .model import EmbeddingItem, EmbeddingRequest, EmbeddingResponse


class Encoder(Protocol):
    def encode(self, values: list[str]) -> list[list[float]]: ...


class EmbeddingService:
    def __init__(self, model: str, max_batch_size: int, encoder: Encoder) -> None:
        self._model = model
        self._max_batch_size = max_batch_size
        self._encoder = encoder

    def embed(self, request: EmbeddingRequest) -> EmbeddingResponse:
        if request.model != self._model:
            raise ValueError(f"model {request.model} is not loaded")
        values = [request.input] if isinstance(request.input, str) else request.input
        if (
            not values
            or len(values) > self._max_batch_size
            or any(not value.strip() for value in values)
        ):
            raise ValueError(f"input must contain 1 to {self._max_batch_size} non-empty strings")
        vectors = self._encoder.encode(values)
        return EmbeddingResponse(
            model=self._model,
            data=[
                EmbeddingItem(index=index, embedding=vector) for index, vector in enumerate(vectors)
            ],
        )
