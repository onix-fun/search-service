from __future__ import annotations

from typing import Any


class SentenceTransformerEncoder:
    def __init__(self, model: str, device: str) -> None:
        from sentence_transformers import SentenceTransformer

        self._model: Any = SentenceTransformer(model, device=device)

    def encode(self, values: list[str]) -> list[list[float]]:
        vectors = self._model.encode(
            values, normalize_embeddings=True, batch_size=min(32, len(values))
        )
        return [vector.tolist() for vector in vectors]
