from __future__ import annotations

from pydantic import BaseModel, Field


class EmbeddingRequest(BaseModel):
    input: str | list[str]
    model: str


class EmbeddingItem(BaseModel):
    object: str = "embedding"
    index: int
    embedding: list[float]


class Usage(BaseModel):
    prompt_tokens: int = 0
    total_tokens: int = 0


class EmbeddingResponse(BaseModel):
    object: str = "list"
    model: str
    data: list[EmbeddingItem]
    usage: Usage = Field(default_factory=Usage)
