from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    model: str
    device: str
    max_batch_size: int

    @classmethod
    def from_env(cls) -> Settings:
        model = os.getenv("SEARCH_INFERENCE_MODEL", "intfloat/multilingual-e5-small").strip()
        device = os.getenv("SEARCH_INFERENCE_DEVICE", "cpu").strip()
        max_batch_size = int(os.getenv("SEARCH_INFERENCE_MAX_BATCH_SIZE", "64"))
        if not model or not device or max_batch_size <= 0:
            raise ValueError(
                "SEARCH_INFERENCE model, device and positive max batch size are required"
            )
        return cls(model=model, device=device, max_batch_size=max_batch_size)
