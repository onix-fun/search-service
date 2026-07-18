from onix_search_inference.application import EmbeddingService
from onix_search_inference.model import EmbeddingRequest


class FakeEncoder:
    def encode(self, values: list[str]) -> list[list[float]]:
        return [[float(len(value))] for value in values]


def test_embed_preserves_input_order() -> None:
    result = EmbeddingService("test", 4, FakeEncoder()).embed(
        EmbeddingRequest(model="test", input=["one", "three"])
    )
    assert [item.embedding for item in result.data] == [[3.0], [5.0]]


def test_embed_rejects_unknown_model() -> None:
    service = EmbeddingService("loaded", 4, FakeEncoder())
    try:
        service.embed(EmbeddingRequest(model="other", input="value"))
    except ValueError as error:
        assert "not loaded" in str(error)
    else:
        raise AssertionError("unknown model must be rejected")
