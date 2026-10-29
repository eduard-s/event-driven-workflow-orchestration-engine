import pytest

from api.validation import validate_workflow
from shared.errors import DomainError


def test_native_nul_cannot_bypass_json_storage_rules() -> None:
    with pytest.raises(DomainError) as caught:
        validate_workflow(
            {
                "name": "nul",
                "dag": {
                    "nodes": [{"id": "a", "handler": "output", "dependencies": [], "data": "\x00"}]
                },
            }
        )
    assert caught.value.status == 422
