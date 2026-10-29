"""Context callers get the same validation guarantees as HTTP callers."""

import asyncio
from pathlib import Path

import pytest

from api import storage, submissions
from api.context import Context
from api.validation import Workflow
from shared.database import Database
from shared.errors import DomainError
from tests.cpu_helpers import prepare_paused
from tests.settings import Settings

pytestmark = pytest.mark.component


def graph(dependencies: list[list[str]]) -> dict:
    return {
        "name": "direct context caller",
        "dag": {
            "nodes": [
                {"id": f"n{i}", "handler": "input", "dependencies": parents}
                for i, parents in enumerate(dependencies)
            ]
        },
    }


@pytest.mark.parametrize(
    ("dependencies", "code"),
    [([["n1"], ["n0"]], "GRAPH_CYCLE"), ([["missing"]], "MISSING_DEPENDENCY")],
)
@pytest.mark.parametrize("as_model", [False, True])
async def test_engine_rejects_invalid_graph_before_persistence(
    db: Database, settings: Settings, dependencies: list[list[str]], code: str, as_model: bool
) -> None:
    payload = graph(dependencies)
    workflow = Workflow.model_validate(payload) if as_model else payload
    with pytest.raises(DomainError) as failure:
        await submissions.submit(Context(db, settings), workflow, "invalid-definition")
    assert failure.value.code == code
    async with db.tx() as connection:
        row = await (await connection.execute("SELECT count(*) AS n FROM executions")).fetchone()
        assert row is not None and row["n"] == 0


@pytest.mark.parametrize("operation", ["submit", "trigger"])
async def test_engine_validation_keeps_event_loop_responsive(
    db: Database,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    tmp_path: Path,
) -> None:
    context = Context(db, settings)
    identifier = (await submissions.submit(context, graph([[]]), "initial-definition")).execution_id
    entered, release = tmp_path / "entered", tmp_path / "release"
    original = context.validation.run

    async def slow_validation(function, *args):
        return await original(prepare_paused, str(entered), str(release), function, *args)

    monkeypatch.setattr(context.validation, "run", slow_validation)
    pending = asyncio.create_task(
        submissions.submit(context, graph([[]]), "next-definition")
        if operation == "submit"
        else submissions.trigger(context, identifier, {})
    )
    try:
        async with asyncio.timeout(5):
            while not entered.exists():
                await asyncio.sleep(0.005)
        status = await asyncio.wait_for(storage.status(context, identifier), 1)
        assert status["state"] == "PENDING"
    finally:
        release.touch()
        await pending
