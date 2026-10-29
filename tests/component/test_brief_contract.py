"""The supplied payload needs no extra fields or receipt headers."""

import pytest

from shared.database import Database
from tests.component.test_api import client_for
from tests.integration.test_engine import SUPPLIED_WORKFLOW
from tests.settings import Settings

pytestmark = pytest.mark.component


async def test_supplied_payload_commits_before_receipt_without_optional_headers(
    db: Database, settings: Settings
) -> None:
    """Accept the supplied graph before an orchestrator or worker starts.

    Args:
        db: Independent connection pool for committed-state assertions.
        settings: API settings for the isolated test database.

    Returns:
        None after receipt, stored definition, and background execution assertions.
    """
    async with client_for(settings) as (client, _):
        response = await client.post("/workflow", json=SUPPLIED_WORKFLOW)
        assert response.status_code == 201, response.text
        identifier = response.json()["execution_id"]

        # A separate connection can see the complete committed receipt.
        async with db.tx() as connection:
            stored = await (
                await connection.execute(
                    "SELECT definition,state,accepted_at FROM executions WHERE id=%s",
                    (identifier,),
                )
            ).fetchone()
            starts = await (
                await connection.execute(
                    "SELECT starts FROM nodes WHERE execution_id=%s", (identifier,)
                )
            ).fetchall()
        assert stored == {
            "definition": SUPPLIED_WORKFLOW,
            "state": "PENDING",
            "accepted_at": None,
        }
        assert len(starts) == 5 and all(node["starts"] == 0 for node in starts)
        assert (await client.get(f"/workflows/{identifier}/results")).status_code == 409

        triggered = await client.post(f"/workflow/trigger/{identifier}")
        assert triggered.status_code == 202, triggered.text
        status = await client.get(f"/workflows/{identifier}")
        assert status.status_code == 200
        assert status.json()["state"] == "RUNNING"
        assert all(node["state"] == "PENDING" for node in status.json()["nodes"])

        async with db.tx() as connection:
            start = await (
                await connection.execute(
                    "SELECT published_id,applied_at FROM outbox WHERE execution_id=%s AND kind='start'",
                    (identifier,),
                )
            ).fetchone()
        assert start == {"published_id": None, "applied_at": None}
