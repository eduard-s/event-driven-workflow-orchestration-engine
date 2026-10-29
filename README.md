# Event-driven workflow engine

The engine runs JSON workflows with parallel branches and concurrent executions.
External-service and LLM handlers return mock results without external HTTP requests. The API has no authentication, as specified in the brief.

## Setup

Install Docker with Compose. Run commands from this directory.
For local settings, copy [.env.example](.env.example) to `.env` before startup.
The curl walkthrough below also uses Python 3 to capture the execution ID and format JSON.

Start the API, orchestrator, workers, PostgreSQL, and Redis with one Docker command:

```sh
WF_API_PORT=28400 WF_POSTGRES_PORT=25442 WF_REDIS_PORT=26489 \
docker compose -p workflow-ready -f infra/docker-compose.yml \
  up --build --detach --wait
```

This starts a separate `workflow-ready` Compose project with the API at `http://localhost:28400`, PostgreSQL on port `25442`, and Redis on port `26489`.
These ports avoid clashes with another checkout using the default ports (`18000`, `15432`, and `16379`).
[db/init.sql](db/init.sql) initializes empty database volumes.
Add `--scale worker=2 --scale orchestrator=2` for more replicas.

Stop the stack and keep saved data:

```sh
docker compose -p workflow-ready -f infra/docker-compose.yml down
```

The `--volumes` option deletes saved workflows.

## Architecture overview

| Application | Command | Responsibility |
|---|---|---|
| [api/](api/) | `python -m api` | Accept definitions and triggers. Return status and results. |
| [orchestrator/](orchestrator/) | `python -m orchestrator` | Dispatch tasks. Record worker reports, update workflow state, and manage retries. |
| [worker/](worker/) | `python -m worker` | Claim tasks in Redis, run handlers, and report starts and results through Redis. |

```mermaid
flowchart LR
    Client[Client]
    subgraph Apps[Separate application containers]
        API[API]
        Orchestrator["Orchestrator replicas<br/>Relay, readiness, recovery"]
        Workers["Stateless worker replicas<br/>Redis claims and concurrent handlers"]
    end
    PG[("PostgreSQL<br/>Definitions, state, tries, outbox")]
    Redis[("Redis<br/>Streams, claims, temporary results")]

    Client -->|Submit, trigger, read| API
    API -->|Receipt after commit| Client
    API -->|Store and read| PG
    PG -->|Outbox records| Orchestrator
    Orchestrator -->|Task JSON and workflow events| Redis
    Redis -->|Workflow and worker events| Orchestrator
    Redis -->|Frozen node inputs and limits| Workers
    Workers -->|Claim try, report start and result| Redis
    Orchestrator -->|Record reports and decide state under row locks| PG
```

The applications run in separate containers with one shared runtime image. The test image adds development tools.
[shared/](shared/) contains common contracts and infrastructure.

PostgreSQL holds workflow state and the outbox. Only the API and orchestrator connect to it. Workers have no database connections or durable local state.
Redis Streams carries complete task JSON and worker events. Redis hashes hold claims and temporary results.
Redis claims use `(execution_id, node_id, try_number)`, so branches and workflows can run in parallel.

API receipts return after commit. Workflow execution continues in the background.
[DESIGN.md](DESIGN.md) explains readiness, fan-in, idempotency, recovery, and trade-offs.

## Trigger the test workflows

Submit the linear workflow (Scenario A: A -> B -> C) and capture its execution ID:

```sh
ID=$(curl -fsS http://localhost:28400/workflow \
  -H 'Content-Type: application/json' \
  --data-binary @examples/linear.json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution_id"])')

echo "$ID"
```

Start the workflow with input data:

```sh
curl -sS -X POST "http://localhost:28400/workflow/trigger/$ID" \
  -H 'Content-Type: application/json' \
  --data '{"input":{"id":7}}'
```

Check its status:

```sh
curl -sS "http://localhost:28400/workflows/$ID" | python3 -m json.tool
```

Get the results:

```sh
curl -sS "http://localhost:28400/workflows/$ID/results" | python3 -m json.tool
```

Submission returns `201`. Trigger acceptance returns `202`. Results return `200` for `COMPLETED` and `409` for other existing execution states.
Repeat the GET requests until the state becomes `COMPLETED`, `FAILED`, or `EXPIRED`.
[linear.json](examples/linear.json) passes data through an LLM mock. Its completed result includes:

```json
{
  "outputs": {
    "c": "mock: ID 7"
  }
}
```

For Scenario B, submit the supplied parallel workflow instead:

```sh
ID=$(curl -fsS http://localhost:28400/workflow \
  -H 'Content-Type: application/json' \
  --data-binary @examples/parallel.json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["execution_id"])')

echo "$ID"
```

Repeat the trigger, status, and results commands above with this new ID.
[parallel.json](examples/parallel.json) uses the supplied payload. Its three external-service mocks each wait one second and overlap.
The parallel result contains all branches in `outputs.output`.
The sample PDF uses `/workflows`. This API uses `POST /workflow`, as specified in the brief.

`Idempotency-Key` is optional and accepts 1 to 128 characters. Without it, each accepted submission creates a new execution.
For submission retries, send the same key with `-H 'Idempotency-Key: example-1'`.
The key returns its original ID, even with different valid content. Retention deletes the key with its execution.

Each execution accepts one trigger. Repeated triggers return `409`. After a lost trigger response, read the status.
The trigger accepts `input` and `node_params` overrides.

## Tests

Run unit, component, and integration tests with isolated dependencies:

```sh
WF_POSTGRES_PORT=25432 WF_REDIS_PORT=26379 \
  docker compose --project-name workflow-tests -f infra/docker-compose.yml \
  --profile test run --build --rm test
```

| Suite | Coverage |
|---|---|
| `tests/unit` | Graphs, templates, limits, retries, handlers, and cancellation. |
| `tests/component` | API receipts, validation, quotas, audit records, and retention. |
| `tests/integration` | Parallel workflows, fan-in races, duplicate tasks, Redis claims, delayed results, and repair. |
| `tests/docker` | Process crashes, dependency outages, broker loss, and backup restoration. |
| `tests/performance` | Capacity, rate limits, memory growth, and replica loss. |

Run unit tests without PostgreSQL or Redis:

```sh
docker compose -f infra/docker-compose.yml run --build --rm --no-deps test pytest -q tests/unit
```

For crash and performance suites, use Python 3.14 and a separate Compose project:

```sh
python3.14 -m venv .venv
.venv/bin/pip install -r requirements-dev.lock
export COMPOSE_PROJECT_NAME=workflow-tests WF_POSTGRES_PORT=25432 WF_REDIS_PORT=26379 WF_API_PORT=28000
export WF_TEST_BASE_URL=http://localhost:28000 WF_TEST_REDIS_URL=redis://localhost:26379/0
export WF_TEST_POSTGRES_DSN=postgresql://workflow:workflow@localhost:25432/workflow
docker compose -f infra/docker-compose.yml up --build --detach --wait --scale worker=2
WF_RUN_DOCKER_TESTS=1 .venv/bin/pytest -q tests/docker
# Reset service restart counts before memory measurements.
docker compose -f infra/docker-compose.yml up --force-recreate --detach --wait --scale worker=2 api orchestrator worker
WF_RUN_PERFORMANCE=1 .venv/bin/pytest -q -s tests/performance
```

Run the suites serially. Crash tests replace isolated test data.
After the suites finish, remove the isolated project:

```sh
docker compose --project-name workflow-tests -f infra/docker-compose.yml down --volumes
```

[CI](.github/workflows/ci.yml) runs static analysis and functional tests. Manual CI runs include crash recovery.
