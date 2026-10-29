\set ON_ERROR_STOP on

-- Docker creates the workflow database before this script starts.
\if :{?audit_db}
\else
  \getenv audit_db WF_AUDIT_DB
  \if :{?audit_db}
  \else
    \set audit_db workflow_audit
  \endif
\endif

-- Workflow state.
BEGIN;

CREATE TABLE executions (
 id uuid PRIMARY KEY, definition jsonb NOT NULL, policy jsonb NOT NULL,
 state text NOT NULL CHECK(state IN ('PENDING','RUNNING','COMPLETED','FAILED','EXPIRED')),
 trigger jsonb, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 accepted_at timestamptz, deadline_at timestamptz, terminal_at timestamptz,
 maintenance_due timestamptz NOT NULL DEFAULT clock_timestamp(),
 has_failure boolean NOT NULL DEFAULT false, input_bytes bigint NOT NULL DEFAULT 0,
 output_bytes bigint NOT NULL DEFAULT 0
);
CREATE INDEX executions_due ON executions(maintenance_due);
CREATE INDEX executions_terminal ON executions(terminal_at) WHERE terminal_at IS NOT NULL;
CREATE TABLE nodes (
 execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
 node_id text NOT NULL, handler text NOT NULL, dependencies text[] NOT NULL,
 state text NOT NULL DEFAULT 'PENDING' CHECK(state IN ('PENDING','RUNNING','COMPLETED','RETRY_WAIT','FAILED','BLOCKED','EXPIRED')),
 reason text NOT NULL DEFAULT 'WAITING_DEPENDENCIES', try_number integer NOT NULL DEFAULT 0,
 starts integer NOT NULL DEFAULT 0, resolved_input jsonb, has_input boolean NOT NULL DEFAULT false,
 has_output boolean NOT NULL DEFAULT false, error jsonb,
 ready_at timestamptz NOT NULL DEFAULT clock_timestamp(), next_retry_at timestamptz,
 PRIMARY KEY(execution_id,node_id)
);
CREATE INDEX nodes_ready ON nodes(execution_id,state,ready_at,node_id);
CREATE TABLE edges (
 execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
 parent_id text NOT NULL, child_id text NOT NULL,
 PRIMARY KEY(execution_id,parent_id,child_id),
 CONSTRAINT edges_parent_fk FOREIGN KEY(execution_id,parent_id)
 REFERENCES nodes(execution_id,node_id) ON DELETE CASCADE,
 CONSTRAINT edges_child_fk FOREIGN KEY(execution_id,child_id)
 REFERENCES nodes(execution_id,node_id) ON DELETE CASCADE
);
CREATE INDEX edges_child ON edges(execution_id,child_id,parent_id);
CREATE TABLE tries (
 execution_id uuid NOT NULL, node_id text NOT NULL, try_number integer NOT NULL,
 event_id uuid NOT NULL UNIQUE, token uuid, started_at timestamptz, lease_until timestamptz,
 finished_at timestamptz, has_outcome boolean NOT NULL DEFAULT false,
 success boolean, value jsonb, error jsonb, runtime jsonb,
 PRIMARY KEY(execution_id,node_id,try_number),
 FOREIGN KEY(execution_id,node_id) REFERENCES nodes(execution_id,node_id) ON DELETE CASCADE
);
CREATE INDEX tries_lease ON tries(lease_until) WHERE token IS NOT NULL AND has_outcome=false;
CREATE TABLE outbox (
 event_id uuid PRIMARY KEY, execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
 kind text NOT NULL CHECK(kind IN ('start','task','completion')),
 node_id text NOT NULL DEFAULT '', try_number integer NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_id text,
 published_at timestamptz, checked_at timestamptz,
 lease_owner uuid, lease_until timestamptz, applied_at timestamptz, publications integer NOT NULL DEFAULT 0,
 UNIQUE(execution_id,kind,node_id,try_number)
);
CREATE INDEX outbox_publish ON outbox(created_at) WHERE applied_at IS NULL;
CREATE TABLE submission_keys (
 key text PRIMARY KEY,
 execution_id uuid NOT NULL REFERENCES executions(id) ON DELETE CASCADE
);
CREATE INDEX submission_keys_execution ON submission_keys(execution_id);

COMMIT;

-- Audit records use a separate database and the same setup entry point.
SELECT format('CREATE DATABASE %I', :'audit_db')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname=:'audit_db') \gexec
\connect :audit_db

BEGIN;
CREATE TABLE IF NOT EXISTS request_audit (
 request_id uuid PRIMARY KEY,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 method text NOT NULL, route text NOT NULL,
 status smallint NOT NULL CHECK(status BETWEEN 100 AND 599),
 outcome text NOT NULL CHECK(outcome IN ('rejected','duplicate')),
 reason text NOT NULL, execution_id uuid
);
CREATE INDEX IF NOT EXISTS request_audit_created ON request_audit(created_at);
COMMIT;
