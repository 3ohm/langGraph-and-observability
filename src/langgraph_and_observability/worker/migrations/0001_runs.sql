-- The runs table: one row per agent run (spec 7.3, 8). From intake on, Postgres owns the
-- run, not the queue. run_id is a uuid7 and also the run's LangGraph thread_id.
-- A pod that runs a run holds its lease: lease_owner is the pod's name, and the lease
-- ends at lease_expires_at. attempt counts the failed attempts; error is the last one.
-- checkpoints_pruned_at is set when the pruner has deleted the run's checkpoints.
CREATE TABLE runs (
    run_id uuid PRIMARY KEY,
    agent text NOT NULL,
    dedup_key text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CONSTRAINT runs_status_check
        CHECK (status IN ('pending', 'running', 'success', 'failed')),
    input jsonb NOT NULL,
    attempt integer NOT NULL DEFAULT 0,
    lease_owner text,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    ended_at timestamptz,
    error jsonb,
    checkpoints_pruned_at timestamptz
);

-- Intake inserts with ON CONFLICT (dedup_key) DO NOTHING, so a message is one run.
CREATE UNIQUE INDEX runs_dedup_key_idx ON runs (dedup_key);
-- The periodic check looks for pending runs and running runs with an expired lease.
CREATE INDEX runs_status_lease_expires_at_idx ON runs (status, lease_expires_at);
