-- The Incident Triage results (spec 7.3, 9.8, 9.9): one row per run, written by the
-- publish step with ON CONFLICT (run_id) DO NOTHING. run_id is the run's uuid7. It is
-- not a foreign key to the worker's runs table: the worker is removable (spec 13.9).
-- result is the whole validated TriageResult; the other columns repeat some of its
-- fields for queries. category is the analysis's category, or 'unknown' when the run
-- has no analysis (a refusal).
CREATE TABLE triage_results (
    run_id uuid PRIMARY KEY,
    dedup_key text NOT NULL,
    target jsonb NOT NULL,
    category text NOT NULL,
    result jsonb NOT NULL,
    model_id text,
    usage jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Retention deletes the rows created before a cutoff (spec 7.4).
CREATE INDEX triage_results_created_at_idx ON triage_results (created_at);
-- The results of one dedup key: the alerts of one workload in one window (spec 9.11).
CREATE INDEX triage_results_dedup_key_idx ON triage_results (dedup_key);
