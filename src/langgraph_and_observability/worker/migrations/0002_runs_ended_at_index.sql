-- The retention pruner (spec 7.4) reads finished runs by ended_at, oldest first, and
-- deletes them by the same condition. This partial index holds finished runs only, so
-- those reads never scan the pending and running ones.
CREATE INDEX runs_finished_ended_at_idx ON runs (ended_at, run_id)
    WHERE status IN ('success', 'failed');
