-- The action ledger: one row per side effect that must happen at most once.
-- key is the idempotency key, args_hash a hash of the action's arguments, and result
-- what the action returned, so a repeated call gets the first result back.
CREATE TABLE action_ledger (
    key text PRIMARY KEY,
    action text NOT NULL,
    args_hash text NOT NULL,
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
