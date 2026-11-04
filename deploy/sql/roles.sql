-- Database roles (spec §7.1): lps_migrator runs the migrations (DDL); lps_app, which the
-- worker uses, can only read and write rows.
--
-- Run it as a superuser, in each application database, before the first migrate:
--
--   psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 \
--     -v migrator_password="$LPS_MIGRATOR_PASSWORD" -v app_password="$LPS_APP_PASSWORD" \
--     -f deploy/sql/roles.sql
--
-- Then connect APP_MIGRATION_DATABASE_URL as lps_migrator and APP_DATABASE_URL as lps_app.
-- The script is safe to run again; each run also resets both passwords. Roles belong to
-- the whole server, the grants to the current database. lps_app gets row access to the
-- tables that exist now, and to every table that lps_migrator creates later.

-- psql does not fill in variables inside $$ ... $$, so the passwords are set below.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lps_migrator') THEN
        CREATE ROLE lps_migrator;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lps_app') THEN
        CREATE ROLE lps_app;
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO lps_migrator, lps_app', current_database());
END
$$;

ALTER ROLE lps_migrator WITH LOGIN PASSWORD :'migrator_password';
ALTER ROLE lps_app WITH LOGIN PASSWORD :'app_password';

-- Postgres 15 and later already deny CREATE in public to PUBLIC; a database upgraded
-- from an older version may still allow it.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO lps_migrator;
GRANT USAGE ON SCHEMA public TO lps_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO lps_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO lps_app;
ALTER DEFAULT PRIVILEGES FOR ROLE lps_migrator IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lps_app;
ALTER DEFAULT PRIVILEGES FOR ROLE lps_migrator IN SCHEMA public
    GRANT USAGE ON SEQUENCES TO lps_app;
