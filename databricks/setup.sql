-- Run once in the Databricks SQL editor (Free Edition works).
-- If your workspace does not allow CREATE CATALOG, replace `gridpulse` with `workspace`
-- here, in databricks.yml (var.catalog) and in the GRIDPULSE_VOLUME secret.

CREATE CATALOG IF NOT EXISTS gridpulse
  COMMENT 'GridPulse: European electricity market lakehouse';

CREATE SCHEMA IF NOT EXISTS gridpulse.raw    COMMENT 'Landing zone: untouched API responses';
CREATE SCHEMA IF NOT EXISTS gridpulse.bronze COMMENT 'Raw envelopes as Delta tables, append-only';
CREATE SCHEMA IF NOT EXISTS gridpulse.silver COMMENT 'Typed, deduplicated, one row per series and timestamp';
CREATE SCHEMA IF NOT EXISTS gridpulse.gold   COMMENT 'Business-ready tables the agent is allowed to query';

CREATE VOLUME IF NOT EXISTS gridpulse.raw.landing
  COMMENT 'JSON envelopes uploaded by the GitHub Actions collector';
