-- This file should undo anything in `up.sql`
DROP INDEX ls_events_sq_idx;
ALTER TABLE ls_events DROP COLUMN sq;
ALTER TABLE ls_pools RENAME COLUMN last_event TO last_tx_version;