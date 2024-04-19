-- drop trigger
DROP TRIGGER IF EXISTS  ls_events_after_insert_trigger ON "ls_events";
DROP FUNCTION IF EXISTS ls_events_after_insert_trigger;
-- last_version
ALTER TABLE ls_pools DROP COLUMN last_event;
ALTER TABLE ls_pools ADD COLUMN last_version bigint DEFAULT 0 NOT NULL;


