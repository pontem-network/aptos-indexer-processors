-- Your SQL goes here
ALTER TABLE ls_events ADD sq bigserial NOT NULL;
CREATE INDEX ls_events_sq_idx ON ls_events (sq);
ALTER TABLE ls_pools RENAME COLUMN last_tx_version TO last_event;
