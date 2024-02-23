CREATE INDEX ls_events_version_idx ON ls_events ("version");
CREATE INDEX ls_events_pool_id_idx ON ls_events (pool_id);
CREATE INDEX ls_events_tx_hash_idx ON ls_events (tx_hash);
CREATE INDEX ls_events_sender_idx ON ls_events (sender);