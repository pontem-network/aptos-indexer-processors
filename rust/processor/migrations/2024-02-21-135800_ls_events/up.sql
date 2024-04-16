CREATE TYPE event_type AS ENUM (
    'pool_created_event',
	'liquidity_added_event',
    'liquidity_removed_event',
    'swap_event',
    'flashloan_event',
    'oracle_updated_event',
    'update_fee_event',
    'update_dao_fee_event',
    'coin_deposited_event',
);

CREATE TABLE public.ls_events (
	sq bigserial NOT NULL,
	id varchar NOT NULL,
	pool_id varchar(64) NOT NULL,
	"tp" event_type NOT NULL,
	"version" bigint NOT NULL,
	tx_hash varchar(64) NOT NULL,
	sender varchar(66) NOT NULL,
	"event" jsonb NOT NULL,
	"timestamp" bigint NOT NULL,
	x_val numeric(21),
	y_val numeric(21),
	fee int8,
	dao_fee int8,
	CONSTRAINT ls_events_pk PRIMARY KEY (id)
);

-- Column comments

COMMENT ON COLUMN public.ls_events.id IS 'event.key.account_address + "_" + event.key.creation_number + "_" + event.sequence_number';
COMMENT ON COLUMN public.ls_events."version" IS 'tx.version';
COMMENT ON COLUMN public.ls_events."timestamp" IS 'tx.timestamp in sec';

CREATE INDEX ls_events_version_idx ON ls_events ("version");
CREATE INDEX ls_events_pool_id_idx ON ls_events (pool_id);
CREATE INDEX ls_events_tx_hash_idx ON ls_events (tx_hash);
CREATE INDEX ls_events_sender_idx ON ls_events (sender);
CREATE INDEX ls_events_sq_idx ON ls_events (sq);
CREATE INDEX ls_events_x_val_idx ON ls_events (x_val);
CREATE INDEX ls_events_y_val_idx ON ls_events (y_val);
