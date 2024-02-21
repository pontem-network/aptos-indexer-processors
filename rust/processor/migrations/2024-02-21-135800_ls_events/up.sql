CREATE TYPE event_type AS ENUM (
    'pool_created_event', 
	'liquidity_added_event',
    'liquidity_removed_event',
    'swap_event',
    'flashloan_event',
    'oracle_updated_event',
    'update_fee_event',
    'update_dao_fee_event'
);

CREATE TABLE public.ls_events (
	id varchar NOT NULL,
	pool_id varchar(64) NOT NULL,
	"tp" event_type NOT NULL,
	"version" bigint NOT NULL,
	tx_hash varchar(64) NOT NULL,
	sender varchar(66) NOT NULL,
	even_type jsonb NOT NULL,
	"timestamp" bigint NOT NULL,
	CONSTRAINT ls_events_pk PRIMARY KEY (id)
);

-- Column comments

COMMENT ON COLUMN public.ls_events.id IS 'event.key.account_address + "_" + event.key.creation_number + "_" + event.sequence_number';
COMMENT ON COLUMN public.ls_events."version" IS 'tx.version';
COMMENT ON COLUMN public.ls_events."timestamp" IS 'tx.timestamp in sec';
