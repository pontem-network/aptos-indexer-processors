CREATE TABLE ls_transactios (
	hash varchar NOT NULL,
	address varchar NOT NULL,
	"transaction" jsonb NOT NULL,
	"timestamp" timestamp NULL,
	CONSTRAINT ls_transactios_pk PRIMARY KEY (hash)
);

-- Column comments

COMMENT ON COLUMN public.ls_transactios.hash IS 'transaction hash';
COMMENT ON COLUMN public.ls_transactios.address IS 'sender''s account';
COMMENT ON COLUMN public.ls_transactios."transaction" IS 'protos/rust/src/pb/aptos.transaction.v1.rs#L38';


























































CREATE TABLE "ls_transactions"(
	"hash" VARCHAR(64) NOT NULL PRIMARY KEY,
	"address" VARCHAR(64) NOT NULL,
	"transaction" JSONB NOT NULL,
	"timestamp" TIMESTAMP NOT NULL
);

