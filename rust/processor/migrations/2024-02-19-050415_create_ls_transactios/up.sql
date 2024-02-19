CREATE TABLE ls_transactions (
	hash varchar(64) NOT NULL,
	sender varchar(64) NOT NULL,
	"transaction" jsonb NOT NULL,
	"timestamp" bigint NULL,
	CONSTRAINT ls_transactions_pk PRIMARY KEY (hash)
);
CREATE INDEX ls_transactions_sender_idx ON ls_transactions (sender);


-- Column comments

COMMENT ON COLUMN ls_transactions.hash IS 'transaction hash';
COMMENT ON COLUMN ls_transactions.sender IS 'sender account';
COMMENT ON COLUMN ls_transactions."transaction" IS 'protos/rust/src/pb/aptos.transaction.v1.rs#L38';