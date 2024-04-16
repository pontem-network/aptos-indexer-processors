-- rollback: dao_fee
ALTER TABLE ls_pools DROP COLUMN dao_fee;
ALTER TABLE ls_events DROP COLUMN dao_fee;