-- This file should undo anything in `up.sql`
ALTER TABLE ls_pools
    ALTER COLUMN x_val SET NOT NULL;
ALTER TABLE ls_pools
    ALTER COLUMN x_val SET DEFAULT 0;

ALTER TABLE ls_pools
    ALTER COLUMN y_val SET NOT NULL;
ALTER TABLE ls_pools
    ALTER COLUMN y_val SET DEFAULT 0;

ALTER TABLE ls_pools
    ALTER COLUMN fee SET NOT NULL;
ALTER TABLE ls_pools
    ALTER COLUMN fee SET DEFAULT 0;
