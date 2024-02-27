-- This file should undo anything in `up.sql`
ALTER TABLE ls_pools
    ALTER COLUMN fee DROP NOT NULL;