-- This file should undo anything in `up.sql`
ALTER TABLE ls_events DROP COLUMN x_val;
ALTER TABLE ls_events DROP COLUMN y_val;
ALTER TABLE ls_events DROP COLUMN fee;