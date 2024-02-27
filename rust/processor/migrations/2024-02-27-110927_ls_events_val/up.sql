-- Your SQL goes here
ALTER TABLE ls_events ADD x_val numeric NOT NULL DEFAULT 0;
ALTER TABLE ls_events ADD y_val numeric NOT NULL DEFAULT 0;
ALTER TABLE ls_events ADD fee int8 NOT NULL DEFAULT 0;