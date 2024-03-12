CREATE TABLE public.ls_pools (
	id varchar(64) NOT NULL,
	x_name varchar NOT NULL,
	y_name varchar NOT NULL,
	curve varchar NOT NULL,
	x_val numeric(21) DEFAULT 0 NOT NULL,
	y_val numeric(21) DEFAULT 0 NOT NULL,
	fee bigint DEFAULT 0 NOT NULL,
	"last_event" bigint DEFAULT 0 NOT NULL,
	CONSTRAINT ls_pools_pk PRIMARY KEY (id)
);



-- Column comments

COMMENT ON COLUMN public.ls_pools.id IS 'sha256(x_full_name + y_full_name + curve)';

