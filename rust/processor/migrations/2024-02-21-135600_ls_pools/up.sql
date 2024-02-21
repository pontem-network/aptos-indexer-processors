CREATE TABLE public.ls_pools (
	id varchar(64) NOT NULL,
	x_name varchar NOT NULL,
	y_name varchar NOT NULL,
	curve varchar NOT NULL,
	x_val bigint DEFAULT 0 NULL,
	y_val bigint DEFAULT 0 NULL,
	fee bigint DEFAULT 0 NULL,
	CONSTRAINT ls_pools_pk PRIMARY KEY (id)
);

-- Column comments

COMMENT ON COLUMN public.ls_pools.id IS 'ha256(x_full_name + y_full_name + curve)';
