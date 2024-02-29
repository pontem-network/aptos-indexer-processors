-- DROP FUNCTION public.calc_pool(varchar);

CREATE OR REPLACE FUNCTION public.calc_pool(fpool_id character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
	cn int:=0;
	
	olast_event int := 0;
	old_x_val numeric := 0;
   	old_y_val numeric := 0;
   	old_fee int :=0;
   
   	nlast_event int := 0;
    nx_val numeric := 0;
    ny_val numeric := 0;
	nfee int := 0;
begin
	raise info '= = = ';
	
	select lp.last_event, x_val, y_val, fee
		into olast_event, old_x_val, old_y_val, old_fee
		from ls_pools lp 
		where lp.id = fpool_id
		for NO KEY update;
	
	raise info 'olast_event: %', olast_event;
	raise info 'old_x_val: %', old_x_val;
	raise info 'old_y_val: %', old_y_val;
	raise info 'old_fee: %', old_fee;
	
	if olast_event is null then return 0; end if;

	select sq into nlast_event
		from ls_events le 
		where 
			le.pool_id = fpool_id 
			and le.sq > olast_event
		order by le.sq desc 
		limit 1;
	
	raise info 'nlast_event: %', nlast_event;
	
	if nlast_event is null or olast_event >= nlast_event then return 0; end if;

	select sum(x_val), sum(y_val), count(id) into nx_val, ny_val, cn
		from ls_events le 
		where 
			le.pool_id = fpool_id
			and le.sq > olast_event and le.sq <= nlast_event;

	if cn = 0 then return 0; end if;
		
	raise info 'nx_val: %', nx_val;
	raise info 'ny_val: %', ny_val;

	select fee into nfee
		from ls_events le 
		where 
			le.pool_id = fpool_id 
			and le.fee is not null
			and le.sq > olast_event and le.sq <= nlast_event
		order by sq desc 
		limit 1;

	raise info 'nfee: %', nfee;

	
	if nfee is null then nfee := old_fee; end if;
	if nx_val is null then nx_val := 0; end if;
	if ny_val is null then ny_val := 0; end if;
	
	update ls_pools 
		set 
			x_val = x_val + nx_val, 
			y_val = y_val + ny_val, 
			fee = nfee, 
			last_event = nlast_event
		where id = fpool_id;
		
	return cn;

 end;
$function$
;
