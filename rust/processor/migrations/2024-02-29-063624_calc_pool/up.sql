CREATE OR REPLACE function calc_pool(fpool_id varchar)
returns int as
$$
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
	select lp.last_event, x_val, y_val, fee
		into olast_event, old_x_val, old_y_val, old_fee
		from ls_pools lp 
		where lp.id = fpool_id
		for NO KEY update;
	
	if olast_event is null then return 0; end if;

	select sq into nlast_event
		from ls_events le 
		where 
			le.pool_id = fpool_id 
			and le.sq > olast_event
			and (
				(x_val is not null and x_val != 0 ) 
				or (y_val is not null and y_val != 0 )
				or (fee is not null and fee != 0 )
			)
				
		order by le.sq desc 
		limit 1;
	
	if nlast_event is null or olast_event >= nlast_event then return 0; end if;

	select sum(x_val), sum(y_val), count(id) into nx_val, ny_val, cn
		from ls_events le 
		where 
			le.pool_id = fpool_id
			and le.sq > olast_event and le.sq <= nlast_event;

	if cn = 0 then return 0; end if;
		
	select fee into nfee
		from ls_events le 
		where 
			le.pool_id = fpool_id 
			and le.fee is not null
			and le.sq > olast_event and le.sq <= nlast_event
		order by sq desc 
		limit 1;

	if nfee is null then
		nfee := old_fee; 
	end if;
	
	update ls_pools 
		set 
			x_val = x_val + nx_val, 
			y_val = y_val + ny_val, 
			fee = nfee, 
			last_event = nlast_event
		where id = fpool_id;
		
	return cn;

end;
$$
language plpgsql;