

CREATE OR REPLACE function calc_pool(fpool_id varchar)
returns int as
$$
DECLARE 
	temprow RECORD;
	cn int:=0;
	nfee int := 0;
    nx_val numeric := 0;
    ny_val numeric := 0;
   	last_version int := 0;
   	calc_ids varchar[] := array[]::varchar[];
begin
	
	select sum(le.x_val),sum(le.y_val), array_agg(le.id), max(le."version")
		into nx_val, ny_val, calc_ids, last_version
		from (
			select l1.id, l1.x_val, l1.y_val, l1."version" 
				from ls_events l1 
				where 
					l1.pool_id = '92a538bba6b8c1932815e0064e84f35c1c37781a30aebe52e776edff782b4d9b'
					and l1.calc = false
				order by l1.version asc
				limit 100
		) le;
	
	select le.fee into nfee
		from ls_events le 
		where le.id = any (calc_ids) and le.fee is not null 
		order by id desc
		limit 1;
		
	
	cn := array_length(calc_ids,1);

   	if cn is null then
   		return 0;
   	end if;	

	if nfee <> 0 then
   		update ls_pools
   			set fee = nfee, last_tx_version  = last_version
   			where lp.id = fpool_id;
   	end if;

   	if nx_val <> 0 or ny_val <> 0 then
   		update ls_pools 
   			set x_val = x_val + nx_val, y_val = y_val + ny_val, last_tx_version = last_version
   			where id = fpool_id;
   	end if;	

   	update ls_events 
		set calc = true 
		where id = any (calc_ids);
   
   	raise info '= = = ';
   	raise info 'cn: %', cn;
   	raise info 'nfee: %', nfee;
    raise info 'nx_val: %', nx_val;
   	raise info 'nx_val: %', nx_val;
    raise info 'calc_ids: %', calc_ids;
    raise info 'last_version: %', last_version;
   
    return cn;
 end;
$$
language plpgsql;