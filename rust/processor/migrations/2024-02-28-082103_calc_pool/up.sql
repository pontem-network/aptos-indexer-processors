
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
	
	FOR temprow IN
        SELECT le.id, le.pool_id, le.x_val, le.y_val, le.fee, le.version 
        	FROM ls_events le
        	where le.pool_id = fpool_id and le.calc = false and (
        		(le.x_val is not null and le.x_val != 0 ) 
        			or (le.y_val is not null and le.y_val != 0 )
        			or (le.x_val is not null and le.x_val != 0 )
    		)
        	order by le.version asc
    		LIMIT 4000
    loop
		cn := cn + 1;
		last_version := temprow.version;
		
		if temprow.fee > 0 then 
			nfee := temprow.fee;
		end if;
	
		nx_val := nx_val + temprow.x_val;
		ny_val := ny_val + temprow.y_val;

	
		calc_ids = calc_ids || array[temprow.id];
    END LOOP;
   
	update ls_events set calc = true where id = any (calc_ids);
   
   	if nfee > 0 then
   		update ls_pools
   			set fee = nfee, last_tx_version  = last_version
   			where lp.id = fpool_id;
   	end if;
   
   	if nx_val <> 0 or ny_val <> 0 then
   		update ls_pools 
   			set x_val = x_val + nx_val, y_val = y_val + ny_val, last_tx_version = last_version
   			where id = fpool_id;
   	end if;
   
    return cn;
 end;
$$
language plpgsql;