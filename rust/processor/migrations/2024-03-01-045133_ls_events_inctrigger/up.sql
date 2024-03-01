-- add trigger

CREATE OR REPLACE FUNCTION ls_events_after_insert_trigger()
  	RETURNS trigger AS
$$
DECLARE
	cn int:=0;
	
   	nlast_event int := 0;
    nx_val numeric := 0;
    ny_val numeric := 0;
	nfee int := 0;
begin
	if new.x_val is null 
		and new.y_val is null 
		and new.fee is null 
	then 
		return new;
	end if;
	
	if new.x_val is not null then nx_val := new.x_val; end if;
	if new.y_val is not null then ny_val := new.y_val; end if;

	update ls_pools lp
		set x_val = (case 
				when new.x_val is null then x_val
				else x_val + new.x_val end
			),
			y_val = (case 
				when new.y_val is null then y_val
				else y_val + new.y_val end
			),
			fee = (case 
				when new.fee is null then fee
				else new.fee  end
			),
			last_event = new.sq
		where lp.id = new.pool_id;

	RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

create or replace TRIGGER ls_events_after_insert_trigger
  AFTER INSERT
  ON "ls_events"
  FOR EACH ROW
  EXECUTE PROCEDURE ls_events_after_insert_trigger();

DROP FUNCTION calc_pool(varchar);