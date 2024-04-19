
ALTER TABLE ls_pools DROP COLUMN last_version;
ALTER TABLE ls_pools ADD COLUMN last_event bigint DEFAULT 0 NOT NULL;

-- add trigger

CREATE OR REPLACE FUNCTION ls_events_after_insert_trigger()
  	RETURNS trigger AS
$$
begin
	if new.x_val is null 
		and new.y_val is null 
		and new.fee is null 
		and new.dao_fee is null 
	then 
		return new;
	end if;

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
				else new.fee end
			),
			dao_fee = (case 
				when new.dao_fee is null then dao_fee
				else new.dao_fee end
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
