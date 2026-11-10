alter table customer_source modify modified_date  default SYSTIMESTAMP;
create or replace trigger new_lat  before insert or update on customer_source
for each row 
begin
:NEW.modified_date := systimestamp;
end;
/