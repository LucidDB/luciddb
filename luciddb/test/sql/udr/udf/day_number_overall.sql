-- $Id$
set schema 'udftest';
set path 'udftest';


values applib.day_number_overall(DATE'2343-5-30');
values applib.day_number_overall(TIMESTAMP'1674-10-09 08:00:59');

-- failures
values applib.day_number_overall(DATE'2341');
values applib.day_number_overall(TIMESTAMP'1990 12:12:12');

-- create view with reference to applib.day_number_overall
create view days (fm, fromDate, fromTs) as
select fm, applib.day_number_overall(datecol), applib.day_number_overall(tscol)
from data_source
where applib.day_number_overall(datecol) >= 30 
  or applib.day_number_overall(datecol) >= 30;

select * from days
order by 1;

-- in expressions
select fm, applib.day_number_overall(datecol) + applib.day_number_overall(tscol) / fm
from data_source
order by 1;

-- cleanup 
drop view days;
