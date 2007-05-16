-- $Id$
-- Test queries for FYMonth UDF
set schema 'udftest';
set path 'udftest';


values applib.fiscal_month(DATE '2005-10-12', 3);
values applib.fiscal_month(DATE '2006-1-12', 1);

values applib.fiscal_month(TIMESTAMP '2006-2-12 13:00:00', 3);
values applib.fiscal_month(TIMESTAMP '1999-3-3 00:00:00', 3);
values applib.fiscal_month(TIMESTAMP '2006-4-12 13:00:00', 3);

-- null input
values applib.fiscal_month(cast(null as date), 2);
values applib.fiscal_month(current_timestamp, cast(null as integer));

-- create view referencing applib.fiscal_month
create view fiscal_months(fm, from_date, from_ts) as
select fm, applib.fiscal_month(datecol, fm), applib.fiscal_month(tscol, fm)
from data_source;

select * from fiscal_months
order by 1;

-- in expressions
select fm, applib.fiscal_month(datecol, fm) + applib.fiscal_month(tscol, fm)
from data_source
order by 1;

-- nested 
select fm, applib.fiscal_month(datecol, applib.fiscal_month(datecol, fm))
from data_source
order by 1;

-- should fail can't invoke using specific name
values applib.fiscal_month_date(DATE'2000-12-25', 2);

-- should fail invalid argument
select fm, applib.fiscal_month(timecol, fm)
from data_source;

-- cleanup
drop view fiscal_months;
