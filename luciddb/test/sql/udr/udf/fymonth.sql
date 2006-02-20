-- $Id$
-- Test queries for FYMonth UDF
set schema 'udftest';
set path 'udftest';


values applib.fymonth(DATE '2005-10-12', 3);
values applib.fymonth(DATE '2006-1-12', 1);

values applib.fymonth(TIMESTAMP '2006-2-12 13:00:00', 3);
values applib.fymonth(TIMESTAMP '1999-3-3 00:00:00', 3);
values applib.fymonth(TIMESTAMP '2006-4-12 13:00:00', 3);

-- create view referencing applib.fymonth
create view fiscal_months(fm, from_date, from_ts) as
select fm, applib.fymonth(datecol, fm), applib.fymonth(tscol, fm)
from data_source;

select * from fiscal_months
order by 1;

-- in expressions
select fm, applib.fymonth(datecol, fm) + applib.fymonth(tscol, fm)
from data_source
order by 1;

-- nested 
select fm, applib.fymonth(datecol, applib.fymonth(datecol, fm))
from data_source
order by 1;

-- should fail can't invoke using specific name
values applib.fymonth_date(DATE'2000-12-25', 2);

-- should fail invalid argument
select fm, applib.fymonth(timecol, fm)
from data_source;

-- cleanup
drop view fiscal_months;
