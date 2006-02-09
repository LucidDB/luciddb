-- $Id$
-- Test queries for FYMonth UDF
set schema 'udftest';
set path 'udftest';

-- define FYMonth functions
create function fymonth(dt date, fm integer)
returns integer
language java
specific fymonth_date
no sql
external name 'class com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

create function fymonth(ts timestamp, fm integer)
returns integer
language java
specific fymonth_ts
no sql
external name 'class com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

-- create view referencing fymonth
create view fiscal_months(fm, from_date, from_ts) as
select fm, fymonth(datecol, fm), fymonth(tscol, fm)
from data_source;

select * from fiscal_months
order by 1;

-- in expressions
select fm, fymonth(datecol, fm) + fymonth(tscol, fm)
from data_source
order by 1;

-- nested 
select fm, fymonth(datecol, fymonth(datecol, fm))
from data_source
order by 1;

-- should fail can't invoke using specific name
values fymonth_date(DATE'2000-12-25', 2);

-- should fail invalid argument
select fm, fymonth(timecol, fm)
from data_source;
