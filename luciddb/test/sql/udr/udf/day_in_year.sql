-- $Id$
-- Tests for DayInYear UDF
set schema 'udftest';
set path 'udftest';

-- define function
create function day_in_year(dt date)
returns integer
language java
specific day_in_year_date
no sql
external name 'class com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

create function day_in_year(ts timestamp)
returns integer
language java
specific day_in_year_timestamp
no sql
external name 'class com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

create function day_in_year(yr integer, mth integer, dt integer)
returns integer
language java
specific day_in_year_ymd
no sql
external name 'class com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

values day_in_year(date'2006-10-31');
values day_in_year(timestamp'2020-1-1 12:59:00');
values day_in_year(1988, 5, 22);

-- create view with reference to day_in_year
create view diy(fm, fromDt, fromTs, fromFm) as
select fm, day_in_year(datecol), day_in_year(tscol), day_in_year(1999, fm, fm)
from data_source;

select * from diy
order by 1;

select fm, day_in_year(fromDt+1800, mod(fromTs, 13), mod(fromFm, 31))
from diy
order by 1;

-- in expressions
select fm, (day_in_year(datecol) + day_in_year(tscol)) / day_in_year(1999, fm, fm) 
from data_source
order by 1;

-- nested
values (day_in_year(2006, day_in_year(date'1800-01-12'), day_in_year(timestamp'1976-01-05 10:50:45')));