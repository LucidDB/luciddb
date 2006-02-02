-- $Id$
-- Tests for FYYear UDF
set schema 'udftest';
set path 'udftest';

-- define functions
create function fy_year(dt date, fm integer)
returns integer
language java
specific fy_year_date
no sql
external name 'class com.lucidera.luciddb.applib.FYYear.FunctionExecute';

create function fy_year(ts timestamp, fm integer)
returns integer
language java
specific fy_year_timestamp
no sql
external name 'class com.lucidera.luciddb.applib.FYYear.FunctionExecute';

values fy_year(date'2006-12-30', 2);
values fy_year(timestamp'1800-2-25 1:1:11', 4);

-- these should fail
values fy_year(date'1000-4-4', 1);
values fy_year(2005-12-1, 9);

-- create view with reference to fy_year
create view fyy(fm, fromdt, fromts) as
select fm, fy_year(datecol, fm), fy_year(tscol, fm)
from data_source;

select * from fyy
order by 1;

-- in expressions
select fm, fy_year(datecol, fm) * fm + fy_year(tscol, fm)/fm
from data_source
order by 1;

