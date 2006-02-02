-- $Id$
-- Tests for FYQuarter UDF
set schema 'udftest';
set path 'udftest';

-- define functions
create function fy_quarter(yr integer, mth integer, fm integer)
returns varchar(10)
language java
specific fy_quarter_ymfm
no sql
external name 'class com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function fy_quarter(dt date, fm integer)
returns varchar(10)
language java
specific fy_quarter_date
no sql
external name 'class com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function fy_quarter(ts timestamp, fm integer)
returns varchar(10)
language java
specific fy_quarter_timestamp
no sql
external name 'class com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';


values fy_quarter(1210, 8, 4);
values fy_quarter(date'2222-12-25', 6);
values fy_quarter(timestamp'1879-8-05 10:29:45.05', 1);
values fy_quarter(date'1601-11-12', 12);

-- these should fail
values fy_quarter(date'2000-7-30', 3, 2);
values fy_quarter('2001-9-12', 3);
values fy_quarter(timestamp'1800-13-01 12:45:38', 1);

-- create view with reference to fy_quarter
create view fy(fm, fromdt, fromts) as
select fm, fy_quarter(datecol, fm), fy_quarter(tscol, fm)
from data_source;

select * from fy 
order by 1;

-- in expressions
select fm, 'combo: ' || fy_quarter(datecol, fm) || fy_quarter(tscol, fm)
from data_source
order by 1;

