-- $Id$
set schema 'udftest';
set path 'udftest';

-- define functions
create function day_number(dt Date)
returns integer
language java
specific day_number_date
no sql
external name 'class com.lucidera.luciddb.applib.toDayNumberOverall.FunctionExecute';

create function day_number(ts timestamp)
returns integer
language java
specific day_number_ts
no sql
external name 'class com.lucidera.luciddb.applib.toDayNumberOverall.FunctionExecute';

values day_number(DATE'2343-5-30');
values day_number(TIMESTAMP'1674-10-09 08:00:59');

-- failures
values day_number(DATE'2341');
values day_number(TIMESTAMP'1990 12:12:12');

-- create view with reference to day_number
create view days (fm, fromDate, fromTs) as
select fm, day_number(datecol), day_number(tscol)
from data_source
where day_number(datecol) >= 30 
  or day_number(datecol) >= 30;

select * from days
order by 1;

-- in expressions
select fm, day_number(datecol) + day_number(tscol) / fm
from data_source
order by 1;

-- cleanup 
drop view days;
drop routine day_number_ts;
drop routine day_number_date;