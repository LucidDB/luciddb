-- $Id$
set schema 'udftest';
set path 'udftest';

-- define functions
create function cy_quarter(dt date)
returns varchar(128)
language java
specific cy_quarter_date
no sql
external name 'class com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

create function cy_quarter(ts timestamp)
returns varchar(128)
language java
specific cy_quarter_ts
no sql
external name 'class com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

values cy_quarter(DATE'1896-1-11');
values cy_quarter(TIMESTAMP'2001-5-9 9:57:59');
values cy_quarter(DATE'1-1-1');

-- failures
values cy_quarter(TIMESTAMP'2 12:59:21');

-- create view with reference to cy_quarter
create view v1(fm, dateQ, tsQ) as
select fm, cy_quarter(datecol), cy_quarter(tscol)
from data_source;

select * from v1
order by 1;

-- in expressions
select v1.fm, cy_quarter(datecol) || cy_quarter(tscol) || dateQ
from data_source, v1
order by 1;

-- cleanup
drop view v1;

drop routine cy_quarter_date;
drop routine cy_quarter_ts;