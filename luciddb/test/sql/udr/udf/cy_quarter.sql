-- $Id$
set schema 'udftest';
set path 'udftest';


values applib.cy_quarter(DATE'1896-1-11');
values applib.cy_quarter(TIMESTAMP'2001-5-9 9:57:59');
values applib.cy_quarter(DATE'1-1-1');

-- failures
values applib.cy_quarter(TIMESTAMP'2 12:59:21');

-- create view with reference to applib.cy_quarter
create view v1(fm, dateQ, tsQ) as
select fm, applib.cy_quarter(datecol), applib.cy_quarter(tscol)
from data_source;

select * from v1
order by 1;

-- in expressions
select v1.fm, applib.cy_quarter(datecol) || applib.cy_quarter(tscol) || dateQ
from data_source, v1
order by 1;

-- cleanup
drop view v1;
