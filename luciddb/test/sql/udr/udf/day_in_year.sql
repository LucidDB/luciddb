-- $Id$
-- Tests for DayInYear UDF
set schema 'udftest';
set path 'udftest';


values applib.day_in_year(date'2006-10-31');
values applib.day_in_year(timestamp'2020-1-1 12:59:00');
values applib.day_in_year(1988, 5, 22);

-- null input
values applib.day_in_year(cast (null as date));
values applib.day_in_year(1929, cast(null as integer), 9);

-- create view with reference to applib.day_in_year
create view diy(fm, fromDt, fromTs, fromFm) as
select fm, applib.day_in_year(datecol), applib.day_in_year(tscol), applib.day_in_year(1999, fm, fm)
from data_source;

select * from diy
order by 1;

select fm, applib.day_in_year(fromDt+1800, mod(fromTs, 13), mod(fromFm, 31))
from diy
order by 1;

-- in expressions
select fm, (applib.day_in_year(datecol) + applib.day_in_year(tscol)) / applib.day_in_year(1999, fm, fm) 
from data_source
order by 1;

-- nested
values (applib.day_in_year(2006, applib.day_in_year(date'1800-01-12'), applib.day_in_year(timestamp'1976-01-05 10:50:45')));

-- cleanup
drop view diy;
