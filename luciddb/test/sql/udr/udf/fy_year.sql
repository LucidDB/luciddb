-- $Id$
-- Tests for FYYear UDF
set schema 'udftest';
set path 'udftest';


values applib.fiscal_year(date'2006-12-30', 2);
values applib.fiscal_year(timestamp'1800-2-25 1:1:11', 4);

-- these should fail
values applib.fiscal_year(date'1000-4-4', 13);
values applib.fiscal_year('2005-12-1', 9);

-- null input
values applib.fiscal_year(cast (null as timestamp), 2);
values applib.fiscal_year(current_date, cast(null as integer));

-- create view with reference to applib.fiscal_year
create view fyy(fm, fromdt, fromts) as
select fm, applib.fiscal_year(datecol, fm), applib.fiscal_year(tscol, fm)
from data_source;

select * from fyy
order by 1;

-- in expressions
select fm, applib.fiscal_year(datecol, fm) * fm + applib.fiscal_year(tscol, fm)/fm
from data_source
order by 1;

-- cleanup
drop view fyy;
