-- $Id$
-- Tests for FYQuarter UDF
set schema 'udftest';
set path 'udftest';


values applib.fiscal_quarter(1210, 8, 4);
values applib.fiscal_quarter(date'2222-12-25', 6);
values applib.fiscal_quarter(timestamp'1879-8-05 10:29:45.05', 1);
values applib.fiscal_quarter(date'1601-11-12', 12);

-- these should fail
values applib.fiscal_quarter(date'2000-7-30', 3, 2);
values applib.fiscal_quarter('2001-9-12', 3);
values applib.fiscal_quarter(timestamp'1800-13-01 12:45:38', 1);

-- create view with reference to applib.fiscal_quarter
create view fiscal(fm, fromdt, fromts) as
select fm, applib.fiscal_quarter(datecol, fm), applib.fiscal_quarter(tscol, fm)
from data_source;

select * from fiscal 
order by 1;

-- in expressions
select fm, 'combo: ' || applib.fiscal_quarter(datecol, fm) || applib.fiscal_quarter(tscol, fm)
from data_source
order by 1;

-- cleanup
drop view fiscal;
