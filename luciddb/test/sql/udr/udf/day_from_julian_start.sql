-- $Id$
set schema 'udftest';
set path 'udftest';

values applib.day_from_julian_start(DATE'2005-10-30');
values applib.day_from_julian_start(DATE'1582-10-3');
values applib.day_from_julian_start(DATE'1582-10-4');
values applib.day_from_julian_start(DATE'1582-10-15');
values applib.day_from_julian_start(DATE'1582-10-16');

-- failures (the missing 10 days from julian to gregorian)
values applib.day_from_julian_start(DATE'1582-10-5');
values applib.day_from_julian_start(DATE'1582-10-14');

-- view test
select TIME_KEY, applib.day_from_julian_start("TIME_KEY") 
from table(applib.time_dimension(1997, 9, 17, 1997, 10, 1));

create view tempview as select applib.day_from_julian_start("TIME_KEY"), applib.day_from_julian_start("FIRST_DAY_OF_WEEK")
from table(applib.time_dimension(2001, 1, 30, 2001, 2, 5));

select * from tempview
order by 1;

-- in expressions
select applib.day_from_julian_start("TIME_KEY")+ applib.day_from_julian_start("FIRST_DAY_OF_WEEK") / 2
from table(applib.time_dimension(2001, 1, 30, 2001, 2, 5));

-- cleanup
drop view tempview;