-- $Id$
create schema jdt;
set schema 'jdt';

--
-- julian day tests
--
values applib.day_from_julian_start(DATE'2005-10-30');
values applib.day_from_julian_start(DATE'1582-10-3');
values applib.day_from_julian_start(DATE'1582-10-4');
values applib.day_from_julian_start(DATE'1582-10-15');
values applib.day_from_julian_start(DATE'1582-10-16');

values applib.julian_day_to_timestamp(
  applib.day_from_julian_start(
    cast(TIMESTAMP'2396-2-29 20:45:47' as DATE)));
values applib.julian_day_to_date(-3452);
 
-- null input
values applib.day_from_julian_start(cast (null as DATE));
values applib.julian_day_to_date(cast (null as integer));
values applib.julian_day_to_timestamp(cast (null as integer));

-- failures (the missing 10 days from julian to gregorian)
values applib.day_from_julian_start(DATE'1582-10-5');
values applib.day_from_julian_start(DATE'1582-10-14');

-- view test
select TIME_KEY, applib.day_from_julian_start("TIME_KEY") 
from table(applib.time_dimension(1997, 9, 17, 1997, 10, 1));

create view tempview as 
  select 
    "TIME_KEY",
    applib.day_from_julian_start("TIME_KEY") as jd,
    applib.day_from_julian_start("FIRST_DAY_OF_WEEK") as fdow_jd,
    applib.julian_day_to_date("DAY_NUMBER_OVERALL" + 2440588) as to_date,
    applib.julian_day_to_timestamp("DAY_NUMBER_OVERALL" + 2440588) as to_ts
  from table(applib.time_dimension(2001, 1, 30, 2001, 2, 5));

select * from tempview
order by 1;

-- in expressions
select applib.day_from_julian_start("TIME_KEY")+ applib.day_from_julian_start("FIRST_DAY_OF_WEEK") / 2
from table(applib.time_dimension(2001, 1, 30, 2001, 2, 5));

--
-- current_date_in_julian test
--
values (applib.day_from_julian_start(current_date) = applib.current_date_in_julian());

values (applib.current_date_in_julian() < applib.day_from_julian_start(DATE'2005-12-12'));
values (applib.julian_day_to_date(applib.current_date_in_julian()) = current_date);

--
-- tests with fiscal_time_dimension udx around epoch and missing julian to 
-- gregorian dates
--
select time_key, day_of_week, day_number_overall, day_from_julian,
  applib.julian_day_to_date(day_from_julian) as to_date, 
  applib.julian_day_to_timestamp(day_from_julian) as to_timestamp
from table( applib.fiscal_time_dimension (1969, 12, 29, 1970, 1, 4, 8) );

select time_key, day_of_week, day_number_overall, day_from_julian,
  applib.julian_day_to_date(day_from_julian) as to_date,
  applib.julian_day_to_timestamp(day_from_julian) as to_timestamp
from table( applib.fiscal_time_dimension (1582, 10, 2, 1582, 10, 16, 7) );

-- cleanup
drop view tempview;
drop schema jdt cascade;
