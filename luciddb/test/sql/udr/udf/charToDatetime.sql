create schema ctdt;
set schema 'ctdt';

-- below query commented out due to large size of ref file
-- select
--   time_key,
--   day_of_week as dow,
--   week_start_date as week_start,
--   week_number_in_year as wiy,
--   cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4)) as
--     wk_yr,
--   applib.char_to_date(
--     'w-yyyy', 
--     cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4)))
--     as wk_yr_start
-- from table( applib.fiscal_time_dimension(1995, 1, 1, 2010, 12, 31, 1))
-- order by time_key;

select
  time_key,
  day_of_week as dow,
  week_start_date as week_start,
  week_number_in_year as wiy,
  cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4))
    as wk_yr,
  applib.char_to_date(
    'w-yyyy',
    cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4)))
    as wk_yr_start
from table (applib.fiscal_time_dimension(1994, 12, 20, 1995, 1, 15, 1));

select
  time_key,
  day_of_week as dow,
  week_start_date as week_start,
  week_number_in_year as wiy,
  cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4))
    as wk_yr,
  applib.char_to_date(
    'w-yyyy',
    cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4)))
    as wk_yr_start
from table (applib.fiscal_time_dimension(1996, 12, 20, 1997, 1, 15, 3));

select
  time_key,
  day_of_week as dow,
  week_start_date as week_start,
  week_number_in_year as wiy,
  cast(week_number_in_year as varchar(2)) || '/' || cast(yr as varchar(4))
    as wk_yr,
  applib.char_to_date(
    'w/yyyy',
    cast(week_number_in_year as varchar(2)) || '/' || cast(yr as varchar(4)))
    as wk_yr_start
from table (applib.fiscal_time_dimension(1999, 12, 20, 2000, 1, 15, 7));

select
  time_key,
  day_of_week as dow,
  week_start_date as week_start,
  week_number_in_year as wiy,
  cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4))
    as wk_yr,
  applib.char_to_date(
    'w-yyyy',
    cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4)))
    as wk_yr_start
from table (applib.fiscal_time_dimension(2000, 12, 20, 2001, 1, 15, 10));

select
  time_key,
  day_of_week as dow,
  week_start_date as week_start,
  week_number_in_year as wiy,
  cast(yr as varchar(4)) || '-' || cast(week_number_in_year as varchar(2))
    as wk_yr,
  applib.char_to_date(
    'yyyy-w',
    cast(yr as varchar(4)) || '-' || cast(week_number_in_year as varchar(2)))
    as wk_yr_start
from table (applib.fiscal_time_dimension(2006, 12, 20, 2007, 1, 15, 12));

select
  time_key,
  day_of_week as dow,
  week_start_date as week_start,
  week_number_in_year as wiy,
  cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4))
    as wk_yr,
  applib.char_to_date(
    'w-yyyy',
    cast(week_number_in_year as varchar(2)) || '-' || cast(yr as varchar(4)))
    as wk_yr_start
from table (applib.fiscal_time_dimension(2009, 12, 20, 2010, 1, 15, 6));

values (applib.char_to_date('yyyy/w', '2007/01'));
values (applib.char_to_date('w/yyyy', '01/1996'));
values (applib.char_to_date('yyyy/w', '1998/1'));
values (applib.char_to_date('w/yyyy', '1/2010'));
values (applib.char_to_date('*$%w~#yyyy+=', '*$%54~#2000+='));

-- negative tests
values (applib.char_to_date('***yyy-w', '***027-1'));
values (applib.char_to_date('*$%w~#yyyy+=', '*$%01~#1997+'));
values (applib.char_to_date('w-yyyy-M', '1-2000-9'));
values (applib.char_to_date('yyyy-w', '2001-54'));
values (applib.char_to_date('yyyy/w', '-123/2'));
values (applib.char_to_date('yyyy+w', '2007/-2'));

