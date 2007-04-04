-- Tests for FiscalTimeDimension UDX

create schema ftd;
set schema 'ftd';

-- note: the last field is getting cut off due to length
select * from table(applib.fiscal_time_dimension(1997, 2, 29, 1997, 3, 2, 1))
order by time_key_seq;

select * from table(applib.fiscal_time_dimension(1997, 2, 29, 1997, 3, 2, 4))
order by time_key_seq;

select * from table(applib.fiscal_time_dimension(1997, 2, 29, 1997, 3, 2, 5))
order by time_key_seq;

select * from table(applib.fiscal_time_dimension(1997, 2, 29, 1997, 3, 2, 2))
order by time_key_seq;

-- check new fields
select 
  time_key, 
  day_number_in_quarter,
  fiscal_year,
  fiscal_day_number_in_quarter,
  fiscal_day_number_in_year,
  week_start_date,
  fiscal_week_start_date,
  week_end_date,
  fiscal_week_end_date
from table(applib.fiscal_time_dimension(2005,12,18,2006,4,2,12))
order by time_key;

select 
  time_key, 
  day_number_in_quarter,
  fiscal_year,
  fiscal_day_number_in_quarter,
  fiscal_day_number_in_year
from table(applib.fiscal_time_dimension(2005,12,18,2006,4,2,1))
order by time_key;

select
  time_key,
  day_number_in_quarter,
  fiscal_year,
  fiscal_day_number_in_quarter,
  fiscal_day_number_in_year,
  week_start_date,
  fiscal_week_start_date,
  week_end_date,
  fiscal_week_end_date
from table (applib.fiscal_time_dimension(2000,1,1,2001,1,1,2))
order by time_key;

select
  time_key,
  day_number_in_quarter,
  fiscal_year,
  fiscal_day_number_in_quarter,
  fiscal_day_number_in_year,
  week_start_date,
  fiscal_week_start_date,
  week_end_date,
  fiscal_week_end_date
from table (applib.fiscal_time_dimension(1999,12,1,2000,2,1,2))
order by time_key;

-- 
select 
  time_key,
  week_number_in_month as wim,
  week_number_in_quarter as wiq,
  week_number_in_year as wiy,
  fiscal_week_number_in_month as fwim,
  fiscal_week_number_in_quarter as fwiq,
  fiscal_week_number_in_year as fwiy,
  fiscal_year,
  fiscal_day_number_in_year
from table("APPLIB"."FISCAL_TIME_DIMENSION"(2010,3,27,2010,4,12,4))
order by time_key;

-- check 1st week date always begins on January 1st 
-- and last week date always ends on last day of the year
select * 
from table(applib.fiscal_time_dimension(2006, 12, 10, 2007, 1, 20, 2))
order by time_key;

-- check leap years
select * from table(applib.fiscal_time_dimension(1996, 2, 20, 1996, 3, 15, 10))
order by time_key;

select * from table(applib.fiscal_time_dimension(2016, 2, 27, 2016, 3, 12, 11))
order by time_key;