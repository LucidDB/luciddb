-- Tests for TimeDimension UDX

create schema udxtest;
set schema 'udxtest';

-- Positive tests

select * from table(applib.time_dimension(1997, 2, 29, 1997, 3, 2, 1))
order by time_key_seq;

select * from 
(select * from table(applib.time_dimension(2060, 1, 31, 2060, 2, 1, 1)))
order by time_key_seq;

-- Negative tests

select * from table(applib.time_dimension(1997, 3, 1, 1997, 2, 1, 1));
select * from table(applib.time_dimension( 1994, 1, 1, 1993, 1, 1, 1)); 
select * from table(applib.time_dimension( 1994, -1, 1, 1995, 1, 1, 1));
select * from table(applib.time_dimension( 1994, 1, -1, 1995, 1, 1, 1 ));
select * from table(applib.time_dimension( 1994, 1, 1, 1995, -1, 1, 1 ));
select * from table(applib.time_dimension( 1994, 1, 1, 1995, 1, -1, 1 ));

-- Time functions

select 
 time_key, 
 applib.calendar_quarter( time_key ), 
 applib.fiscal_quarter( time_key, 4 ), 
 applib.fiscal_month( time_key, 4 ), 
 applib.fiscal_year( time_key, 4 )
from table(applib.time_dimension( 1821, 11, 29, 1821, 12, 1, 1 ))
order by time_key;

-- Negative tests

select time_key, applib.fiscal_quarter( time_key, 0 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1, 1 ));

select time_key, applib.fiscal_quarter( time_key, 13 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1, 1 ));

select time_key, applib.fiscal_month( time_key, 0 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1, 1 ));

select time_key, applib.fiscal_month( time_key, 13 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1, 1 ));

select time_key, applib.fiscal_year( time_key, 0 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1, 1 ));

select time_key, applib.fiscal_year( time_key, 13 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1, 1 ));

-- create views w/ reference to time_dimension

create view td1 as
select * 
from table(applib.time_dimension(1997, 1, 1, 1997, 2, 1, 1));

select * from td1;

create table udxtest.period (
time_key_seq integer,
time_key date,
quarter integer,
yr integer,
calendar_quarter varchar(15) );

insert into udxtest.period
select 
g_period.time_key_seq,
g_period.time_key,
g_period.quarter,
g_period.yr,
g_period.calendar_quarter
from
(select * from table(applib.time_dimension(1996, 5, 1, 1996, 5, 31, 1)))g_period;

select * from udxtest.period
order by 1;

insert into udxtest.period
select
t_period.time_key_seq,
t_period.time_key,
t_period.quarter,
t_period.yr,
t_period.calendar_quarter
from
(select * from table(applib.time_dimension(1996, 5, 12, 1996, 6, 2, 1)))t_period;

select period.time_key, count(period.time_key_seq)
from period
group by period.time_key
having count(period.time_key_seq) > 1
order by 1;


-- check no multiple records for certain dates
select periods.time_key, count(periods.time_key_seq)
from 
(select * from table(applib.time_dimension(1900, 1, 1, 2010, 12, 31, 1)))periods
group by periods.time_key
having count(periods.time_key_seq) > 1
order by 1;

-- LER-1635
create view CAL as
SELECT
"TIME_KEY_SEQ",
"TIME_KEY",
"DAY_OF_WEEK",
"WEEKEND",
"DAY_NUMBER_IN_WEEK",
"DAY_NUMBER_IN_MONTH",
"DAY_NUMBER_IN_YEAR",
"DAY_NUMBER_OVERALL",
"WEEK_NUMBER_IN_YEAR",
"WEEK_NUMBER_OVERALL",
"MONTH_NAME",
"MONTH_NUMBER_IN_YEAR",
"MONTH_NUMBER_OVERALL",
"QUARTER",
"YR",
"CALENDAR_QUARTER",
"WEEK_START_DATE"
FROM TABLE(SPECIFIC "LOCALDB"."APPLIB"."TIME_DIMENSION"(
    2004, 1, 1, 2005, 12, 31, 1));

select time_key, time_key_seq 
from CAL where TIME_KEY_SEQ = 731;

select time_key, time_key_seq 
from CAL where TIME_KEY = CAST('2005-12-31' AS DATE);


--LER-1706

-- check fiscal fields
select 
  time_key, 
  fiscal_week_start_date, 
  fiscal_week_end_date, 
  fiscal_week_number_in_month, 
  fiscal_week_number_in_quarter,
  fiscal_week_number_in_year, 
  fiscal_month_start_date,
  fiscal_month_end_date,
  fiscal_month_number_in_quarter,
  fiscal_month_number_in_year,
  fiscal_quarter_start_date,
  fiscal_quarter_end_date,
  fiscal_quarter_number_in_year,
  fiscal_year_start_date,
  fiscal_year_end_date
from table(applib.time_dimension(2005,12,18,2006,4,2,12));

select 
  time_key, 
  fiscal_week_start_date, 
  fiscal_week_end_date, 
  fiscal_week_number_in_month, 
  fiscal_week_number_in_quarter,
  fiscal_week_number_in_year, 
  fiscal_month_start_date,
  fiscal_month_end_date,
  fiscal_month_number_in_quarter,
  fiscal_month_number_in_year,
  fiscal_quarter_start_date,
  fiscal_quarter_end_date,
  fiscal_quarter_number_in_year,
  fiscal_year_start_date,
  fiscal_year_end_date
from table(applib.time_dimension(2005,12,18,2006,4,2,1));

select
  time_key,
  day_of_week as dow,
  fiscal_week_number_in_month as fwim,
  fiscal_week_number_in_quarter as fwiq,
  fiscal_week_number_in_year as fwiy,
  fiscal_month_number_in_quarter as fmiq,
  fiscal_month_number_in_year as fmiy,
  fiscal_quarter_number_in_year as fqiy
from table (applib.time_dimension(2000,1,1,2001,1,1,2));

select
  time_key,
  day_of_week as dow,
  fiscal_week_number_in_month as fwim,
  fiscal_week_number_in_quarter as fwiq,
  fiscal_week_number_in_year as fwiy,
  fiscal_month_number_in_quarter as fmiq,
  fiscal_month_number_in_year as fmiy,
  fiscal_quarter_number_in_year as fqiy
from table (applib.time_dimension(1999,12,1,2000,2,1,2));
