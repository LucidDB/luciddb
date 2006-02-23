-- $ID: //open/lu/dev/luciddb/test/sql/udr/udx/timeDimension.sql#1 $ 
-- Tests for TimeDimension UDX

create schema udxtest;
set schema 'udxtest';

-- Positive tests

select * from table(applib.time_dimension(1997, 2, 29, 1997, 4, 1));

select   
  TIME_KEY,
  DAY_OF_WEEK,
  WEEKEND,
  DAY_NUMBER_IN_WEEK,
  DAY_NUMBER_IN_MONTH,
  DAY_NUMBER_IN_YEAR,
  WEEK_NUMBER_IN_YEAR,
  MONTH_NAME,
  MONTH_NUMBER_IN_YEAR,
  QUARTER,
  YR,
  CALENDAR_QUARTER,
  FIRST_DAY_OF_WEEK
from table(applib.time_dimension(1997, 2, 1, 1997, 2, 29))
order by TIME_KEY;

select * from 
(select * from table(applib.time_dimension(1997, 1, 31, 1997, 3, 1)))
order by time_key_seq;

select * from table(applib.time_dimension(2060, 0, 1, 2060, 2, 1))
order by 1;

select * from table(applib.time_dimension(2006, 2, 1, 2006, 2, 1))
order by time_key_seq;

select * from table(applib.time_dimension( 1814, 1, 1, 1821, 12, 31))
order by day_of_week, time_key_seq;

-- Negative tests

select * from table(applib.time_dimension(1997, 3, 1, 1997, 2, 1));
select * from table(applib.time_dimension( 1994, 1, 1, 1993, 1, 1)); 
select * from table(applib.time_dimension( 1994, -1, 1, 1995, 1, 1));
select * from table(applib.time_dimension( 1994, 1, -1, 1995, 1, 1 ));
select * from table(applib.time_dimension( 1994, 1, 1, 1995, -1, 1 ));
select * from table(applib.time_dimension( 1994, 1, 1, 1995, 1, -1 ));

-- Time functions

select 
 time_key, 
 applib.calendar_quarter( time_key ), 
 applib.fiscal_quarter( time_key, 4 ), 
 applib.fiscal_month( time_key, 4 ), 
 applib.fiscal_year( time_key, 4 )
from table(applib.time_dimension( 1999, 1, 1, 1999, 12, 31 ))
order by time_key;

-- Negative tests

select time_key, applib.fiscal_quarter( time_key, 0 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1 ));

select time_key, applib.fiscal_quarter( time_key, 13 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1 ));

select time_key, applib.fiscal_month( time_key, 0 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1 ));

select time_key, applib.fiscal_month( time_key, 13 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1 ));

select time_key, applib.fiscal_year( time_key, 0 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1 ));

select time_key, applib.fiscal_year( time_key, 13 ) 
from table(applib.time_dimension( 1994, 1, 1, 1994, 1, 1 ));

-- create views w/ reference to time_dimension

create view td1 as
select * 
from table(applib.time_dimension(1997, 1, 1, 1997, 2, 1));

select * from td1;

create view td2 as 
select time_key, day_of_week, weekend, calendar_quarter 
from table(applib.time_dimension(1997, 1, 1, 1997, 2, 1));

select * from td2;


create table udxtest.period (
time_key_seq integer,
time_key date,
quarter integer,
yr integer,
calendar_quarter varchar(15) );

select * from udxtest.period
order by time_key;

insert into udxtest.period
select 
g_period.time_key_seq,
g_period.time_key,
g_period.quarter,
g_period.yr,
g_period.calendar_quarter
from
(select * from table(applib.time_dimension(1995, 1, 1, 1996, 12, 31)))g_period;

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
(select * from table(applib.time_dimension(1996, 10, 1, 1996, 10, 31)))t_period;
select period.time_key, count(period.time_key_seq)
from period
group by period.time_key
having count(period.time_key_seq) > 1;


-- check no multiple records for certain dates
select periods.time_key, count(periods.time_key_seq)
from 
(select * from table(applib.time_dimension(1900, 1, 1, 2010, 12, 31)))periods
group by periods.time_key
having count(periods.time_key_seq) > 1;
