-- $ID: //open/lu/dev/luciddb/test/sql/udr/udx/installJar.sql#1 $
create schema applib;
set schema 'applib';
set path 'applib';

call sqlj.install_jar('file:../../../../plugin/applib.jar','applibJar', 0);

-- define functions
create function applib.time_dimension(startYr int, startMth int, startDay int, endYr int, endMth int, endDay int)
returns table(
  TIME_KEY_SEQ int,
  TIME_KEY date,
  DAY_OF_WEEK varchar(10),
  WEEKEND varchar(1),
  DAY_NUMBER_IN_WEEK int,
  DAY_NUMBER_IN_MONTH int,
  DAY_NUMBER_IN_YEAR int,
  DAY_NUMBER_OVERALL int,
  WEEK_NUMBER_IN_YEAR int,
  WEEK_NUMBER_OVERALL int,
  MONTH_NAME varchar(10),
  MONTH_NUMBER_IN_YEAR int,
  MONTH_NUMBER_OVERALL int,
  QUARTER int,
  YR int,
  CALENDAR_QUARTER varchar(6),
  FIRST_DAY_OF_WEEK date)
language java
parameter style system defined java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.TimeDimension.FunctionExecute';

-- ApplibResource tests
create function tryApplibResourceStr()
returns int
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.ApplibResourceTest.tryApplibResourceStr';

create function tryApplibResourceEx(bl boolean)
returns int
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.ApplibResourceTest.tryApplibResourceEx';

values tryApplibResourceStr();
values tryApplibResourceEx(true);

-- helper UDFs for TimeDimension

create function applib.toCYQuarter(dt date)
returns varchar(6)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

create function applib.toFYQuarter(dt date, fm int)
returns varchar(6)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function applib.toFYMonth(dt date, fm int)
returns int
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

create function applib.toFYYear(dt date, fm int)
returns int
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYYear.FunctionExecute';

