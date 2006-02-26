create schema applib;
set schema 'applib';
set path 'applib';

call sqlj.install_jar('file:plugin/applib.jar','applibJar', 0);

-- UDFs
-- define CharReplace functions
create function CHARREPLACE(str varchar(128), oldC varchar(128), newC varchar(128)) 
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CharReplace.FunctionExecute';

create function CHARREPLACE(str varchar(128), oldC integer, newC integer) 
returns varchar(128)
language java
specific CHAR_REPLACE_INT
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CharReplace.FunctionExecute';

-- define CleanPhoneInternational functions
create function CLEANPHONEINTERNATIONAL(str varchar(128), b boolean)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhoneInternational.FunctionExecute';

-- define CleanPhone functions
create function CLEANPHONE(str varchar(128))
returns varchar(128)
language java
specific CLEANPHONE_NOFORMAT
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

create function CLEANPHONE(inStr varchar(128), format integer)
returns varchar(128)
language java
specific CLEANPHONE_INTFORMAT
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

create function CLEANPHONE(inStr varchar(128), format integer, reject boolean)
returns varchar(128)
language java
specific CLEANPHONE_INTFORMAT_REJECTABLE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

create function CLEANPHONE(inStr varchar(128), format varchar(128), reject boolean)
returns varchar(128)
language java
specific CLEANPHONE_STRFORMAT_REJECTABLE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

-- define ContainsNumber function
create function CONTAINSNUMBER(str varchar(128))
returns boolean
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.containsNumber.FunctionExecute';

-- define CYQuarter functions
create function CYQUARTER(dt date)
returns varchar(128)
language java
specific CYQUARTER_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

create function CYQUARTER(ts timestamp)
returns varchar(128)
language java
specific CYQUARTER_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

-- define internalDate function
create function INTERNALDATE(indate bigint)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DateBBInternal.FunctionExecute';

-- define DAYINYEAR function
create function DAYINYEAR(dt date)
returns integer
language java
specific DAYINYEAR_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

create function DAYINYEAR(ts timestamp)
returns integer
language java
specific DAYINYEAR_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

create function day_in_year(yr integer, mth integer, dt integer)
returns integer
language java
specific DAYINYEAR_YMD
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

-- define dayNumberOverall functions
create function DAYNUMBEROVERALL(dt Date)
returns integer
language java
specific DAYNUMBEROVERALL_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDayNumberOverall.FunctionExecute';

create function DAYNUMBEROVERALL(ts timestamp)
returns integer
language java
specific DAYNUMBEROVERALL_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDayNumberOverall.FunctionExecute';

-- define FYMonth functions
create function FYMONTH(dt date, fm integer)
returns integer
language java
specific FYMONTH_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

create function FYMONTH(ts timestamp, fm integer)
returns integer
language java
specific FYMONTH_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

-- define FYQuarter functions
create function FYQUARTER(yr integer, mth integer, fm integer)
returns varchar(10)
language java
specific FYQUARTER_YMFM
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function FYQUARTER(dt date, fm integer)
returns varchar(10)
language java
specific FYQUARTER_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function FYQUARTER(ts timestamp, fm integer)
returns varchar(10)
language java
specific FYQUARTER_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

-- define FYYear functions
create function FYYEAR(dt date, fm integer)
returns integer
language java
specific FYYEAR_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYYear.FunctionExecute';

create function FYYEAR(ts timestamp, fm integer)
returns integer
language java
specific FYYEAR_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYYear.FunctionExecute';

-- define leftN functions
create function LEFTN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.leftN.FunctionExecute';

-- define rand functions
create function RAND(minVal integer, maxVal integer)
returns integer
language java
not deterministic
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.rand.FunctionExecute';

-- define repeater function
create function REPEATER(str varchar(128), times integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.repeater.FunctionExecute';

-- define rightn function
create function RIGHTN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.rightN.FunctionExecute';

-- define StrReplace function
create function STRREPLACE(inStr varchar(128), oldStr varchar(128), newStr varchar(128))
returns varchar(128)
language java
no sql
external name "applib.applibJar:com.lucidera.luciddb.applib.strReplace.FunctionExecute";

-- define TODATE function
create function TODATE(str varchar(128), mask varchar(50), rej boolean)
returns date
language java
specific TODATE_REJCHOICE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDate.FunctionExecute';

create function TODATE(str varchar(128), mask varchar(50))
returns date
language java
specific TODATE_NOCHOICE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDate.FunctionExecute';

-- UDXs
-- define TIMEDIMENSION
create function APPLIB.TIMEDIMENSION(startYr int, startMth int, startDay int, endYr int, endMth int, endDay int)
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
