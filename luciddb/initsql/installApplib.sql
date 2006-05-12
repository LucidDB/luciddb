-- $Id$
create schema applib;
set schema 'applib';
set path 'applib';

call sqlj.install_jar('file:${FARRAGO_HOME}/plugin/applib.jar','applibJar', 0);

-- UDFs
-- define CharReplace functions
create function applib.char_replace(str varchar(128), oldC varchar(128), newC varchar(128)) 
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.CharReplaceUdf.execute';

create function applib.char_replace(str varchar(128), oldC integer, newC integer) 
returns varchar(128)
language java
specific char_replace_int
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.CharReplaceUdf.execute';

-- define CleanPhoneInternational functions
create function applib.clean_phone_international(str varchar(128), b boolean)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.phone.CleanPhoneInternationalUdf.execute';

-- define CleanPhone functions
create function applib.clean_phone(str varchar(128))
returns varchar(128)
language java
specific clean_phone_no_format
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.phone.CleanPhoneUdf.execute';

create function applib.clean_phone(inStr varchar(128), format integer)
returns varchar(128)
language java
specific clean_phone_int_format
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.phone.CleanPhoneUdf.execute';

create function applib.clean_phone(inStr varchar(128), format integer, reject boolean)
returns varchar(128)
language java
specific clean_phone_int_format_rejectable
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.phone.CleanPhoneUdf.execute';

create function applib.clean_phone(inStr varchar(128), format varchar(128), reject boolean)
returns varchar(128)
language java
specific clean_phone_str_format_rejectable
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.phone.CleanPhoneUdf.execute';

-- define ContainsNumber function
create function applib.contains_number(str varchar(128))
returns boolean
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.ContainsNumberUdf.execute';

-- define CYQuarter functions
create function applib.calendar_quarter(dt date)
returns varchar(128)
language java
specific calendar_quarter_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.CalendarQuarterUdf.execute';

create function applib.calendar_quarter(ts timestamp)
returns varchar(128)
language java
specific calendar_quarter_ts
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.CalendarQuarterUdf.execute';

-- define internalDate function (example function)
create function applib.internal_date(indate bigint)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.contrib.InternalDateUdf.execute';

-- define DAYINYEAR function
create function applib.day_in_year(dt date)
returns integer
language java
specific DAYINYEAR_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.DayInYearUdf.execute';

create function applib.day_in_year(ts timestamp)
returns integer
language java
specific DAYINYEAR_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.DayInYearUdf.execute';

create function applib.day_in_year(yr integer, mth integer, dt integer)
returns integer
language java
specific DAYINYEAR_YMD
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.DayInYearUdf.execute';

-- define dayNumberOverall functions
create function applib.day_number_overall(dt Date)
returns integer
language java
specific DAYNUMBEROVERALL_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.DayNumberOverallUdf.execute';

create function applib.day_number_overall(ts timestamp)
returns integer
language java
specific DAYNUMBEROVERALL_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.DayNumberOverallUdf.execute';

-- define FYMonth functions
create function applib.fiscal_month(dt date, fm integer)
returns integer
language java
specific FYMONTH_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalMonthUdf.execute';

create function applib.fiscal_month(ts timestamp, fm integer)
returns integer
language java
specific FYMONTH_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalMonthUdf.execute';

-- define FYQuarter functions
create function applib.fiscal_quarter(yr integer, mth integer, fm integer)
returns varchar(10)
language java
specific FYQUARTER_YMFM
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalQuarterUdf.execute';

create function applib.fiscal_quarter(dt date, fm integer)
returns varchar(10)
language java
specific FYQUARTER_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalQuarterUdf.execute';

create function applib.fiscal_quarter(ts timestamp, fm integer)
returns varchar(10)
language java
specific FYQUARTER_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalQuarterUdf.execute';

-- define FYYear functions
create function applib.fiscal_year(dt date, fm integer)
returns integer
language java
specific FYYEAR_DATE
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalYearUdf.execute';

create function applib.fiscal_year(ts timestamp, fm integer)
returns integer
language java
specific FYYEAR_TS
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.FiscalYearUdf.execute';

-- define leftN functions
create function applib.leftn(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.LeftNUdf.execute';

-- define rand functions
create function applib.rand(minVal integer, maxVal integer)
returns integer
language java
not deterministic
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.numeric.RandUdf.execute';

-- define repeater function
create function applib.REPEATER(str varchar(128), times integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.RepeaterUdf.execute';

-- define rightn function
create function applib.RIGHTN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.RightNUdf.execute';

-- define StrReplace function
create function applib.str_replace(inStr varchar(128), oldStr varchar(128), newStr varchar(128))
returns varchar(128)
language java
no sql
external name "applib.applibJar:com.lucidera.luciddb.applib.string.StrReplaceUdf.execute";

-- define convert_date function
create function applib.convert_date(str varchar(128), mask varchar(50), rej boolean)
returns date
language java
specific convert_date_rejectable
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.ConvertDateUdf.execute';

create function applib.convert_date(str varchar(128), mask varchar(50))
returns date
language java
specific convert_date_not_rejectable
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.ConvertDateUdf.execute';

-- define dayFromJulianStart
create function applib.day_from_julian_start(dt Date)
returns integer
language sql
contains sql
return (
  applib.day_number_overall(dt) + 2440588
);

-- define padweeknumber
create function applib.padweeknumber(wk_num_as_yr integer)
returns varchar(16)
language sql
contains sql
return (
  case 
    WHEN (wk_num_as_yr < 10) THEN ('0' || cast (wk_num_as_yr as varchar(1)))
    ELSE cast (wk_num_as_yr as varchar(2))  
  end
);

-- define INSTR
create function applib.instr(str varchar(65535), subStr varchar(65535), startPos int, nthAppearance int)
returns int
language java
specific instr_with_optional_vars
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.InStrUdf.execute';

create function applib.instr(str varchar(65535), subStr varchar(65535))
returns int
language java
specific instr_without_optional_vars
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.string.InStrUdf.execute';

----
-- UDXs
----

-- define TIMEDIMENSION
create function APPLIB.TIME_DIMENSION(startYr int, startMth int, startDay int, endYr int, endMth int, endDay int)
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
external name 'applib.applibJar:com.lucidera.luciddb.applib.datetime.TimeDimensionUdx.execute';

----
-- Application variables (a.k.a. "appvars")
-- 
-- Appvars are defined within a named context, allowing sets of
-- variables to be manipulated as a unit.
----

-- Creates a variable (or a context if var_id is null).  When var_id
-- is not null, the context is implicitly created if it does not exist.
-- The initial value for the new variable is null.  If the variable
-- already exists, this call has no effect.
create procedure applib.create_var(
    context_id varchar(128), 
    var_id varchar(128),
    description varchar(65535))
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.variable.CreateAppVarUdp.execute';

-- Deletes a variable (or a context if var_id is null).
create procedure applib.delete_var(
    context_id varchar(128), 
    var_id varchar(128))
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.variable.DeleteAppVarUdp.execute';

-- Sets the value for a variable.  var_id must not be null, and must
-- reference a previously created variable.
create procedure applib.set_var(
    context_id varchar(128), 
    var_id varchar(128), 
    new_value varchar(65535))
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.variable.SetAppVarUdp.execute';

-- Flushes modifications to a variable (or a context if var_id is null).
-- Before flush, there is no guarantee that modifications have
-- been made permanent.
create procedure applib.flush_var(
    context_id varchar(128), 
    var_id varchar(128))
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.variable.FlushAppVarUdp.execute';

-- Retrieves the current value of a variable.  var_id must not be null,
-- and must reference a previously created variable.
-- Declared as deterministic to let the optimizer know that it should
-- be evaluated once per statement (rather than once per row) when
-- the arguments are constant literals.
create function applib.get_var(
    context_id varchar(128), 
    var_id varchar(128)) 
returns varchar(65535)
language java
deterministic
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.variable.GetAppVarUdf.execute';
