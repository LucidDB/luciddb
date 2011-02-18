-- $Id$
create or replace schema localdb.applib;
set schema 'localdb.applib';
set path 'localdb.applib';

create or replace jar applib.applibJar
library 'file:${FARRAGO_HOME}/plugin/eigenbase-applib.jar'
options(0);

-- UDFs
-- define CharReplace functions
create or replace function applib.char_replace(str varchar(65535), oldC varchar(65535), newC varchar(65535)) 
returns varchar(65535)
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.CharReplaceUdf.execute';

create or replace function applib.char_replace(str varchar(65535), oldC integer, newC integer) 
returns varchar(65535)
language java
specific char_replace_int
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.CharReplaceUdf.execute';

-- define CleanPhoneInternational functions
create or replace function applib.clean_phone_international(str varchar(128), b boolean)
returns varchar(128)
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.phone.CleanPhoneInternationalUdf.execute';

-- define CleanPhone functions
create or replace function applib.clean_phone(str varchar(128))
returns varchar(128)
language java
specific clean_phone_no_format
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.phone.CleanPhoneUdf.execute';

create or replace function applib.clean_phone(inStr varchar(128), format integer)
returns varchar(128)
language java
specific clean_phone_int_format
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.phone.CleanPhoneUdf.execute';

create or replace function applib.clean_phone(inStr varchar(128), format integer, reject boolean)
returns varchar(128)
language java
specific clean_phone_int_format_rejectable
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.phone.CleanPhoneUdf.execute';

create or replace function applib.clean_phone(inStr varchar(128), format varchar(128), reject boolean)
returns varchar(128)
language java
specific clean_phone_str_format_rejectable
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.phone.CleanPhoneUdf.execute';

-- define ContainsNumber function
create or replace function applib.contains_number(str varchar(65535))
returns boolean
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.ContainsNumberUdf.execute';

-- define CYQuarter functions
create or replace function applib.calendar_quarter(dt date)
returns varchar(128)
language java
specific calendar_quarter_date
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.CalendarQuarterUdf.execute';

create or replace function applib.calendar_quarter(ts timestamp)
returns varchar(128)
language java
specific calendar_quarter_ts
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.CalendarQuarterUdf.execute';

-- define internalDate function (example function)
create or replace function applib.internal_date(indate bigint)
returns varchar(128)
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.contrib.InternalDateUdf.execute';

-- define DAYINYEAR function
create or replace function applib.day_in_year(dt date)
returns integer
language java
specific DAYINYEAR_DATE
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.DayInYearUdf.execute';

create or replace function applib.day_in_year(ts timestamp)
returns integer
language java
specific DAYINYEAR_TS
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.DayInYearUdf.execute';

create or replace function applib.day_in_year(yr integer, mth integer, dt integer)
returns integer
language java
specific DAYINYEAR_YMD
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.DayInYearUdf.execute';

-- define dayNumberOverall functions
create or replace function applib.day_number_overall(dt Date)
returns integer
language java
specific DAYNUMBEROVERALL_DATE
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.DayNumberOverallUdf.execute';

create or replace function applib.day_number_overall(ts timestamp)
returns integer
language java
specific DAYNUMBEROVERALL_TS
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.DayNumberOverallUdf.execute';

-- define FYMonth functions
create or replace function applib.fiscal_month(dt date, fm integer)
returns integer
language java
specific FYMONTH_DATE
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalMonthUdf.execute';

create or replace function applib.fiscal_month(ts timestamp, fm integer)
returns integer
language java
specific FYMONTH_TS
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalMonthUdf.execute';

-- define FYQuarter functions
create or replace function applib.fiscal_quarter(yr integer, mth integer, fm integer)
returns varchar(10)
language java
specific FYQUARTER_YMFM
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalQuarterUdf.execute';

create or replace function applib.fiscal_quarter(dt date, fm integer)
returns varchar(10)
language java
specific FYQUARTER_DATE
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalQuarterUdf.execute';

create or replace function applib.fiscal_quarter(ts timestamp, fm integer)
returns varchar(10)
language java
specific FYQUARTER_TS
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalQuarterUdf.execute';

-- define FYYear functions
create or replace function applib.fiscal_year(dt date, fm integer)
returns integer
language java
specific FYYEAR_DATE
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalYearUdf.execute';

create or replace function applib.fiscal_year(ts timestamp, fm integer)
returns integer
language java
specific FYYEAR_TS
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalYearUdf.execute';

-- define leftN functions
create or replace function applib.leftn(str varchar(65535), len integer)
returns varchar(65535)
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.LeftNUdf.execute';

-- define rand functions
create or replace function applib.rand(minVal integer, maxVal integer)
returns integer
language java
not deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.numeric.RandUdf.execute';

-- define repeater function
create or replace function applib.REPEATER(str varchar(65535), times integer)
returns varchar(65535)
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.RepeaterUdf.execute';

-- define rightn function
create or replace function applib.RIGHTN(str varchar(65535), len integer)
returns varchar(65535)
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.RightNUdf.execute';

-- define StrReplace function
create or replace function applib.str_replace(inStr varchar(65535), oldStr varchar(65535), newStr varchar(65535))
returns varchar(65535)
language java
deterministic
no sql
external name "applib.applibJar:org.eigenbase.applib.string.StrReplaceUdf.execute";

-- define convert_date function
create or replace function applib.convert_date(str varchar(65535), mask varchar(65535), rej boolean)
returns date
language java
specific convert_date_rejectable
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.ConvertDateUdf.execute';

create or replace function applib.convert_date(str varchar(65535), mask varchar(65535))
returns date
language java
specific convert_date_not_rejectable
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.ConvertDateUdf.execute';

-- converts a string to a date, according to the specified format string
create or replace function applib.char_to_date(format varchar(65535), dateString varchar(65535))
returns date
language java
specific applib_std_char_to_date
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.StdConvertDateUdf.char_to_date';

create or replace function applib.char_to_time(format varchar(65535), timeString varchar(65535))
returns time
language java
specific applib_std_char_to_time
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.StdConvertDateUdf.char_to_time';

create or replace function applib.char_to_timestamp(
    format varchar(65535), timestampString varchar(65535))
returns timestamp
language java
specific applib_std_char_to_timestamp
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.StdConvertDateUdf.char_to_timestamp';

-- formats a string as a date, according to the specified format string
create or replace function applib.date_to_char(format varchar(65535), d date)
returns varchar(65535)
language java
specific applib_std_date_to_char
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.StdConvertDateUdf.date_to_char';

create or replace function applib.time_to_char(format varchar(65535), t time)
returns varchar(65535)
language java
specific applib_std_time_to_char
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.StdConvertDateUdf.time_to_char';

create or replace function applib.timestamp_to_char(format varchar(65535), ts timestamp)
returns varchar(65535)
language java
specific applib_std_timestamp_to_char
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.StdConvertDateUdf.timestamp_to_char';

-- adds n number of days to a date
create or replace function applib.add_days(d date, n int)
returns date
specific add_days_date
deterministic
contains sql
return (
  d + cast(cast(n as bigint) as interval day(10))
);

-- adds n number of days to a timestamp
create or replace function applib.add_days(ts timestamp, n int)
returns timestamp
specific add_days_timestamp
deterministic
contains sql
return (
  ts + cast(cast(n as bigint) as interval day(10))
);

-- adds n number of hours to a timestamp
create or replace function applib.add_hours(ts timestamp, n int)
returns timestamp
specific add_hours_timestamp
deterministic
contains sql
return (
  ts + cast(cast(n as bigint) as interval hour(10)));

-- returns the difference in units of days between two dates
create or replace function applib.days_diff(d1 date, d2 date)
returns bigint
specific days_diff_dates
deterministic
contains sql
return (
  extract( day from ((d1 - d2) day) )
);

-- returns the difference in units of days between two timestamps. Note that
-- partial days that are less than 24 hours will not count as a day.
create or replace function applib.days_diff(ts1 timestamp, ts2 timestamp)
returns bigint
specific days_diff_timestamps
deterministic
contains sql
return (
  extract( day from ((ts1 - ts2) day) )
);

-- returns the difference in units of hours between two timestamps. Note that 
-- partial hours that are less than 60 minutes will not count as an hour.
create or replace function applib.hours_diff(ts1 timestamp, ts2 timestamp)
returns bigint
specific hours_diff_timestamps
deterministic
contains sql
return (
  extract( day from ((ts1 - ts2) day)) * 24  +
  extract( hour from ((ts1 - ts2) hour))
);

-- define dayFromJulianStart
-- 2440588 is the number of days from the Julian Calendar start date to 
-- the epoch Jan 1, 1970 
create or replace function applib.day_from_julian_start(dt Date)
returns integer
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.ConvertJulianDayUdf.dateToJulianDay';

-- define current_date_in_julian
-- 2440588 is the number of days from the Julian Calendar start date to 
-- the epoch Jan 1, 1970 
create or replace function applib.current_date_in_julian()
returns integer
deterministic
dynamic_function
contains sql
return (
  applib.day_from_julian_start(CURRENT_DATE)
);

-- define julian_day_to_date
create or replace function applib.julian_day_to_date(jd integer)
returns date
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.ConvertJulianDayUdf.julianDayToDate';

-- define julian_day_to_timestamp
create or replace function applib.julian_day_to_timestamp(jd integer)
returns timestamp
language java
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.datetime.ConvertJulianDayUdf.julianDayToTimestamp';

-- define padweeknumber
create or replace function applib.padweeknumber(wk_num_as_yr integer)
returns varchar(16)
language sql
deterministic
contains sql
return (
  case 
    WHEN (wk_num_as_yr < 10) THEN ('0' || cast (wk_num_as_yr as varchar(1)))
    ELSE cast (wk_num_as_yr as varchar(2))  
  end
);

-- define INSTR
create or replace function applib.instr(str varchar(65535), subStr varchar(65535), startPos int, nthAppearance int)
returns int
language java
specific instr_with_optional_vars
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.InStrUdf.execute';

create or replace function applib.instr(str varchar(65535), subStr varchar(65535))
returns int
language java
specific instr_without_optional_vars
deterministic
no sql
returns null on null input
external name 'applib.applibJar:org.eigenbase.applib.string.InStrUdf.execute';


----
-- Application variables (a.k.a. "appvars")
-- 
-- Appvars are defined within a named context, allowing sets of
-- variables to be manipulated as a unit.
----

-- Creates a variable (or a context if var_id is null).
-- The initial value for a new variable is null.  It is an error
-- to attempt to create a context or variable which already exists.
create or replace procedure applib.create_var(
    context_id varchar(255), 
    var_id varchar(255),
    description varchar(65535))
language java
no sql
external name 'applib.applibJar:org.eigenbase.applib.variable.AppVarApi.executeCreate';

-- Deletes a variable (or a context if var_id is null), which must
-- currently exist.
create or replace procedure applib.delete_var(
    context_id varchar(255), 
    var_id varchar(255))
language java
no sql
external name 'applib.applibJar:org.eigenbase.applib.variable.AppVarApi.executeDelete';

-- Sets the value for a variable.  var_id must not be null, and must
-- reference a previously created variable.
create or replace procedure applib.set_var(
    context_id varchar(255), 
    var_id varchar(255), 
    new_value varchar(65535))
language java
no sql
external name 'applib.applibJar:org.eigenbase.applib.variable.AppVarApi.executeSet';

-- Flushes modifications to a variable (or a context if var_id is null).
-- Before flush, there is no guarantee that modifications have
-- been made permanent.
create or replace procedure applib.flush_var(
    context_id varchar(255), 
    var_id varchar(255))
language java
no sql
external name 'applib.applibJar:org.eigenbase.applib.variable.AppVarApi.executeFlush';

-- Retrieves the current value of a variable.  var_id must not be null,
-- and must reference a previously created variable.
-- Declared as deterministic to let the optimizer know that it should
-- be evaluated once per statement (rather than once per row) when
-- the arguments are constant literals.
create or replace function applib.get_var(
    context_id varchar(255), 
    var_id varchar(255)) 
returns varchar(65535)
language java
deterministic
dynamic_function
no sql
external name 'applib.applibJar:org.eigenbase.applib.variable.AppVarApi.executeGet';

----
-- UDXs
----

-- define TIMEDIMENSION
create or replace function APPLIB.TIME_DIMENSION(startYr int, startMth int, startDay int, endYr int, endMth int, endDay int, fiscalYrStartMth int)
returns table(
  TIME_KEY_SEQ int,
  TIME_KEY date,
  DAY_OF_WEEK varchar(10),
  WEEKEND varchar(1),
  DAY_NUMBER_IN_WEEK int,
  DAY_NUMBER_IN_MONTH int,
  DAY_NUMBER_IN_YEAR int,
  DAY_NUMBER_OVERALL int,
  DAY_FROM_JULIAN int,
  WEEK_NUMBER_IN_MONTH int,
  WEEK_NUMBER_IN_QUARTER int,
  WEEK_NUMBER_IN_YEAR int,
  WEEK_NUMBER_OVERALL int,
  MONTH_NAME varchar(10),
  MONTH_NUMBER_IN_QUARTER int,
  MONTH_NUMBER_IN_YEAR int,
  MONTH_NUMBER_OVERALL int,
  QUARTER int,
  YR int,
  CALENDAR_QUARTER varchar(6),
  WEEK_START_DATE date,
  WEEK_END_DATE date,
  MONTH_START_DATE date,
  MONTH_END_DATE date,
  QUARTER_START_DATE date,
  QUARTER_END_DATE date,
  YEAR_START_DATE date,
  YEAR_END_DATE date,
  FISCAL_WEEK_START_DATE date,
  FISCAL_WEEK_END_DATE date,
  FISCAL_WEEK_NUMBER_IN_MONTH int,
  FISCAL_WEEK_NUMBER_IN_QUARTER int,
  FISCAL_WEEK_NUMBER_IN_YEAR int,
  FISCAL_MONTH_START_DATE date,
  FISCAL_MONTH_END_DATE date,
  FISCAL_MONTH_NUMBER_IN_QUARTER int,
  FISCAL_MONTH_NUMBER_IN_YEAR int,
  FISCAL_QUARTER_START_DATE date,
  FISCAL_QUARTER_END_DATE date,
  FISCAL_QUARTER_NUMBER_IN_YEAR int,
  FISCAL_YEAR_START_DATE date,
  FISCAL_YEAR_END_DATE date)
language java
parameter style system defined java
specific time_dimension_fm
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.TimeDimensionUdx.execute';

-- time dimension without fiscal information
create or replace function APPLIB.TIME_DIMENSION(startYr int, startMth int, startDay int, endYr int, endMth int, endDay int)
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
specific time_dimension
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.TimeDimensionUdx.execute';

-- Fiscal time dimension 
create or replace function APPLIB.FISCAL_TIME_DIMENSION(startYr int, startMth int, startDay int, endYr int, endMth int, endDay int, fiscalYrStartMth int)
returns table(
  TIME_KEY_SEQ int,
  TIME_KEY date,
  DAY_OF_WEEK varchar(10),
  WEEKEND varchar(1),
  DAY_NUMBER_IN_WEEK int,
  DAY_NUMBER_IN_MONTH int,
  DAY_NUMBER_IN_QUARTER int,
  DAY_NUMBER_IN_YEAR int,
  DAY_NUMBER_OVERALL int,
  DAY_FROM_JULIAN int,
  WEEK_NUMBER_IN_MONTH int,
  WEEK_NUMBER_IN_QUARTER int,
  WEEK_NUMBER_IN_YEAR int,
  WEEK_NUMBER_OVERALL int,
  MONTH_NAME varchar(10),
  MONTH_NUMBER_IN_QUARTER int,
  MONTH_NUMBER_IN_YEAR int,
  MONTH_NUMBER_OVERALL int,
  QUARTER int,
  YR int,
  CALENDAR_QUARTER varchar(6),
  WEEK_START_DATE date,
  WEEK_END_DATE date,
  MONTH_START_DATE date,
  MONTH_END_DATE date,
  QUARTER_START_DATE date,
  QUARTER_END_DATE date,
  YEAR_START_DATE date,
  YEAR_END_DATE date,
  FISCAL_YEAR int,
  FISCAL_DAY_NUMBER_IN_QUARTER int,
  FISCAL_DAY_NUMBER_IN_YEAR int,
  FISCAL_WEEK_START_DATE date,
  FISCAL_WEEK_END_DATE date,
  FISCAL_WEEK_NUMBER_IN_MONTH int,
  FISCAL_WEEK_NUMBER_IN_QUARTER int,
  FISCAL_WEEK_NUMBER_IN_YEAR int,
  FISCAL_MONTH_START_DATE date,
  FISCAL_MONTH_END_DATE date,
  FISCAL_MONTH_NUMBER_IN_QUARTER int,
  FISCAL_MONTH_NUMBER_IN_YEAR int,
  FISCAL_QUARTER_START_DATE date,
  FISCAL_QUARTER_END_DATE date,
  FISCAL_QUARTER_NUMBER_IN_YEAR int,
  FISCAL_YEAR_START_DATE date,
  FISCAL_YEAR_END_DATE date)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.FiscalTimeDimensionUdx.execute';

-- Calculate Effective To Timestamps
create or replace function applib.derive_effective_to_timestamp(
        c cursor, 
        units_to_subtract integer, 
        unit_type_to_subtract varchar(32))
returns table(
        id varchar(255), 
        effective_from_timestamp timestamp, 
        effective_to_timestamp timestamp)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.datetime.DeriveEffectiveToTimestampUdx.execute';

-- Flatten hierarchical data, producing rows corresponding to leaf nodes only
create or replace function applib.flatten_recursive_hierarchy(c cursor)
returns table(
    vertices integer,
    multipath boolean,
    level1 varchar(65535),
    level2 varchar(65535),
    level3 varchar(65535),
    level4 varchar(65535),
    level5 varchar(65535),
    level6 varchar(65535),
    level7 varchar(65535),
    level8 varchar(65535),
    level9 varchar(65535),
    level10 varchar(65535),
    level11 varchar(65535),
    level12 varchar(65535),
    level13 varchar(65535),
    level14 varchar(65535),
    level15 varchar(65535))
language java
parameter style system defined java
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.FlattenRecursiveHierarchyUdx.execute';

-- Flatten hierarchical data, producing rows for both leaf and non-leaf nodes
create or replace function applib.flatten_recursive_hierarchy_all_levels(
    c cursor)
returns table(
    vertices integer,
    multipath boolean,
    non_leaf boolean,
    level1 varchar(65535),
    level2 varchar(65535),
    level3 varchar(65535),
    level4 varchar(65535),
    level5 varchar(65535),
    level6 varchar(65535),
    level7 varchar(65535),
    level8 varchar(65535),
    level9 varchar(65535),
    level10 varchar(65535),
    level11 varchar(65535),
    level12 varchar(65535),
    level13 varchar(65535),
    level14 varchar(65535),
    level15 varchar(65535))
language java
parameter style system defined java
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.FlattenRecursiveHierarchyUdx.executeAllLevels';

-- Generate CRC udx
create or replace function generate_crc(c cursor)
returns table(c.*, crc_value bigint)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.GenerateCrcUdx.execute';

-- Generate CRC UDX with specified column set used to calculate CRC
create or replace function generate_crc(
  c cursor,
  r select from c,
  exclude boolean)
returns table(c.*, crc_value bigint)
language java
parameter style system defined java
deterministic
no sql
specific generate_crc_for_column_subset
external name 'applib.applibJar:org.eigenbase.applib.cursor.GenerateCrcUdx.execute';

-- Generate sequence
create or replace function generate_sequence(
  input_table cursor,
  start_number bigint,
  increment bigint)
returns table(
  input_table.*, 
  seq_num bigint)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.GenerateSequenceUdx.execute(java.sql.ResultSet, java.lang.Long, java.lang.Long, java.sql.PreparedStatement)';

create or replace function generate_sequence_partitioned(
  input_table cursor,
  partitioning_columns select from input_table,
  start_number bigint,
  increment bigint)
returns table(
  input_table.*, 
  seq_num bigint)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.GenerateSequenceUdx.execute(java.sql.ResultSet, java.util.List, java.lang.Long, java.lang.Long, java.sql.PreparedStatement)';

-- Show candidate indexes for possible creation

create or replace function applib.show_idx_candidates(
  schema_name varchar(128),
  table_name varchar(128),
  threshold integer)
returns table(
  catalog_name varchar(128),
  schema_name varchar(128),
  table_name varchar(128),
  column_name varchar(128))
language java
specific show_idx_candidates
parameter style system defined java
deterministic
modifies sql data
external name 'applib.applibJar:org.eigenbase.applib.util.ShowIndexCandidatesUdx.execute';

-- other catalogs version

create or replace function applib.show_idx_candidates(
  catalog_name varchar(128),
  schema_name varchar(128),
  table_name varchar(128),
  threshold integer)
returns table(
  catalog_name varchar(128),
  schema_name varchar(128),
  table_name varchar(128),
  column_name varchar(128))
language java
specific show_idx_candidates2
parameter style system defined java
deterministic
modifies sql data
external name 'applib.applibJar:org.eigenbase.applib.util.ShowIndexCandidatesUdx.execute';

-- Create indexes
create or replace procedure applib.create_indexes(
  index_table_sql varchar(65535))
language java
parameter style java
modifies sql data
external name 'applib.applibJar:org.eigenbase.applib.util.CreateIndexesUdp.execute';

-- Pivot columns to rows
create or replace function pivot_columns_to_rows(c cursor)
returns table(col_name varchar(65535), col_value varchar(65535))
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.PivotColumnsToRowsUdx.execute';

-- retain top most rows
create or replace function applib.topN(IN_CURSOR cursor, N int)
returns table(IN_CURSOR.*)
language java
parameter style system defined java
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.TopNUdx.execute';

-- collapse rows
create or replace function collapse_rows(c cursor, delimiter varchar(1))
returns table(
  parent_value varchar(65535), 
  concatenated_child_values varchar(65535),
  collapsed_row_count integer)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.CollapseRowsUdx.execute';

-- split strings
create or replace function applib.split_string_to_rows(
       IN_STRING varchar(65535), 
       SEPARATOR_CHAR char(1), 
       ESCAPE_CHAR char(1),
       TRIM_TOKENS boolean)
returns table(OUT_STRINGS varchar(65535))
language java
specific split_string_to_rows
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.string.SplitStringUdx.execute';

create or replace function applib.split_rows(
       IN_CURSOR cursor, 
       SEPARATOR_CHAR char(1), 
       ESCAPE_CHAR char(1), 
       TRIM_TOKENS boolean)
returns table(IN_CURSOR.*)
language java
specific split_strings_singlecol
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.string.SplitStringUdx.execute';

create or replace function applib.split_rows(
       IN_CURSOR cursor, 
       COL_NAME select from IN_CURSOR, 
       SEPARATOR_CHAR char(1), 
       ESCAPE_CHAR char(1), 
       TRIM_TOKENS boolean)
returns table(IN_CURSOR.*)
language java
specific split_strings_multicol
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.string.SplitStringUdx.execute';

-- split strings with sequence number
create or replace function applib.split_string_to_rows(
       IN_STRING varchar(65535), 
       SEPARATOR_CHAR char(1), 
       ESCAPE_CHAR char(1),
       TRIM_TOKENS boolean,
       START_NUMBER bigint,
       INCREMENT bigint)
returns table(OUT_STRINGS varchar(65535), SEQ_NUM bigint)
language java
specific split_string_to_rows_sequence
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.string.SplitStringUdx.splitSingleStringWithSequence(java.lang.String,java.lang.String,java.lang.String,boolean,java.lang.Long,java.lang.Long,java.sql.PreparedStatement)';

create or replace function applib.split_rows(
       IN_CURSOR cursor, 
       SEPARATOR_CHAR char(1), 
       ESCAPE_CHAR char(1), 
       TRIM_TOKENS boolean,
       START_NUMBER bigint,
       INCREMENT bigint)
returns table(IN_CURSOR.*, SEQ_NUM bigint)
language java
specific split_strings_singlecol_sequence
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.string.SplitStringUdx.splitSingleColumnWithSequence(java.sql.ResultSet,java.lang.String,java.lang.String,boolean,java.lang.Long,java.lang.Long,java.sql.PreparedStatement)';

create or replace function applib.split_rows(
       IN_CURSOR cursor, 
       COL_NAME select from IN_CURSOR, 
       SEPARATOR_CHAR char(1), 
       ESCAPE_CHAR char(1), 
       TRIM_TOKENS boolean,
       START_NUMBER bigint,
       INCREMENT bigint)
returns table(IN_CURSOR.*, SEQ_NUM bigint)
language java
specific split_strings_multicol_sequence
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.string.SplitStringUdx.splitMultiColumnWithSequence(java.sql.ResultSet,java.util.List,java.lang.String,java.lang.String,boolean,java.lang.Long,java.lang.Long,java.sql.PreparedStatement)';

-- enforce row constraints, default message catalog
create or replace function enforce_row_constraints(c cursor, r select from c)
returns table(c.*)
language java
parameter style system defined java
not deterministic
no sql
specific enforce_row_constraints_default_msg_jar
external name 'applib.applibJar:org.eigenbase.applib.util.EnforceRowConstraintsUdx.execute';

-- enforce row constraints with msg jar
create or replace function enforce_row_constraints(
  c cursor,
  r select from c,
  msgJarName varchar(128))
returns table(c.*)
language java
parameter style system defined java
not deterministic
no sql
specific enforce_row_constraints_with_msg_jar
external name 'applib.applibJar:org.eigenbase.applib.util.EnforceRowConstraintsUdx.execute';

-- enforce row constraints with tag for logging
create or replace function enforce_row_constraints(
  c cursor,
  r select from c,
  msgJarName varchar(128),
  tag varchar(128))
returns table(c.*)
language java
parameter style system defined java
not deterministic
no sql
specific enforce_row_constraints_with_tag
external name 'applib.applibJar:org.eigenbase.applib.util.EnforceRowConstraintsUdx.execute';

create or replace function penultimate_values(
  input_table cursor,
  grouping_columns select from input_table,
  designated_value_and_timestamp select from input_table)
returns table(
  input_table.*,
  until_timestamp timestamp)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.PenultimateValuesUdx.execute';

create or replace function contiguous_value_intervals(
  input_table cursor,
  partitioning_columns select from input_table,
  timestamp_column select from input_table)
returns table(
  input_table.*,
  until_timestamp timestamp,
  previous_clump varchar(65535),
  previous_from_timestamp timestamp,
  next_clump varchar(65535),
  next_until_timestamp timestamp)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.cursor.ContiguousValueIntervalsUdx.execute';

----
-- System procedures
----

-- UDP for granting a user select privileges for all tables and views in a schema
create or replace procedure grant_select_for_schema(
in schemaname varchar(255), 
in username varchar(255))
language java
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.security.GrantSelectForSchemaUdp.execute';

-- UDP for executing a sql statement for each table and view in an entire schema
create or replace procedure do_for_entire_schema(
in sqlString varchar(65535),
in schemaName varchar(255),
in objTypeStr varchar(128))
language java
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.util.DoForEntireSchemaUdp.execute';

-- UDP for computing statistics for all tables in a schema
create or replace procedure compute_statistics_for_schema(
in schemaName varchar(255))
language java
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.analysis.ComputeStatisticsForSchemaUdp.execute';

-- UDP for estimating statistics for all tables in a schema
create or replace procedure estimate_statistics_for_schema(
in schemaName varchar(255))
language java
specific estimate_statistics_for_schema_no_samplingrate
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.analysis.EstimateStatisticsForSchemaUdp.execute';

-- UDP for estimating statistics for all tables in a schema with a fixed sampling rate
create or replace procedure estimate_statistics_for_schema(
in schemaName varchar(255),
in samplingRate float)
language java
specific estimate_statistics_for_schema_float_samplingrate
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.analysis.EstimateStatisticsForSchemaUdp.execute(java.lang.String, java.lang.Double)';

-- UDP for dropping a schema if it exists
create or replace procedure drop_schema_if_exists(
in schemaname varchar(255),
in restrict_or_cascade varchar(255))
language java
parameter style java
reads sql data
external name  'applib.applibJar:org.eigenbase.applib.util.DropSchemaIfExistsUdp.execute';

-- UDP for replicating a Mondrian schema
create or replace procedure applib.replicate_mondrian(
    mondrian_schema_filename varchar(65535), 
    foreign_server_name varchar(128), 
    foreign_schema_name varchar(128), 
    local_schema_name varchar(128),
    script_file_name varchar(65535),
    copy_data boolean)
language java
modifies sql data
external name 'applib.applibJar:org.eigenbase.applib.mondrian.ReplicateMondrianUdp.execute';

-- UDP for clearing Mondrian Schema Cache on Pentaho Server
create or replace procedure applib.clear_pentaho_mondrian_cache(
    base_server_url varchar(65535), 
    username varchar(128), 
    password varchar(128))
language java
no sql
external name 'applib.applibJar:org.eigenbase.applib.mondrian.ClearPentahoMondrianCacheUdp.execute';

-- UDP for conditionally executing a SQL statement based on input set 
create or replace procedure exec_sql_if_no_rows( 
in evalSQL varchar(65535),
in execSQL varchar(65535))
language java 
parameter style java 
reads sql data 
external name  'applib.applibJar:org.eigenbase.applib.util.ExecSqlIfNoRows.execute'; 

create or replace function APPLIB.WRITE_ROWS_TO_FILE(
IN_CURSOR cursor, 
URL varchar(65535), 
IS_COMPRESSED boolean)
returns table(status int, message varchar(6000))
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.impexp.WriteRowsToFileUDX.execute';
 
create or replace function APPLIB.READ_ROWS_FROM_FILE(
IN_CURSOR cursor, 
URL varchar(65535), 
IS_COMPRESSED boolean)
returns table (IN_CURSOR.*)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.impexp.ReadRowsFromFileUDX.execute';

create or replace function APPLIB.REMOTE_ROWS(IN_CURSOR cursor, PORT int, IS_COMPRESSED boolean)
returns table (IN_CURSOR.*)
language java
parameter style system defined java
deterministic
no sql
external name 'applib.applibJar:org.eigenbase.applib.impexp.RemoteRowsUDX.execute';

create or replace procedure create_table_from_source_table(
in sourceTable varchar(1024),
in schemaName varchar(128),
in tableName varchar(128),
in additonalColsInfo varchar(65535))
language java
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.util.CreateTbFromSrcTbUdp.execute';

create or replace procedure create_table_as(
in schemaName varchar(128),
in tableName varchar(128),
in selectStmt varchar(65535),
in shouldLoad boolean)
language java
parameter style java
reads sql data
external name 'applib.applibJar:org.eigenbase.applib.util.CreateTbFromSelectStmtUdp.execute';
