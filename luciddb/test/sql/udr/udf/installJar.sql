-- $ID: //open/lu/dev/luciddb/test/sql/udr/udf/installJar.sql#1 $
create schema applib;
set schema 'applib';
set path 'applib';

call sqlj.install_jar('file:../../../../plugin/applib.jar','applibJar', 0);

-- define char_replace functions
create function char_replace(str varchar(128), oldC varchar(128), newC varchar(128)) 
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CharReplace.FunctionExecute';

create function char_replace(str varchar(128), oldC integer, newC integer) 
returns varchar(128)
language java
specific char_replace_int
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CharReplace.FunctionExecute';

-- define clean_phone_international function
create function clean_phone_international(str varchar(128), b boolean)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhoneInternational.FunctionExecute';

-- define CleanPhone functions
create function clean_phone(str varchar(128))
returns varchar(128)
language java
specific clean_phone_no_format
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

create function clean_phone(inStr varchar(128), format integer)
returns varchar(128)
language java
specific clean_phone_int_format
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

create function clean_phone(inStr varchar(128), format integer, reject boolean)
returns varchar(128)
language java
specific clean_phone_int_format_rejectable
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

create function clean_phone(inStr varchar(128), format varchar(128), reject boolean)
returns varchar(128)
language java
specific clean_phone_str_format_rejectable
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';


-- define contains_number function
create function contains_number(str varchar(128))
returns boolean
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.containsNumber.FunctionExecute';

-- define cy_quarter functions
create function cy_quarter(dt date)
returns varchar(128)
language java
specific cy_quarter_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

create function cy_quarter(ts timestamp)
returns varchar(128)
language java
specific cy_quarter_ts
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toCYQuarter.FunctionExecute';

-- define date_internal function
create function date_internal(indate bigint)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DateBBInternal.FunctionExecute';

-- define day_in_year function
create function day_in_year(dt date)
returns integer
language java
specific day_in_year_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

create function day_in_year(ts timestamp)
returns integer
language java
specific day_in_year_timestamp
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

create function day_in_year(yr integer, mth integer, dt integer)
returns integer
language java
specific day_in_year_ymd
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.DayInYear.FunctionExecute';

-- define day_number_overall functions
create function day_number_overall(dt Date)
returns integer
language java
specific day_number_overall_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDayNumberOverall.FunctionExecute';

create function day_number_overall(ts timestamp)
returns integer
language java
specific day_number_overall_ts
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDayNumberOverall.FunctionExecute';

-- define FYMonth functions
create function fymonth(dt date, fm integer)
returns integer
language java
specific fymonth_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

create function fymonth(ts timestamp, fm integer)
returns integer
language java
specific fymonth_ts
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

-- define fy_quarter functions
create function fy_quarter(yr integer, mth integer, fm integer)
returns varchar(10)
language java
specific fy_quarter_ymfm
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function fy_quarter(dt date, fm integer)
returns varchar(10)
language java
specific fy_quarter_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

create function fy_quarter(ts timestamp, fm integer)
returns varchar(10)
language java
specific fy_quarter_timestamp
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYQuarter.FunctionExecute';

-- define fy_year functions
create function fy_year(dt date, fm integer)
returns integer
language java
specific fy_year_date
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYYear.FunctionExecute';

create function fy_year(ts timestamp, fm integer)
returns integer
language java
specific fy_year_timestamp
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.FYYear.FunctionExecute';

-- define leftN functions
create function leftN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.leftN.FunctionExecute';

-- define rand functions
create function rand(minVal integer, maxVal integer)
returns integer
language java
not deterministic
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.rand.FunctionExecute';

-- define repeater function
create function repeater(str varchar(128), times integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.repeater.FunctionExecute';

-- define rightn function
create function rightN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.rightN.FunctionExecute';

-- define str_replace function
create function str_replace(inStr varchar(128), oldStr varchar(128), newStr varchar(128))
returns varchar(128)
language java
no sql
external name "applib.applibJar:com.lucidera.luciddb.applib.strReplace.FunctionExecute";

-- define to_date function
create function to_date(str varchar(128), mask varchar(50), rej boolean)
returns date
language java
specific to_date_choice
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDate.FunctionExecute';

create function to_date(str varchar(128), mask varchar(50))
returns date
language java
specific to_date_no_choice
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.toDate.FunctionExecute';

