-- Test queries with UDF invocations

create schema udftest;

set schema 'udftest';

set path 'udftest';


-- test CharReplace

create function replaceChar(str varchar(128), oldC varchar(128), newC varchar(128)) 
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.test.udr.CharReplace.FunctionExecute';

create function replaceCharInt(str varchar(128), oldC integer, newC integer) 
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.test.udr.CharReplace.FunctionExecute';

values replaceChar('AA AA AA AA', 'A', 'B');
values replaceChar('bb bb bb bb', 'b', 'A');
values replaceChar('bbbbbbbb', 'b', 'AA');

values replaceCharInt('111111', 49, 51);
values replaceCharInt('//////', 47, 42);
values replaceCharInt('******', 42, 49);


-- test FYMonth

create function calcFiscalMonth(d Date, firstMo integer) 
returns integer
language java
no sql
external name 'class com.lucidera.luciddb.test.udr.FYMonth.FunctionExecute';

-- fails: cannot override function using same # of params
create function calcFiscalMonth(t Timestamp, firstMo integer) 
returns integer
language java
no sql
external name 'class com.lucidera.luciddb.test.udr.FYMonth.FunctionExecute';

create function calcFiscalMonthT(t Timestamp, firstMo integer) 
returns integer
language java
no sql
external name 'class com.lucidera.luciddb.test.udr.FYMonth.FunctionExecute';

values calcFiscalMonth(DATE '2005-10-12', 3);
values calcFiscalMonth(DATE '2006-1-12', 1);

values calcFiscalMonthT(TIMESTAMP '2006-2-12 13:00:00', 3);
values calcFiscalMonthT(TIMESTAMP '1999-3-3 00:00:00', 3);
values calcFiscalMonthT(TIMESTAMP '2006-4-12 13:00:00', 3);
