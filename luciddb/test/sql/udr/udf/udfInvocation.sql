-- $Id$
-- Test queries with UDF invocations

set schema 'udftest';

set path 'udftest';


-- test CharReplace

create function replaceChar(str varchar(128), oldC varchar(128), newC varchar(128)) 
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CharReplace.FunctionExecute';

create function replaceCharInt(str varchar(128), oldC integer, newC integer) 
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CharReplace.FunctionExecute';

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
external name 'class com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

-- fails: cannot override function
create function calcFiscalMonth(t Timestamp, firstMo integer) 
returns integer
language java
no sql
external name 'class com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

create function calcFiscalMonthT(t Timestamp, firstMo integer) 
returns integer
language java
no sql
external name 'class com.lucidera.luciddb.applib.FYMonth.FunctionExecute';

values calcFiscalMonth(DATE '2005-10-12', 3);
values calcFiscalMonth(DATE '2006-1-12', 1);

values calcFiscalMonthT(TIMESTAMP '2006-2-12 13:00:00', 3);
values calcFiscalMonthT(TIMESTAMP '1999-3-3 00:00:00', 3);
values calcFiscalMonthT(TIMESTAMP '2006-4-12 13:00:00', 3);


-- test CleanPhone

create function cleanPhone(num varchar(128))
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

values cleanPhone('1 2 3 4 5 6 7 8 9  0');
values cleanPhone('123456789012');

create function cleanPhoneFormat(num varchar(128), format integer)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

values cleanPhoneFormat('123 456 789 012', 1);
values cleanPhoneFormat('123.456.7890', 0);
values cleanPhoneFormat('1234567890', -1);

create function cleanPhoneFormRjct(num varchar(128), format integer, reject boolean)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

values cleanPhoneFormRjct('1234567890', 1, true);
values cleanPhoneFormRjct('123456789012', 1, false);
values cleanPhoneFormRjct('123456789012', 1, true);

create function cleanPhoneFormRjct2(num varchar(128), format varchar(128), reject boolean)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CleanPhone.FunctionExecute';

values cleanPhoneFormRjct2('aBcDeFgHiJkLm', '(999) 999 999 999 9', true);
values cleanPhoneFormRjct2('1800TESTING', '9-999-999-9999', true);
values cleanPhoneFormRjct2('123-4567-890123', '(999)9999999', true);
