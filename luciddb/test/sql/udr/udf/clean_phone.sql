set schema 'udftest';
set path 'udftest';

-- define CleanPhone functions

create function clean_phone(str varchar(128))
returns varchar(128)
language java
specific clean_phone_no_format
no sql
external name 'class com.lucidera.luciddb.test.udr.CleanPhone.FunctionExecute';

create function clean_phone(inStr varchar(128), format integer)
returns varchar(128)
language java
specific clean_phone_int_format
no sql
external name 'class com.lucidera.luciddb.test.udr.CleanPhone.FunctionExecute';

create function clean_phone(inStr varchar(128), format integer, reject boolean)
returns varchar(128)
language java
specific clean_phone_int_format_rejectable
no sql
external name 'class com.lucidera.luciddb.test.udr.CleanPhone.FunctionExecute';

create function clean_phone(inStr varchar(128), format varchar(128), reject boolean)
returns varchar(128)
language java
specific clean_phone_str_format_rejectable
no sql
external name 'class com.lucidera.luciddb.test.udr.CleanPhone.FunctionExecute';

-- create view with references ro clean_phone
create view fmtphone(fname, lname, phone1, phone2, phone3, phone4) as
select fname, lname, clean_phone(phone), clean_phone(phone, 0), clean_phone(phone, 1, true), clean_phone(phone, 'ph:999=& 99 !!99999', true)
from customers;

select * from fmtphone
order by 1;

-- clean_phone in expressions

select phone1 || clean_phone(phone2), phone4 || clean_phone(phone3, 0, true)
from fmtphone
order by 1;

-- nested clean_phone
values clean_phone(clean_phone('5558459190', 1, false), 'My Phone Number:(999 999 9999)', true); 

values clean_phone(' df        ' || clean_phone('1 2 3 4 5 6 7 8 9 0', 0, true) || '          ');

-- should fail and throw exception
values clean_phone('  fd   1 2 3 4 5 6 7 8 9 0          ', 0, true);
values clean_phone('sdfjl92378sflkds', 'ph:999-999-9999', true);