-- $Id$
-- Tests for strReplace UDF
set schema 'udftest';
set path 'udftest';


values (applib.str_replace('This is my test string let''s try this test out! yay!', '''', 'apostrophe'));
values (applib.str_replace('This is my test string let''s try this test out! yay!', 'test', 'simple test'));
values applib.str_replace('3204jwsd213980djsakl##@@@ djsflkds#@', '#', 'Hohoho!');
values applib.str_replace('This shouldn''t change', 'f', 'yyyy');
values applib.str_replace('0013000000BvwzRAAR~TEST1~TEST2~TEST3~TEST5~TEST6~TEST7~TEST8~TEST10~TEST11~TEST14~TEST1_PERF~TEST1_PERF1~TEST1_PERF2~TEST1_PARTNER~','PARTNER~','12345678');

-- these should fail
values applib.str_replace(3434, 3, 5);

-- null input
values applib.str_replace(cast(null as varchar(22)), 'null', 'new');
values applib.str_replace(
  'This is not a null value', cast(null as varchar(20)), 'nonono');
values applib.str_replace(
  'This is not a null value', 'is', cast(null as varchar(10)));

-- create view with reference to applib.str_replace
create view changedph (fname, phone, chphone)as
select fname, phone, applib.str_replace(phone, '234', '###') 
from customers;

select * from changedph
order by 1;

create view fixph (fname, chphone, fphone) as
select fname, chphone, applib.str_replace(chphone, '###', '999')
from changedph;

select * from fixph
order by 1;

-- in expressions
select fname, chphone || fphone 
from fixph
order by 1;

-- nested
select fname, applib.str_replace( applib.str_replace(phone, '234', '999'), '999', 'abcde')
from customers
order by 1;

-- cleanup
drop view fixph;
drop view changedph;
