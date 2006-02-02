-- $Id$
-- Tests for strReplace UDF
set schema 'udftest';
set path 'udftest';

-- define function
create function str_replace(inStr varchar(128), oldStr varchar(128), newStr varchar(128))
returns varchar(128)
language java
no sql
external name "class com.lucidera.luciddb.applib.strReplace.FunctionExecute";

values (str_replace('This is my test string let''s try this test out! yay!', '''', 'apostrophe'));
values (str_replace('This is my test string let''s try this test out! yay!', 'test', 'simple test'));
values str_replace('3204jwsd213980djsakl##@@@ djsflkds#@', '#', 'Hohoho!');
values str_replace('This shouldn''t change', 'f', 'yyyy');

-- these should fail
values str_replace(3434, 3, 5);

-- create view with reference to str_replace
create view changedph (fname, phone, chphone)as
select fname, phone, str_replace(phone, '234', '###') 
from customers;

select * from changedph
order by 1;

create view fixph (fname, chphone, fphone) as
select fname, chphone, str_replace(chphone, '###', '999')
from changedph;

select * from fixph
order by 1;

-- in expressions
select fname, chphone || fphone 
from fixph
order by 1;

-- nested
select fname, str_replace( str_replace(phone, '234', '999'), '999', 'abcde')
from customers
order by 1;