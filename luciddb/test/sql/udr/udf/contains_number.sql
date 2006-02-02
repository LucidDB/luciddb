-- $Id$
-- Test queries for containsNumber UDF
set schema 'udftest';
set path 'udftest';

-- define function
create function contains_number(str varchar(128))
returns boolean
language java
no sql
external name 'class com.lucidera.luciddb.applib.containsNumber.FunctionExecute';

-- basic tests
values contains_number('a');
values contains_number('Ahsdkj6sadsal');
values contains_number('gsdfglksjf^%$^$%dslkfjskfjw~!@$EWFDZVcxvkjdsifio#@%^$_
+@fdjgklfdirue');

-- create view with reference to contains_number
create view has_number(fname, fcol, phcol) as
select fname, contains_number(fname), contains_number(phone) 
from customers;

select * from has_number 
order by 1;

-- in expressions
select * from customers
where contains_number(fname)
order by 1;

select * from customers
where contains_number(fname) 
and sex = 'F'
order by 1;

