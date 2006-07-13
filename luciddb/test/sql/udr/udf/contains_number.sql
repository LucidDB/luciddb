-- $Id$
-- Test queries for containsNumber UDF
set schema 'udftest';
set path 'udftest';

-- basic tests
values applib.contains_number('a');
values applib.contains_number('Ahsdkj6sadsal');
values applib.contains_number('gsdfglksjf^%$^$%dslkfjskfjw~!@$EWFDZVcxvkjdsifio#@%^$_
+@fdjgklfdirue');

-- create view with reference to applib.contains_number
create view has_number(fname, fcol, phcol) as
select fname, applib.contains_number(fname), applib.contains_number(phone) 
from customers;

select * from has_number 
order by 1;

-- in expressions
select * from customers
where applib.contains_number(fname)
order by 1;

select * from customers
where applib.contains_number(fname) 
and sex = 'F'
order by 1;

-- cleanup 
drop view has_number;

