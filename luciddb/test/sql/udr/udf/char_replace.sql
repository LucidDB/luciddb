-- $Id$
-- Test queries for charReplace UDF
set schema 'udftest';
set path 'udftest';


values applib.char_replace('AA AA AA AA', 'A', 'B');
values applib.char_replace('bb bb bb bb', 'b', 'A');
values applib.char_replace('111111', 49, 51);
values applib.char_replace('//////', 47, 42);
values applib.char_replace('******', 42, 49);

-- failures
values applib.char_replace('bbbbbbbb', 'b', 'AA');


-- create views with reference to applib.char_replace
create view new_names(first, last) as
select applib.char_replace(fname, 'g', 'M'), applib.char_replace(lname, 'a', 'e') 
from customers;

select * from new_names
order by 1;

create view new_ages(first, last, newage) as
select fname, lname, applib.char_replace( cast(age as varchar(10)), 49, 51)
from customers;

select * from new_ages
order by 1;

-- applib.char_replace in expressions

select fname || applib.char_replace(fname, 'r', 'K'), lname || applib.char_replace(lname, 'y', 'Y')
from customers
order by 1;

select first || applib.char_replace(first, 'r', 'K'), cast (newage as integer) + cast (applib.char_replace(newage, 51, 48) as integer)
from new_ages
order by 1;

-- nested applib.char_replace
values applib.char_replace( applib.char_replace('Moma is a great place to be', 'e', 'E'), 'a', 'A');

-- cleanup
drop view new_names;
drop view new_ages;