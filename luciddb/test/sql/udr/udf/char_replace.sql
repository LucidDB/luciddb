-- Test queries for charReplace UDF
set schema 'udftest';
set path 'udftest';

-- define CharReplace functions

create function char_replace(str varchar(128), oldC varchar(128), newC varchar(128)) 
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.test.udr.CharReplace.FunctionExecute';

create function char_replace(str varchar(128), oldC integer, newC integer) 
returns varchar(128)
language java
specific char_replace_int
no sql
external name 'class com.lucidera.luciddb.test.udr.CharReplace.FunctionExecute';
-- create views with reference to char_replace
create view new_names(first, last) as
select char_replace(fname, 'g', 'M'), char_replace(lname, 'a', 'e') 
from customers;

select * from new_names
order by 1;

create view new_ages(first, last, newage) as
select fname, lname, char_replace( cast(age as varchar(10)), 49, 51)
from customers;

select * from new_ages
order by 1;

-- char_replace in expressions

select fname || char_replace(fname, 'r', 'K'), lname || char_replace(lname, 'y', 'Y')
from customers
order by 1;

select first || char_replace(first, 'r', 'K'), cast (newage as integer) + cast (char_replace(newage, 51, 48) as integer)
from new_ages
order by 1;

-- nested char_replace
values char_replace( char_replace('Moma is a great place to be', 'e', 'E'), 'a', 'A');

