-- $Id$
-- Test DDL for user-defined functions

create schema udftest;
create schema udftest2;

set schema udftest;

create function celsius_to_fahrenheit(in c double)
returns double
contains sql
return c*1.8 + 32;

create function add_integers(in i int,in j int)
returns int
contains sql
return i + j;

-- this should fail since add_integers is not a procedure
drop procedure add_integers;

-- but DROP ROUTINE should work
drop routine add_integers;

-- test overloading

-- should fail
create function celsius_to_fahrenheit(in c real)
returns real
contains sql
return c*1.8 + 32;

-- should succeed:  specific name avoids conflict
create function celsius_to_fahrenheit(in c real)
returns real
specific celsius_to_fahrenheit_real
contains sql
return c*1.8 + 32;

-- should fail:  schema for specific name must match schema for invocation name
create function add_integers(in i int,in j int)
returns int
specific udftest2.add_ints
contains sql
return i + j;

-- drop by specific name
drop function celsius_to_fahrenheit;
drop function celsius_to_fahrenheit_real;

-- should fail:  can't declare OUT parameter to function
create function add_integers(out i int,in j int)
returns int
contains sql
return i + j;

-- should fail:  can't declare INOUT parameter to function
create function add_integers(inout i int,in j int)
returns int
contains sql
return i + j;

-- test various modifiers

-- should fail:  NO SQL can't be specified for SQL-defined routines
create function add_integers(in i int,in j int)
returns int
no sql
return i + j;

-- a bit of a fib, but should pass
create function add_integers(in i int,in j int)
returns int
reads sql data
return i + j;

drop function add_integers;

-- a bit of a fib, but should pass
create function add_integers(in i int,in j int)
returns int
modifies sql data
return i + j;

drop function add_integers;

create function add_integers(in i int,in j int)
returns int
contains sql
deterministic
return i + j;

drop function add_integers;

create function add_integers(in i int,in j int)
returns int
contains sql
not deterministic
return i + j;

drop function add_integers;

create function add_integers(in i int,in j int)
returns int
contains sql
returns null on null input
return i + j;

drop function add_integers;

create function add_integers(in i int,in j int)
returns int
contains sql
called on null input
return i + j;

drop function add_integers;

create schema udftest3;

-- test dependencies and cascade/restrict

create function to_upper(in v varchar(128))
returns varchar(128)
contains sql
return upper(v);

create view upper_crust as
select to_upper(name)
from sales.depts;

create function to_upper2(in v varchar(128))
returns varchar(128)
contains sql
return upper(v);

create function to_uppertrim(in v varchar(128))
returns varchar(128)
contains sql
return trim(trailing ' ' from to_upper2(v));

-- should fail:  restrict
drop function to_upper;

drop function to_upper cascade;

-- should fail:  restrict
drop function to_upper2;

drop function to_uppertrim;

-- should succeed now that to_uppertrim is gone
drop function to_upper2;
