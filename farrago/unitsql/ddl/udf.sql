-- $Id$
-- Test DDL for user-defined functions

create schema udftest;
create schema udftest2;

set schema 'udftest';

set path 'udftest';

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

-- test redundant language specification
create function celsius_to_fahrenheit(in c real)
returns real
language sql
contains sql
return c*1.8 + 32;
drop function celsius_to_fahrenheit;

-- test redundant parameter style specification
create function celsius_to_fahrenheit(in c real)
returns real
parameter style sql
contains sql
return c*1.8 + 32;
drop function celsius_to_fahrenheit;

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

create function add_integers(in i int,in j int)
returns int
contains sql
return i + j;

-- should fail
create schema badpath
create function double_integer(in i int)
returns int
contains sql
return add_integers(i,i);

-- should succeed
create schema goodpath
path udftest
create function double_integer(in i int)
returns int
contains sql
return add_integers(i,i);

-- begin tests for mismatched Java/SQL

-- should fail:  cannot specify both SQL body and external name
create function get_java_property(in name varchar(128))
returns varchar(128)
contains sql
external name 'class java.lang.System.getProperty'
return 'undefined';

-- should fail:  cannot specify language SQL with external name
create function get_java_property(in name varchar(128))
returns varchar(128)
language sql
contains sql
external name 'class java.lang.System.getProperty';

-- should fail:  cannot specify parameter style SQL with external name
create function get_java_property(in name varchar(128))
returns varchar(128)
parameter style sql
contains sql
external name 'class java.lang.System.getProperty';

-- should fail:  cannot specify parameter style SQL with external name
create function get_java_property(in name varchar(128))
returns varchar(128)
contains sql
external name 'class java.lang.System.getProperty'
parameter style sql;

-- should fail:  cannot specify language JAVA with SQL body
create function celsius_to_fahrenheit(in c double)
returns double
language java
contains sql
return c*1.8 + 32;

-- should fail:  cannot specify parameter style JAVA with SQL body
create function celsius_to_fahrenheit(in c double)
returns double
parameter style java
contains sql
return c*1.8 + 32;

-- begin tests for external Java routines

create function get_java_property(in name varchar(128))
returns varchar(128)
no sql
external name 'class java.lang.System.getProperty';
drop function get_java_property;

-- test redundant language specification
create function get_java_property(in name varchar(128))
returns varchar(128)
language java
no sql
external name 'class java.lang.System.getProperty';
drop function get_java_property;

-- test redundant parameter style specification
create function get_java_property(in name varchar(128))
returns varchar(128)
parameter style java
no sql
external name 'class java.lang.System.getProperty';
drop function get_java_property;

-- should fail:  missing method spec
create function get_java_property(in name varchar(128))
returns varchar(128)
no sql
external name 'class foobar';

-- should fail:  unknown class
create function get_java_property(in name varchar(128))
returns varchar(128)
no sql
external name 'class java.lang.Rodent.getProperty';

-- should fail:  unknown method 
create function get_java_property(in name varchar(128))
returns varchar(128)
no sql
external name 'class java.lang.System.getHotels';

-- should fail:  parameter type mismatch
create function get_java_property(in name int)
returns varchar(128)
no sql
external name 'class java.lang.System.getProperty';

-- should fail:  return type mismatch
create function get_java_property(in name varchar(128))
returns int
no sql
external name 'class java.lang.System.getProperty';
