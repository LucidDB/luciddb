-- $Id$
-- Test DDL for user-defined functions

create schema udftest;
create schema udftest2;

set schema 'udftest';

set path 'udftest';

-- test SQL-defined functions

create function celsius_to_fahrenheit(c double)
returns double
contains sql
return c*1.8 + 32;

create function add_integers(i int,j int)
returns int
contains sql
return i + j;

-- this should fail since add_integers is not a procedure
drop procedure add_integers;

-- but DROP ROUTINE should work
drop routine add_integers;

-- test overloading

-- should fail
create function celsius_to_fahrenheit(c real)
returns real
contains sql
return c*1.8 + 32;

-- should succeed:  specific name avoids conflict
create function celsius_to_fahrenheit(c real)
returns real
specific celsius_to_fahrenheit_real
contains sql
return c*1.8 + 32;

-- should fail:  schema for specific name must match schema for invocation name
create function add_integers(i int,j int)
returns int
specific udftest2.add_ints
contains sql
return i + j;

-- drop by specific name
drop function celsius_to_fahrenheit;
drop function celsius_to_fahrenheit_real;

-- test redundant language specification
create function celsius_to_fahrenheit(c real)
returns real
language sql
contains sql
return c*1.8 + 32;
drop function celsius_to_fahrenheit;

-- should fail: can't specify parameter style for SQL-defined routine
create function celsius_to_fahrenheit(c real)
returns real
parameter style sql
contains sql
return c*1.8 + 32;

-- should fail:  can't declare explicit IN parameter to function
create function add_integers(in i int,j int)
returns int
contains sql
return i + j;

-- should fail:  can't declare OUT parameter to function
create function add_integers(out i int,j int)
returns int
contains sql
return i + j;

-- should fail:  can't declare INOUT parameter to function
create function add_integers(inout i int,j int)
returns int
contains sql
return i + j;

-- should fail:  reference to bogus parameter
create function add_integers(i int,j int)
returns int
contains sql
return i + j + k;

-- test various modifiers

-- should fail:  NO SQL can't be specified for SQL-defined routines
create function add_integers(i int,j int)
returns int
no sql
return i + j;

-- a bit of a fib, but should pass
create function add_integers(i int,j int)
returns int
reads sql data
return i + j;

drop function add_integers;

-- a bit of a fib, but should pass
create function add_integers(i int,j int)
returns int
modifies sql data
return i + j;

drop function add_integers;

create function add_integers(i int,j int)
returns int
contains sql
deterministic
return i + j;

drop function add_integers;

create function add_integers(i int,j int)
returns int
contains sql
not deterministic
return i + j;

drop function add_integers;

create function add_integers(i int,j int)
returns int
contains sql
returns null on null input
return i + j;

drop function add_integers;

create function add_integers(i int,j int)
returns int
contains sql
called on null input
return i + j;

drop function add_integers;

-- test dependencies and cascade/restrict

create function to_upper(v varchar(128))
returns varchar(128)
contains sql
return upper(v);

create view upper_crust as
select to_upper(name)
from sales.depts;

create function to_upper2(v varchar(128))
returns varchar(128)
contains sql
return upper(v);

create function to_uppertrim(v varchar(128))
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

create function add_integers(i int,j int)
returns int
contains sql
return i + j;

-- should fail
create schema badpath
create function double_integer(i int)
returns int
contains sql
return add_integers(i,i);

-- should succeed
create schema goodpath
path udftest
create function double_integer(i int)
returns int
contains sql
return add_integers(i,i);

-- begin tests for mismatched Java/SQL

-- should fail:  cannot specify both SQL body and external name
create function get_java_property(name varchar(128))
returns varchar(128)
contains sql
external name 'class java.lang.System.getProperty'
return 'undefined';

-- should fail:  cannot specify language SQL with external name
create function get_java_property(name varchar(128))
returns varchar(128)
language sql
contains sql
external name 'class java.lang.System.getProperty';

-- should fail:  cannot specify parameter style SQL with external name
create function get_java_property(name varchar(128))
returns varchar(128)
language java
parameter style sql
contains sql
external name 'class java.lang.System.getProperty';

-- should fail:  cannot specify parameter style SQL with external name
create function get_java_property(name varchar(128))
returns varchar(128)
language java
contains sql
external name 'class java.lang.System.getProperty'
parameter style sql;

-- should fail:  cannot specify language JAVA with SQL body
create function celsius_to_fahrenheit(c double)
returns double
language java
contains sql
return c*1.8 + 32;

-- should fail:  cannot specify parameter style JAVA with SQL body
create function celsius_to_fahrenheit(c double)
returns double
parameter style java
contains sql
return c*1.8 + 32;

-- test external Java routines

create function get_java_property(name varchar(128))
returns varchar(128)
language java
no sql
external name 'class java.lang.System.getProperty';
drop function get_java_property;

-- test redundant parameter style specification
create function get_java_property(name varchar(128))
returns varchar(128)
language java
parameter style java
no sql
external name 'class java.lang.System.getProperty';
drop function get_java_property;

-- should fail:  missing method spec
create function get_java_property(name varchar(128))
returns varchar(128)
language java
no sql
external name 'class foobar';

-- should fail:  unknown class
create function get_java_property(name varchar(128))
returns varchar(128)
language java
no sql
external name 'class java.lang.Rodent.getProperty';

-- should fail:  unknown method 
create function get_java_property(name varchar(128))
returns varchar(128)
language java
no sql
external name 'class java.lang.System.getHotels';

-- should fail:  method not found due to parameter type mismatch
create function get_java_property(name int)
returns varchar(128)
language java
no sql
external name 'class java.lang.System.getProperty';

-- should fail:  parameter type mismatch for explicit method spec
create function get_java_property(name int)
returns varchar(128)
language java
no sql
external name 'class java.lang.System.getProperty(java.lang.String)';

-- should fail:  return type mismatch
create function get_java_property(name varchar(128))
returns int
language java
no sql
external name 'class java.lang.System.getProperty';

-- test explicit selection of method from overloads
create function to_hex_string(i int)
returns varchar(128)
language java
no sql
external name 
'class net.sf.farrago.test.FarragoTestUDR.toHexString(java.lang.Integer)';


-- test early definition binding

create function magic(i bigint)
returns int
specific magic10
contains sql
return 10;

create function presto()
returns int
contains sql
return magic(1);

create function magic(i int)
returns int
specific magic20
contains sql
return 20;

-- should get 10, even though new overload for magic is a better match
values presto();

-- test stored binding for builtins vs routines

create function "UPPER"(x varchar(128))
returns varchar(128)
contains sql
return x||'_plus_one';

create function tweedledee()
returns varchar(128)
contains sql
return upper('cobol');

create function tweedledum()
returns varchar(128)
contains sql
return information_schema."UPPER"('cobol');

values tweedledee();

values tweedledum();

-- test stored bindings for specific name vs invocation name

create function gargantua()
returns varchar(128)
specific pantagruel
contains sql
return 'gargantua';

create function pantagruel()
returns varchar(128)
specific gargantua
contains sql
return 'pantagruel';

create function rabelais()
returns varchar(128)
contains sql
return gargantua();

values gargantua();

values specific gargantua();

values rabelais();

-- test conflict detection

create procedure set_java_property(in name varchar(128),val varchar(128))
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.setSystemProperty';

-- should fail:  procedures cannot be overloaded on parameter type
create procedure set_java_property(in name char(128),in val char(128))
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.setSystemProperty';

create function piffle(i int)
returns int
specific piffle1
contains sql
return 20;

-- should succeed:  functions can be overloaded on parameter type
create function piffle(d double)
returns int
specific piffle2
contains sql
return 20;

-- should fail
create function piffle(d double)
returns int
specific piffle3
contains sql
return 20;

-- should fail:  even though the parameter type is different, it is
-- in the same type precedence equivalence class
create function piffle(f float)
returns int
specific piffle4
contains sql
return 20;



-- UDX

-- should succeed
create function ramp(n int)
returns table(i int)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.ramp';

-- should fail:  wrong parameter style
create function ramp_bad_param_style(n int)
returns table(i int)
language java
parameter style java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.ramp';

-- should fail:  wrong language
create function ramp_bad_language(n int)
returns table(i int)
contains sql
return n;

-- should succeed
create function stringify(c cursor, delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringify';

-- should fail:  can't accept cursor for normal function
create function scalar_stringify(c cursor, delimiter varchar(128))
returns varchar(65535)
language java
parameter style java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringify';
