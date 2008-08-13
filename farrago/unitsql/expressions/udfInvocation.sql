-- $Id$
-- Test queries with UDF invocations

create schema udftest;

set schema 'udftest';

set path 'udftest';

-- test basic SQL-defined function
create function celsius_to_fahrenheit(c double)
returns double
contains sql
return c*1.8 + 32;

-- test usage of rewritten builtin
create function coalesce2(x varchar(128),y varchar(128))
returns varchar(128)
contains sql
return coalesce(x,y);

-- should fail:  implicit cast not allowed
create function bad_atoi(x varchar(128))
returns integer
contains sql
return x;

-- test that explicit cast does occur
create function good_atoi(x varchar(128))
returns integer
contains sql
return cast(x as integer);

-- test something mildly complicated
create function stirfry(x varchar(128))
returns varchar(128)
contains sql
return case when x like 'A%' then upper(x)||'gator' else lower(x) end;

-- test CALLED ON NULL INPUT
create function replace_null(x varchar(128))
returns varchar(128)
contains sql
called on null input
return coalesce(x,'null and void');

-- test RETURNS NULL ON NULL INPUT
create function dont_replace_null(x varchar(128))
returns varchar(128)
contains sql
returns null on null input
return coalesce(x,'null and void');

-- test external Java function
create function noargs()
returns varchar(128)
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.noargs';

create function substring24(s varchar(128))
returns varchar(2)
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.substring24';

create function prim_int_to_hex_string(i int)
returns varchar(128)
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.toHexString';

create function obj_int_to_hex_string(i int)
returns varchar(128)
language java
no sql
called on null input
external name 
'class net.sf.farrago.test.FarragoTestUDR.toHexString(java.lang.Integer)';

create function null_preserving_int_to_hex_string(i int)
returns varchar(128)
language java
no sql
returns null on null input
external name 
'class net.sf.farrago.test.FarragoTestUDR.toHexString(java.lang.Integer)';

create function decimal_abs(n decimal(6, 4)) 
returns decimal(6, 4)
language java
no sql
external name
'class net.sf.farrago.test.FarragoTestUDR.decimalAbs(java.math.BigDecimal)';

create function atoi(s varchar(128))
returns int
language java
no sql
deterministic
external name 'class net.sf.farrago.test.FarragoTestUDR.atoi';

create function null_preserving_atoi(s varchar(128))
returns int
language java
no sql
returns null on null input
deterministic
external name 'class net.sf.farrago.test.FarragoTestUDR.atoi';

create function atoi_with_null_for_err(s varchar(128))
returns int
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.atoiWithNullForErr';

create function get_java_property(name varchar(128))
returns varchar(128)
language java
no sql
external name 'class java.lang.System.getProperty';

create procedure set_java_property(in name varchar(128),in val varchar(128))
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.setSystemProperty';

create function access_sql_illegal()
returns int
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.accessSql';

create function access_sql_legal()
returns int
language java
contains sql
external name 'class net.sf.farrago.test.FarragoTestUDR.accessSql';

create function throw_sql_exception()
returns int
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.throwSQLException';

create function throw_npe()
returns int
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.throwNPE';


-- test UDF which depends on FarragoUdrRuntime
create function generate_random_number(seed bigint)
returns bigint
language java
no sql
not deterministic
external name 'class net.sf.farrago.test.FarragoTestUDR.generateRandomNumber';

-- alias to avoid common subexpression elimination
create function generate_random_number2(seed bigint)
returns bigint
language java
no sql
not deterministic
external name 'class net.sf.farrago.test.FarragoTestUDR.generateRandomNumber';

-- test UDF which depends on FarragoUdrRuntime, with
-- ClosableAllocation support
create function gargle()
returns integer
language java
no sql
deterministic
external name 'class net.sf.farrago.test.FarragoTestUDR.gargle';

-- test a function that uses another function
create function celsius_to_rankine(c double)
returns double
contains sql
return celsius_to_fahrenheit(c) + 459.67;

-- should fail:  we don't allow recursion
create function factorial(x integer)
returns integer
contains sql
return case when x = 1 then x else x*factorial(x-1) end;

-- UDX
create function ramp(n int)
returns table(i int)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.ramp';

-- UDX that allows a null argument
create function nullableRamp(n int)
returns table(i int)
language java
parameter style system defined java
no sql
external name
'class net.sf.farrago.test.FarragoTestUDR.nullableRamp(java.lang.Integer, java.sql.PreparedStatement)';

-- UDX with input
create function stringify(c cursor, delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringify';

-- UDX with input from which output row type is derived
create function digest(c cursor)
returns table(c.*, row_digest int)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.digest';

-- UDX which specifies a calendar argument
create function foreign_time(
  ts timestamp, tsZoneId varchar(256), foreignZoneId varchar(256))
returns table(
  the_timestamp timestamp, the_date date, the_time time)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.foreignTime';

-- UDX that contains a column list parameter
create function stringifyColumns(
    c cursor,
    cl select from c,
    delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringifyColumns';

-- UDX that contains 2 column list parameters referencing the same cursor
create function stringify2ColumnLists(
    cl select from c,
    c2 select from c,
    c cursor,
    delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringify2ColumnLists';

-- UDX that contains 2 column list parameters referencing different cursors
create function combineStringifyColumns(
    c1 cursor,
    cl1 select from c1,
    c2 cursor,
    cl2 select from c2,
    delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name
'class net.sf.farrago.test.FarragoTestUDR.combineStringifyColumns';

-- same as above but arguments are jumbled
create function combineStringifyColumnsJumbledArgs(
    cl2 select from c2,
    c1 cursor,
    delimiter varchar(128),
    c2 cursor,
    cl1 select from c1)
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name
'class net.sf.farrago.test.FarragoTestUDR.combineStringifyColumnsJumbledArgs';

create view ramp_view as select * from table(ramp(3));

create view stringified_view as 
select * 
from table(stringify(
    cursor(select * from sales.depts where deptno=20 order by 1),
    '|'));

create view stringifiedColumns_view as
select * 
from table(stringifyColumns(
    cursor(select * from sales.emps where deptno=20 order by 1),
    row(name, empno, gender),
    '|'));

create view combineStringifiedColumns_view as
select * 
from table(combineStringifyColumns(
    cursor(select * from sales.emps where deptno=20 order by 1),
    row(name, empno, gender),
    cursor(select * from sales.depts where deptno= 20 order by 1),
    row(name),
    '|'));

-- should fail : empno doesn't exist
select * 
from table(stringifyColumns(
    cursor(select * from sales.depts where deptno=20 order by 1),
    row(name, empno),
    '|'));

-- should fail : should reference column by its alias
select *
from table(stringifyColumns(
    cursor(select name as n from sales.depts where deptno=20 order by 1),
    row(name),
    '|'));

-- should fail : wrong number of arguments
select * 
from table(stringifyColumns(
    cursor(select * from sales.emps where deptno=20 order by 1),
    row(name, empno, gender)));

-- should fail:  we don't allow mutual recursion either
create schema crypto
create function alice(x double)
returns double
contains sql
return bob(x*13)
create function bob(x double)
returns double
contains sql
return alice(x/17);

-- test forward reference
create schema crypto2
create function alice(x double)
returns double
contains sql
return bob(x*13)
create function bob(x double)
returns double
contains sql
return x/17;

values celsius_to_fahrenheit(0);

values celsius_to_fahrenheit(100);

-- should fail due to datatype mismatch
values celsius_to_fahrenheit('freezing');

values celsius_to_rankine(0);

values celsius_to_rankine(-273);

values coalesce2('hello','goodbye');

values coalesce2('hello',cast(null as varchar(128)));

values coalesce2(cast(null as varchar(128)),'goodbye');

values good_atoi('451');

-- this should fail
values good_atoi('nineoneone');

values null_preserving_atoi(cast(null as varchar(128)));

values null_preserving_atoi('451');

values stirfry('Alley');

values stirfry('LaRa');

values replace_null('not null');

values replace_null(cast(null as varchar(128)));

values dont_replace_null('not null');

values dont_replace_null(cast(null as varchar(128)));

values noargs();

values substring24('superman');

-- this should fail with a Java exception
values substring24(cast(null as varchar(128)));

values prim_int_to_hex_string(255);

-- this should fail with an SQL exception for NULL detected
values prim_int_to_hex_string(cast(null as integer));

values obj_int_to_hex_string(255);

-- this should return 'nada'
values obj_int_to_hex_string(cast(null as integer));

values null_preserving_int_to_hex_string(255);

-- this should return null
values null_preserving_int_to_hex_string(cast(null as integer));

    values decimal_abs(-54.1234);

values atoi('451');

-- this should fail with a Java exception
values atoi(cast(null as varchar(128)));

-- this should fail with a Java exception
values atoi('Posey');

values atoi_with_null_for_err('451');

-- this should return null
values atoi_with_null_for_err(cast(null as varchar(128)));

-- this should return null
values atoi_with_null_for_err('Violet');

call set_java_property('net.sf.farrago.test.grue', 'lurker');

values get_java_property('net.sf.farrago.test.grue');

-- verify that we can pass null to procedures without cast
-- FRG-128:  find out why we can't do the same for functions
-- (maybe because of different overloading rules)
-- should fail with NullPointerException
call set_java_property('net.sf.farrago.test.grue', null);

-- here's the inconsistent function behavior
values get_java_property(null);

values access_sql_illegal();

values access_sql_legal();

-- should fail
values throw_sql_exception();

-- should fail
values throw_npe();

-- runtime context
select generate_random_number(42) as rng from sales.depts order by 1;

-- runtime context:  verify that the two instances produce
-- identical sequences independently (no interference)
select generate_random_number(42) as rng1, generate_random_number2(42) as rng2
from sales.depts order by 1;

-- runtime context:  verify that the two instances produce
-- different sequences independently (no interference)
select generate_random_number(42) as rng1, generate_random_number2(43) as rng2
from sales.depts order by 1;

-- runtime context:  verify closeAllocation
values get_java_property('feeble');
values gargle();
values get_java_property('feeble');

-- should fail:  numeric can't be implicitly cast to any integer type
values generate_random_number(42.0);

-- should pass
values generate_random_number(cast(42.0 as int));

!set outputformat csv

-- verify that constant reduction is NOT used for non-deterministic functions
explain plan for values generate_random_number(42);

-- verify that constant reduction IS used for deterministic functions
-- with constant input
explain plan for select atoi('99') from sales.depts;

-- verify that constant reduction is NOT used for deterministic functions
-- with non-constant input
explain plan for select atoi(name) from sales.depts;

-- verify that UDX no-input rowcount is 1.0
explain plan including all attributes for
select * from table(ramp(5));

-- verify that UDX one-input rowcount propagates through to output
explain plan including all attributes for
select v
from table(stringify(
    cursor(select * from sales.depts),
    '|'));

-- verify that UDX multi-input rowcount gets summed like UNION ALL
explain plan including all attributes for
select v
from table(combineStringifyColumns(
    cursor(select empno, name, deptno, gender from sales.emps),
    row(empno, name, gender),
    cursor(select empno, name, deptno, city from sales.emps),
    row(empno, name, city),
    '|'));

!set outputformat table

-- udx invocation
select * from table(ramp(5)) order by 1;

-- udx invocation via view
select * from ramp_view order by 1;

-- udx invocation with restart on RHS of Cartesian product
select count(*) from sales.depts, table(ramp(5));

--  udx invocation with a null argument
select * from table(nullableRamp(cast(null as integer)));

-- udx invocation with input
select upper(v)
from table(stringify(
    cursor(select * from sales.depts order by 1),
    '|'))
order by 1;
select upper(v)
from table(stringifyColumns(
    cursor(select * from sales.depts order by 1),
    row(name),
    '|'))
order by 1;
select upper(v)
from table(stringifyColumns(
    cursor(select name as n from sales.depts order by 1),
    row(n),
    '|'))
order by 1;
select upper(v)
from table(stringify2ColumnLists(
    row(empno, name),
    row(deptno, gender),
    cursor(select * from sales.emps order by 1),
    '|'))
order by 1;
select upper(v)
from table(combineStringifyColumns(
    cursor(select empno, name, deptno, gender from sales.emps order by 1),
    row(empno, name, gender),
    cursor(select empno, name, deptno, city from sales.emps order by 1),
    row(empno, name, city),
    '|'))
order by 1;
select upper(v)
from table(combineStringifyColumnsJumbledArgs(
    row(empno, name, city),
    cursor(select empno, name, deptno, gender from sales.emps order by 1),
    '|',
    cursor(select empno, name, deptno, city from sales.emps order by 1),
    row(empno, name, gender)))
order by 1;
select *
from table(stringifyColumns(
    cursor(select * from sales.depts where deptno=20 order by 1),
    row(name),
    '|'))
union all
select *
from table(stringifyColumns(
    cursor(select * from sales.emps where deptno=20 order by 1),
    row(name, empno, gender),
    '|'));

-- udx invocation with input via view
select * from stringified_view;
select * from stringifiedColumns_view;
select * from combineStringifiedColumns_view;

-- udx invocation with input auto-propagated to output
select * 
from table(digest(cursor(select * from sales.depts)))
order by row_digest;

-- commented out until jrockit R27 bug fixed
-- udx with specified calendar
-- select *
-- from table(foreign_time(timestamp'2006-10-09 18:32:26.992', 'PST', 'EST'));


set path 'crypto2';

values alice(12);

values bob(19);

-- verify path selection and overloading

create schema v1

create function important_constant()
returns integer
contains sql
return 5

create function confusing(x integer)
returns varchar(128)
specific confusing_integer
contains sql
return 'INTEGER:  '||cast(x as varchar(128))

create function confusing(x smallint)
returns varchar(128)
specific confusing_smallint
contains sql
return 'SMALLINT:  '||cast(x as varchar(128))

create function confusing(x varchar(128))
returns varchar(128)
specific confusing_varchar
contains sql
return 'VARCHAR:  '||x

create function confusing(x char(20))
returns varchar(128)
specific confusing_char
contains sql
return 'CHAR:  '||x
;

create schema v2

create function important_constant()
returns integer
contains sql
return 17

create function amusing(x smallint,y varchar(128))
returns integer
specific amusing1
contains sql
return 9

create function amusing(x bigint,y int)
returns integer
specific amusing2
contains sql
return 10

create function amusing(x int,y bigint)
returns integer
specific amusing3
contains sql
return 11

create function confusing(x integer)
returns varchar(128)
specific confusing_integer
contains sql
return 'V2INTEGER:  '||cast(x as varchar(128))

create function "UPPER"(x integer)
returns integer
specific upper1
contains sql
return x+1

create function "UPPER"(x varchar(128))
returns varchar(128)
specific upper2
contains sql
return x||'_plus_one'

create function "LOWER"(x integer)
returns integer
contains sql
return x-1

;

set path 'v1';
values important_constant();

values v2.important_constant();

set path 'v2';
values important_constant();

values v1.important_constant();

set path 'v1,v2';
values important_constant();

set path 'v2,v1';
values important_constant();

set path 'udftest';
-- should fail
values important_constant();

set path 'v1';

values confusing(5);

values confusing(cast(5 as tinyint));

values confusing('hello');

values confusing(cast('hello' as varchar(5)));

set path 'v2,v1';

values confusing(5);

-- v2 shouldn't hide the better match from v1 here
values confusing('hello');

-- verify that parameter filtering is left-to-right
values amusing(cast(null as smallint),cast(null as integer));

-- test resolution against builtins

values upper(7);

values upper('cobol');

values information_schema."UPPER"('cobol');

values lower(7);

values lower('COBOL');

-- should fail
values confusing(true);
