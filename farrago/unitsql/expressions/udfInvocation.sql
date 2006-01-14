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

values access_sql_illegal();

values access_sql_legal();

-- should fail
values throw_sql_exception();

-- should fail
values throw_npe();

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

