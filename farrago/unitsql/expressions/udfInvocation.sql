-- $Id$
-- Test queries with UDF invocations

create schema udftest;

set schema 'udftest';

set path 'udftest';

-- test basic SQL-defined function
create function celsius_to_fahrenheit(in c double)
returns double
contains sql
return c*1.8 + 32;

-- test usage of rewritten builtin
create function coalesce2(in x varchar(128),in y varchar(128))
returns varchar(128)
contains sql
return coalesce(x,y);

-- should fail:  implicit cast not allowed
create function bad_atoi(in x varchar(128))
returns integer
contains sql
return x;

-- test that explicit cast does occur
create function good_atoi(in x varchar(128))
returns integer
contains sql
return cast(x as integer);

-- test something mildly complicated
create function stirfry(in x varchar(128))
returns varchar(128)
contains sql
return case when x like 'A%' then upper(x)||'gator' else lower(x) end;

-- test CALLED ON NULL INPUT
create function replace_null(in x varchar(128))
returns varchar(128)
contains sql
called on null input
return coalesce(x,'null and void');

-- test RETURNS NULL ON NULL INPUT
create function dont_replace_null(in x varchar(128))
returns varchar(128)
contains sql
returns null on null input
return coalesce(x,'null and void');

-- test external Java function
create function noargs()
returns varchar(128)
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.noargs';

create function substring24(in s varchar(128))
returns varchar(2)
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.substring24';

create function prim_int_to_hex_string(in i int)
returns varchar(128)
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.toHexString';

create function obj_int_to_hex_string(in i int)
returns varchar(128)
no sql
called on null input
external name 
'class net.sf.farrago.test.FarragoTestUDR.toHexString(java.lang.Integer)';

create function null_preserving_int_to_hex_string(in i int)
returns varchar(128)
no sql
returns null on null input
external name 
'class net.sf.farrago.test.FarragoTestUDR.toHexString(java.lang.Integer)';

create function atoi(in s varchar(128))
returns int
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.atoi';

create function atoi_with_null_for_err(in s varchar(128))
returns int
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.atoiWithNullForErr';

create function get_java_property(in name varchar(128))
returns varchar(128)
no sql
external name 'class java.lang.System.getProperty';

create procedure set_java_property(in name varchar(128),in val varchar(128))
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.setSystemProperty';

create function access_sql_illegal()
returns int
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.accessSql';

create function access_sql_legal()
returns int
contains sql
external name 'class net.sf.farrago.test.FarragoTestUDR.accessSql';

create function throw_sql_exception()
returns int
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.throwSQLException';

create function throw_npe()
returns int
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.throwNPE';

-- test a function that uses another function
create function celsius_to_rankine(in c double)
returns double
contains sql
return celsius_to_fahrenheit(c) + 459.67;

-- should fail:  we don't allow recursion
create function factorial(in x integer)
returns integer
contains sql
return case when x = 1 then x else x*factorial(x-1) end;

-- should fail:  we don't allow mutual recursion either
create schema crypto
create function alice(in x double)
returns double
contains sql
return bob(x*13)
create function bob(in x double)
returns double
contains sql
return alice(x/17);

-- test forward reference
create schema crypto2
create function alice(in x double)
returns double
contains sql
return bob(x*13)
create function bob(in x double)
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

-- verify path selection

create schema v1
create function important_constant()
returns integer
contains sql
return 5;

create schema v2
create function important_constant()
returns integer
contains sql
return 17;

set path 'v1';
values important_constant();

values v2.important_constant();

set path 'v2';
values important_constant();

values v1.important_constant();

-- TODO:  test with set path 'v1,v2';

set path 'udftest';
-- should fail
values important_constant();
