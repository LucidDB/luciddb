-- $Id$
-- Test queries with UDF invocations

create schema udftest;

set schema udftest;

-- test basic function
create function celsius_to_fahrenheit(in c double)
returns double
contains sql
return c*1.8 + 32;

-- test usage of rewritten builtin
create function coalesce2(in x varchar(128),in y varchar(128))
returns varchar(128)
contains sql
return coalesce(x,y);

-- test that implicit cast does not occur
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

-- FIXME:  this should fail but doesn't yet
values bad_atoi('451');

values good_atoi('451');

-- this should fail
values good_atoi('nineoneone');

values alice(12);

values bob(19);

values stirfry('Alley');

values stirfry('LaRa');
