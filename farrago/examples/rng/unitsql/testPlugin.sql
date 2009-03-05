-- $Id$
-- Test usage of RNG plugin

-- verify that without plugin enabled, custom syntax is unrecognized

create schema rngtest;
set schema 'rngtest';
set path 'rngtest';

-- should fail
create rng rng1 external '${FARRAGO_HOME}/testgen/rng1.dat' seed 999;


-- now, enable plugin personality for this session
alter session implementation set jar sys_boot.sys_boot.rngplugin;


-- create some random number generators; use seeds to guarantee determinism

create rng rng1 external '${FARRAGO_HOME}/testgen/rng1.dat' seed 999;

create rng rng2 external '${FARRAGO_HOME}/testgen/rng2.dat' seed 999;

create rng rng3 external '${FARRAGO_HOME}/testgen/rng3.dat' seed 777;

create function rng_next_int(
    rng_name varchar(512),
    n int)
returns int
language java
reads sql data
external name 
'sys_boot.sys_boot.rngplugin:net.sf.farrago.rng.FarragoRngUDR.rng_next_int';

-- test various ways of naming the rng

values rng_next_int('rng1',10);

values rng_next_int('RNG1',10);

values rng_next_int('localdb.rngtest."RNG1"',10);

values rng_next_int('localdb.rngtest.rng1',10);

values rng_next_int('rng1',10);

values rng_next_int('rng1',10);

-- should fail:  bad schema
values rng_next_int('sales.rng1',10);

-- should fail:  no such RNG
values rng_next_int('rng_nonexistent',10);


-- verify that rng with same initial seed yields same sequence

values rng_next_int('rng2',10);

values rng_next_int('rng2',10);

values rng_next_int('rng2',10);

values rng_next_int('rng2',10);

values rng_next_int('rng2',10);

values rng_next_int('rng2',10);


-- verify that rng with different initial seed yields different sequence

values rng_next_int('rng3',10);

values rng_next_int('rng3',10);

values rng_next_int('rng3',10);

values rng_next_int('rng3',10);

values rng_next_int('rng3',10);

values rng_next_int('rng3',10);


-- test fancy syntax
values next_random_int(ceiling 10 from rng2);

values next_random_int(unbounded from rng2);


-- test view over rng

create view random_personality_view as
values next_random_int(ceiling 10 from rng2);

create view random_udf_view as
values rng_next_int('rng2',10);

select * from random_personality_view;

select * from random_udf_view;

-- should fail:  dependency
drop rng rng2 restrict;

-- should fail:  SELECT DISTINCT feature is disabled in this personality
select distinct empno from sales.emps order by empno;

-- now, disable plugin personality for this session
alter session implementation set default;

-- flush query cache
call sys_boot.mgmt.flush_code_cache();

-- verify that DDL personality is wiped out
-- should fail
create rng rng4 external '${FARRAGO_HOME}/testgen/rng4.dat' seed 777;

-- verify that we can still access plugin functionality via UDF
values rng_next_int('rng3',10);

-- sorry, view based on personality will no longer work  :(
select * from random_personality_view;

-- but view based on UDF will
select * from random_udf_view;

-- verify that DROP CASCADE works correctly even without DDL personality
-- TODO:  use Java filesystem access to verify creation/deletion of .dat file
drop schema rngtest cascade;
drop schema sys_boot.old_stuff cascade;

-- verify that SELECT DISTINCT is working again
select distinct empno from sales.emps order by empno;

-- NOTE jvs 4-Mar-2009:  This doesn't really belong here, but this
-- test is currently the only place where we restore a clean catalog,
-- so it's convenient for testing out the procedure for switching to Unicode

create schema typecheck;

create view typecheck.v as
select "characterSetName","collationName","ordinal"
from sys_fem."SQL2003"."AbstractColumn"
where "name" like 'ASC%DESC';

select * from typecheck.v;

-- should fail because tables still exist
call sys_boot.mgmt.change_default_character_set_to_unicode();

drop schema sales cascade;

-- should succeed now since we dropped all the tables
call sys_boot.mgmt.change_default_character_set_to_unicode();

-- these should have switched from ISO-8859-1 to UNICODE
select * from typecheck.v;

-- last thing we do is to prepare for a restore of pre-upgrade catalog contents
-- NOTE:  this will shut down the system, so don't add any commands
-- after it
alter system replace catalog;
