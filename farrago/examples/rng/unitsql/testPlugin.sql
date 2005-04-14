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

-- should fail
create rng rng_badpath external '/doodoo/blob.dat' seed 999;

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


-- now, disable plugin personality for this session
alter session implementation set default;

-- verify that DDL personality is wiped out
-- should fail
create rng rng4 external '${FARRAGO_HOME}/testgen/rng4.dat' seed 777;

-- verify that we can still access plugin functionality via UDF
values rng_next_int('rng3',10);

-- and verify that DROP CASCADE works correctly even without DDL personality
-- TODO:  use Java filesystem access to verify creation/deletion of .dat file
drop schema rngtest cascade;
