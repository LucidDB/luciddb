-- $Id$
-- Test that catalog contents can be restored via 
-- ALTER SYSTEM REPLACE CATALOG, while preserving upgraded plugin metamodel

-- make sure old catalog contents were restored
select * from sys_boot.old_stuff.t;

-- make sure plugin metamodel still works
create schema rngtest;
set schema 'rngtest';
set path 'rngtest';

-- have to re-register jar since ALTER SYSTEM REPLACE CATALOG nuked it
create jar sys_boot.sys_boot.rngplugin 
library 'file:${FARRAGO_HOME}/examples/rng/plugin/FarragoRng.jar' 
options(0);

alter session implementation set jar sys_boot.sys_boot.rngplugin;

-- NOTE jvs 12-Aug-2006:  we can parse CREATE RNG now, but don't
-- actually try to use new RNG, because CREATE JAR above didn't
-- mark the model extension
create rng rng5 external '${FARRAGO_HOME}/testgen/rng5.dat' seed 999;

-- drop our old stuff since test cleanup won't do it for us
drop schema sys_boot.old_stuff cascade;

-- drop plugin too
drop rng rng5 cascade;
alter session implementation set default;
drop jar sys_boot.sys_boot.rngplugin options(0) cascade;

-- this one WILL get dropped by the test framework, but that will
-- cause a "negative catalog leak", so delete it explicitly here instead
drop schema rngtest cascade;
