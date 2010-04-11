-- $Id$
-- Reproductions for some farrago bugs relating to UDFs.

-- Separate from the main test udfInvocation.sql since it depends on luciddb
-- personality.

-- FRG-331
create schema FRG_331;
set schema 'FRG_331';
set path 'FRG_331';

create function ramp(n int)
returns table(i int)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.ramp';

create function uniq(c cursor)
returns table(c.*)
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.test.FarragoTestUDR.removeDupsFromPresortedCursor';

-- use LucidDB optimizer to avoid buffering on the cartesian join RHS
-- (since that would cover up the original bug)
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- before the bugfix, this returned 20 instead of 200
select count(*) from
(select * from table(uniq(cursor(select * from table(ramp(10)) order by 1)))),
(select * from table(uniq(cursor(select * from table(ramp(20)) order by 1))));

-- before the bugfix, this resulted in a ConcurrentModificationException
select count(*) from
(select * from table(uniq(cursor(select * from table(ramp(10)))))),
(select * from table(uniq(cursor(select * from table(ramp(20))))));

-- make sure there is no buffering
!set outputformat csv
explain plan excluding attributes for
select count(*) from
(select * from table(uniq(cursor(select * from table(ramp(10)) order by 1)))),
(select * from table(uniq(cursor(select * from table(ramp(20)) order by 1))));

alter session implementation set default;
