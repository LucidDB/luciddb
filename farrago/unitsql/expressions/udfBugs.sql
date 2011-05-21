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

-- with bug dt-2387, a udx stalls if it has two or more parameters that are
-- cursors
create or replace function countTwoInputs(
  c1 cursor,
  c2 cursor)
returns table (
  n integer)
language java
parameter style system defined java
no sql
external name
'class net.sf.farrago.test.FarragoTestUDR.countTwoInputs';
-- should return 3 (1 row from left, 2 rows from right)
select * from table(
  countTwoInputs(
    cursor(values (1)),
    cursor(values ('a'), ('b'))));
-- should return 1
select * from table(
  countTwoInputs(
    cursor(values (1)),
    cursor(table sys_boot.mgmt.browse_connect_empty_options)));
-- should return 0; with bug dt-2387, this query hangs
select * from table(
  countTwoInputs(
    cursor(table sys_boot.mgmt.browse_connect_empty_options),
    cursor(table sys_boot.mgmt.browse_connect_empty_options)));
-- workaround to dt-2387 using a UDX rather than a view
select * from table(
  countTwoInputs(
    cursor(select * from table(sys_boot.mgmt.browse_connect_empty_options_udx())),
    cursor(select * from table(sys_boot.mgmt.browse_connect_empty_options_udx()))));

-- end udfBugs.sql

