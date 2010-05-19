-- $Id$
-- Test SQL/MED loopback link support

create schema gloop;
create table gloop.t1(v varchar(10) not null primary key);
create user vogon identified by '' default schema gloop;
grant select on gloop.t1 to vogon;

-- create a server which will loop back to the default catalog (localdb)
create server loopback_localdb
foreign data wrapper sys_jdbc
options(
    driver_class 'net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver',
    url 'jdbc:farrago:',
    user_name 'sa');

-- create a server which will loop back to a specific catalog (sys_boot)
create server loopback_sys_boot
foreign data wrapper sys_jdbc
options(
    driver_class 'net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver',
    url 'jdbc:farrago:',
    user_name 'sa',
    qualifying_catalog_name 'SYS_BOOT');

-- create a server which will not loop back at all
create server no_loopback
foreign data wrapper sys_jdbc
options(
    driver_class 'net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver',
    url 'jdbc:farrago:',
    user_name 'VOGON',
    schema_name 'BOGUS',
    skip_type_check 'TRUE');

-- test loopback queries

select * from loopback_localdb.sales.depts order by deptno;

select * from loopback_sys_boot.jdbc_metadata.catalogs_view order by table_cat;

select emps.name as ename, depts.name as dname from 
loopback_localdb.sales.emps inner join loopback_localdb.sales.depts
on emps.deptno=depts.deptno
order by ename;

-- this should fail due to non-existent table
select * from loopback_localdb.sales.fudge order by deptno;


-- LER-3846:  use loopback to test what happens to views on foreign
-- tables when the foreign server (faked here via loopback) changes
-- column types behind our backs

create schema x;

-- simulate foreign table
create view x.foo as select 100 from (values(0));

-- create a local view which references the foreign table
create view x.bar as
select * from loopback_localdb.x.foo;

-- change the foreign table to produce a new cast-compatible type and value
create or replace view x.foo as select '  500  ' from (values(0));

-- should not fail, but should cast to original type
-- which was frozen when bar was created
select * from x.bar;

-- now force a cast failure to see what happens
create or replace view x.foo as select 'foofah' from (values(0));

call sys_boot.mgmt.flush_code_cache();
select * from x.bar;

-- next test simulates a change to a non-CAST-compatible type;
-- it's currently disabled because it produces an internal error message
-- which varies (TODO:  figure out how to improve the error message
-- create or replace view x.foo as select true from (values(0));
-- call sys_boot.mgmt.flush_code_cache();
-- select * from x.bar;

-- Next test is for making sure that for non-loopback links,
-- we defer all execution until rows are actually fetched.  To
-- avoid foreign SQL access at prepare time, we use
-- SKIP_TYPE_CHECK=TRUE on the no_loopback server, and
-- explicitly instantiate a foreign table.
create foreign table x.pokemon(
    v varchar(10))
server no_loopback
options(table_name 'T1');
create view x.baz as
select * from 
(values ('three'), ('two'))
union all
select * from x.pokemon;
-- break the view by yanking out the underlying table
drop table gloop.t1;
-- this should NOT produce an exception since we should never
-- actually execute the underlying SQL for the x.pokemon reference
!set rowlimit 1
select * from x.baz;
!set rowlimit 0

-- verify loopback via EXPLAIN PLAN
!set outputformat csv

explain plan excluding attributes for 
select * from loopback_localdb.sales.depts order by deptno;

--  REVIEW: SWZ: 2008-10-07: Re-enable this test. Currently disabled because
--  query plan varies based on repository config (Hibernate vs. Netbeans).
--  Once Hibernate because the standard, we can re-enable.
-- explain plan excluding attributes for
-- select * from loopback_sys_boot.jdbc_metadata.catalogs_view;

explain plan excluding attributes for
select emps.name as ename, depts.name as dname from 
loopback_localdb.sales.emps inner join loopback_localdb.sales.depts
on emps.deptno=depts.deptno
order by ename;

-- verify that loopback is NOT used here
explain plan for
select * from x.baz;
