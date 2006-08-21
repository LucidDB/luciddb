-- $Id$
-- Test SQL/MED loopback link support

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

-- test loopback queries

select * from loopback_localdb.sales.depts order by deptno;

select * from loopback_sys_boot.jdbc_metadata.catalogs_view order by table_cat;

select emps.name as ename, depts.name as dname from 
loopback_localdb.sales.emps inner join loopback_localdb.sales.depts
on emps.deptno=depts.deptno
order by ename;

-- this should fail due to non-existent table
select * from loopback_localdb.sales.fudge order by deptno;


-- verify loopback via EXPLAIN PLAN
!set outputformat csv

explain plan excluding attributes for 
select * from loopback_localdb.sales.depts order by deptno;

explain plan excluding attributes for
select * from loopback_sys_boot.jdbc_metadata.catalogs_view;

explain plan excluding attributes for
select emps.name as ename, depts.name as dname from 
loopback_localdb.sales.emps inner join loopback_localdb.sales.depts
on emps.deptno=depts.deptno
order by ename;

