-- $Id$
-- Test pushdown of projections.  This test primarily tests projections 
-- involving views.  Other tests do a fairly good job testing projections
-- on base tables.

create schema pp;
set schema 'pp';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

--------------------------------------------------------------------------
-- test a few queries on FTRS first, but the bulk of the tests are against
-- LCS
--------------------------------------------------------------------------
create view vemps(eno, name, deptno, doubleage)
    as select empno, upper(name), deptno, age * 2 from sales.emps;
create view vdepts(name, deptno)
    as select upper(name), deptno from sales.depts;

!set outputformat csv
explain plan for
    select ve.name, ve.doubleage, vd.name
        from vemps ve, vdepts vd
        where ve.deptno = vd.deptno;
explain plan for
    select lower(ve.name), ve.doubleage/2
        from vemps ve, vdepts vd
        where ve.deptno = vd.deptno;
        
!set outputformat table
select ve.name, ve.doubleage, vd.name
    from vemps ve, vdepts vd
    where ve.deptno = vd.deptno order by 1;
select lower(ve.name), ve.doubleage/2
    from vemps ve, vdepts vd
    where ve.deptno = vd.deptno order by 1;

--------------------------------------------------------------------
-- run a query through Volcano to exercise the rules more thoroughly
--------------------------------------------------------------------
alter session implementation add jar sys_boot.sys_boot.volcano_plugin;
!set outputformat csv
explain plan for
    select lower(ve.name), ve.doubleage/2
        from vemps ve, vdepts vd
        where ve.deptno = vd.deptno;

drop view vemps;
drop view vdepts;

-----------
-- now, LCS
-----------
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table lcsemps(
    empno int, name varchar(12), deptno int, gender char(1), city varchar(12),
    empid int, age int);
insert into lcsemps
    select empno, name, deptno, gender, city, empid, age from sales.emps;
create table lcsdepts(deptno int, name varchar(12));
insert into lcsdepts select * from sales.depts;

create view vemps(eno, name, deptno, doubleage)
    as select empno, upper(name), deptno, age * 2 from lcsemps;
create view vdepts(name, deptno)
    as select upper(name), deptno from lcsdepts;
create view vuemps(eno, name, deptno, age) as
    select * from vemps union select empno, name, deptno, age from sales.emps;
create view vunion(id, name, number) as
    select 'emps', name, eno from vemps union
    select 'depts', name, deptno from vdepts;

explain plan for
    select ve.name, ve.doubleage, vd.name
        from vemps ve, vdepts vd
        where ve.deptno = vd.deptno;
explain plan for
    select lower(ve.name), ve.doubleage/2
        from vemps ve, vdepts vd
        where ve.deptno = vd.deptno;
explain plan for
    select name from vuemps where eno = 110;
explain plan for select id, lcs_rid(name) from vunion;
explain plan for select id, lcs_rid(name) from vunion where number in (20, 120);
        
!set outputformat table
select ve.name, ve.doubleage, vd.name
    from vemps ve, vdepts vd
    where ve.deptno = vd.deptno order by 1;
select lower(ve.name), ve.doubleage
    from vemps ve, vdepts vd
    where ve.deptno = vd.deptno order by 1;
select name from vuemps where eno = 110 order by 1;
select id, lcs_rid(name) from vunion order by 1, 2;
select id, lcs_rid(name) from vunion where number in (20, 120) order by 1;

create table t1(t1a int, t1b int, t1c int);
create table t2(t2a int, t2b int, t2c int, t2d int);
create table t3(t3a int, t3b int, t3c int, t3d int, t3e int);
insert into t1 values(1, 11, 12);
insert into t1 values(2, 21, 22);
insert into t1 values(3, 31, 32);
insert into t1 values(4, 41, 42);
insert into t1 values(5, 51, 52);
insert into t2 values(1, 101, 102, 103);
insert into t2 values(2, 201, 202, 203);
insert into t2 values(3, 301, 302, 303);
insert into t2 values(4, 401, 402, 403);
insert into t2 values(5, 501, 502, 503);
insert into t3 values(1, 1001, 1002, 1003, 1004);
insert into t3 values(2, 2001, 2002, 2003, 2004);
insert into t3 values(3, 3001, 3002, 3003, 3004);
insert into t3 values(4, 4001, 4002, 4003, 4004);
insert into t3 values(5, 5001, 5002, 5003, 5004);
create view vjoin(vja, vjb, vjc) as
    select t1.t1b - 10, t2.t2c - 100, t3.t3d - 1000
        from t1, t2, t3 where t1.t1a = t2.t2a and t2.t2a = t3.t3a;

select * from vjoin order by vja;
select vjc/1000, vja/10, vjb/100 from vjoin order by 1;
select count(*) from vjoin;
select lcs_rid(vja) from vjoin order by 1;
select 2*vjb, lcs_rid(vja) from vjoin order by 2;

!set outputformat csv
explain plan for select * from vjoin order by vja;
explain plan for select vjc/1000, vja/10, vjb/100 from vjoin order by 1;
explain plan for select count(*) from vjoin;
explain plan for select lcs_rid(vja) from vjoin order by 1;
explain plan for select 2*vjb, lcs_rid(vja) from vjoin order by 2;
