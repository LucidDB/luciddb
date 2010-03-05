-- $Id$
-- Test queries which require Cartesian products

set schema 'sales';

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- simple cross product
select emps.name as ename,depts.name as dname 
from emps,depts
order by 1,2;

-- cross product requiring a Java plan restart for the right-hand input
select emps.name as ename,d.dnoplus 
from emps,(select deptno+1 as dnoplus from depts) d
order by 1,2;

-- weird cross product used for uncorrelated subqueries
select depts.name from emps left outer join depts on TRUE 
order by 1;

-- verify plans
!set outputformat csv

explain plan for
select emps.name as ename,depts.name as dname from emps,depts;

explain plan for
select emps.name as ename,depts.name as dname from emps,depts
order by 1,2;

explain plan for
select emps.name as ename,d.dnoplus 
from emps,(select deptno+1 as dnoplus from depts) d;

explain plan for
select depts.name from emps left outer join depts on TRUE;

-- try lcs tables; lcs costing doesn't have a weird fudge factor so there will
-- be cases where it doesn't make sense to buffer
create schema lcscartesian;
set schema 'lcscartesian';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
create table lcsemps(
    empno int, name varchar(12), deptno int, gender char(1), city varchar(12),
    empid int, age int);
insert into lcsemps
    select empno, name, deptno, gender, city, empid, age from sales.emps;
create table lcsdepts(deptno int, name varchar(12));
insert into lcsdepts select * from sales.depts;

!set outputformat table
-- should not use buffering
select * from lcsemps e, lcsdepts d order by 1, 2, 3, 4, 5, 6, 7, 8, 9;
select e.name, d.deptno from lcsemps e, lcsdepts d order by 1, 2;

-- should use buffering
select e.name, d.*
    from lcsemps e, (select min(deptno) from lcsdepts) d order by 1;

-- should still use buffering, swapping the join operands
select e.name, d.*
    from (select min(deptno) from lcsdepts) d, lcsemps e order by 1;
select d.*, e.name
    from (select min(deptno) from lcsdepts) d, lcsemps e order by 2;

!set outputformat csv
-- the following 2 should not use buffering
explain plan for
select * from lcsemps e, lcsdepts d order by 1, 2, 3, 4, 5, 6, 7, 8, 9;

explain plan for
select e.name, d.deptno from lcsemps e, lcsdepts d order by 1, 2;

-- should use buffering
explain plan for
select e.name, d.*
    from lcsemps e, (select min(deptno) from lcsdepts) d order by 1;

-- should still use buffering, swapping the join operands
explain plan for
select e.name, d.*
    from (select min(deptno) from lcsdepts) d, lcsemps e order by 1;
explain plan for
select d.*, e.name
    from (select min(deptno) from lcsdepts) d, lcsemps e order by 2;

-- with dtbug 2070, constant reduction throws the volcano planner into a loop
explain plan for
select * from (values (1)) as va(i), (values (4, 4)) as vb(x, j) where j = x;

-- End cartesian.sql

