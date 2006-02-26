-- $Id$
-- Test queries which make use of indexes

create schema oj;

create table oj.t1(i int not null primary key, j int unique);

create table oj.t2(i int not null primary key, j int unique);

insert into oj.t1 values (1,null), (2, 2), (3, 3);

insert into oj.t2 values (1,null), (2, 2), (4, 4);

create table oj.t3(v varchar(15) not null primary key);
insert into oj.t3 
values ('Mesmer'), ('Houdini'), ('Copperfield'), ('Mandrake');

set schema 'sales';

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- search unique clustered index
select name from depts where deptno=20;

select name from depts where deptno > 20 order by name;

select name from depts where deptno >= 20 order by name;

select name from depts where deptno < 20 order by name;

select name from depts where deptno <= 20 order by name;

select name from depts where deptno between 20 and 30 order by name;

select name from depts where deptno > 20 and deptno < 30 order by name;

select name from depts where deptno < 20 or deptno between 30 and 40 
order by name;

-- scaling
select name from depts where deptno=20.00;

-- scaling with rounding:  strictness change
select name from depts where deptno > 19.6 order by name;

-- scaling with rounding:  strictness change the other way
select name from depts where deptno >= 20.1 order by name;

-- no match:  overflow
select name from depts where deptno=20000000000000;

-- no match:  overflow
select name from depts where deptno>20000000000000;

-- all match:  negative overflow
select name from depts where deptno>-20000000000000 order by name;

-- no match:  make sure truncation doesn't make it look like one
select v from oj.t3 where v='Houdini                 xyz';

-- no match for Houdini:  make sure truncation doesn't make it look like one
select v from oj.t3 where v >= 'Houdini                 xyz'
order by v;

-- match for Houdini:  make sure truncation doesn't obscure that
select v from oj.t3 where v <= 'Houdini                 xyz'
order by v;

-- contradiction:  empty range
select name from depts where deptno=20 and deptno=30;

-- search beyond end
select name from depts where deptno > 50 order by name;

-- search before start
select name from depts where deptno < 5 order by name;

-- search unique clustered index with a prefix key
select name from emps where deptno=20 order by 1;

-- search unique clustered index with a prefix key and a non-indexable condition
select name from emps where deptno=20 and gender='M';

-- search unique unclustered index
select name from emps where empid=3;

-- project columns covered by clustered index
select gender from emps order by 1;

-- project columns covered by an unclustered index
select name from emps order by 1;

-- sort multiple columns covered by an unclustered index
select name,gender,deptno,empno from emps order by 3,4;

-- unique inner join via clustered index
select depts.name as dname,emps.name as ename
from emps inner join depts
on emps.deptno=depts.deptno
order by 1,2;

-- unique left outer join via clustered index
select depts.name as dname,emps.name as ename
from emps left outer join depts
on emps.deptno=depts.deptno
order by 1,2;

-- left outer join via clustered index prefix
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.deptno
order by 2,1;

-- inner join via unclustered index
select emps.name as ename,depts.name as dname
from depts inner join emps
on depts.deptno=emps.empid
order by 2,1;

-- left outer join via unclustered index
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.empid
order by 2,1;

-- inner join on nullable key
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
inner join depts on e.age = depts.deptno;

-- outer join on nullable key
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
left outer join depts on e.age = depts.deptno;

-- outer join with keys on both sides nullable
select *
from oj.t1 left outer join oj.t2
on t1.j = t2.j;

-- index join which requires swapped join inputs
select 
    depts.name as dname,e.name as ename
from 
    depts 
inner join 
    (select name,age - 20 as age from emps) e
on 
    e.age=depts.deptno
order by 1,2;

-- csv format is nicest for query plans
!set outputformat csv

-- Note that we explain some queries both with and without order by;
-- with to make sure what we executed above was using the correct plan
-- without to make sure that the order by doesn't affect other
-- aspects of optimization.

explain plan for
select name from depts where deptno=20;

explain plan for
select name from depts where deptno > 20;

explain plan for
select name from depts where deptno >= 20;

explain plan for
select name from depts where deptno < 20;

explain plan for
select name from depts where deptno <= 20;

explain plan for
select name from depts where deptno between 20 and 30;

explain plan for
select name from depts where deptno > 20 and deptno < 30;

explain plan for
select name from depts where deptno < 20 or deptno between 30 and 40;

explain plan for
select name from depts where deptno=20.00;

explain plan for
select name from depts where deptno > 19.6 order by name;

explain plan for
select name from depts where deptno >= 20.1 order by name;

explain plan for
select name from depts where deptno=20000000000000;

explain plan for
select name from depts where deptno>20000000000000;

explain plan for
select name from depts where deptno>-20000000000000 order by name;

explain plan for
select v from oj.t3 where v='Houdini                 xyz';

explain plan for
select v from oj.t3 where v >= 'Houdini                 xyz'
order by v;

explain plan for
select v from oj.t3 where v <= 'Houdini                 xyz'
order by v;

explain plan for
select name from depts where deptno=20 and deptno=30;

explain plan for
select name from emps where deptno=20 order by 1;

explain plan for
select name from emps where deptno=20 and gender='M';

explain plan for
select name from emps where empid=3;

explain plan for
select gender from emps;

explain plan for
select gender from emps order by 1;

explain plan for
select name from emps;

explain plan for
select name from emps order by 1;

explain plan for
select name,gender,deptno,empno from emps order by 3,4;

explain plan for
select depts.name as dname,emps.name as ename
from emps inner join depts
on emps.deptno=depts.deptno
order by 1,2;

explain plan for
select depts.name as dname,emps.name as ename
from emps left outer join depts
on emps.deptno=depts.deptno
order by 1,2;

explain plan for
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.deptno
order by 2,1;

explain plan for
select emps.name as ename,depts.name as dname
from depts inner join emps
on depts.deptno=emps.empid
order by 2,1;

explain plan for
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.empid
order by 2,1;

explain plan for
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
inner join depts on e.age = depts.deptno;

explain plan for
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
left outer join depts on e.age = depts.deptno;

explain plan for
select 
    depts.name as dname,e.name as ename
from 
    depts 
inner join 
    (select name,age - 20 as age from emps) e
on 
    e.age=depts.deptno
order by 1,2;

explain plan for
select *
from oj.t1 left outer join oj.t2
on t1.j = t2.j;

-- can only explain plan for dynamic parameter search
explain plan for
select name from depts where deptno=?;


----------------------------------------------
-- LucidDB column store bitmap indexes test --
----------------------------------------------

drop schema lbm cascade;
create schema lbm;
set schema 'lbm';
set path 'lbm';

-------------------------------------------------
-- Some ftrs tests to compare behavior against --
-------------------------------------------------

drop table ftrsemps cascade;
create table ftrsemps(
    empno integer not null constraint empno_pk primary key,
    ename varchar(40),
    deptno integer);

create index deptno_ix on ftrsemps(deptno);
create index ename_ix on ftrsemps(ename);

insert into ftrsemps 
select empno+deptno*1000, name, deptno from sales.emps;

!set outputformat csv

-- no sarg pred
explain plan for
select * from ftrsemps;

-- the most simple case
explain plan for
select * from ftrsemps where deptno = 2;

-- negative number in index search
explain plan for
select * from ftrsemps where deptno = -2;

-- range predicate uses index access
explain plan for
select * from ftrsemps where deptno > 2;

-- Merge ranges on the same index key
explain plan for
select * from ftrsemps where deptno = 2 or deptno = 10;

-- Should have only one range
explain plan for
select * from ftrsemps where deptno = 10 or deptno > 2;

-- recognize AND on the same key
explain plan for
select * from ftrsemps where deptno > 2 and deptno < 10;

-- make sure NULL range from sarg analysis is working
explain plan for
select * from ftrsemps where deptno = 2 and deptno = 10;

-- Index only access:
-- It seems index only access is not used here.
explain plan for
select deptno from ftrsemps where deptno = 2;

-- index on char types:
-- simple comparison predicate
explain plan for
select ename from ftrsemps where ename = 'ADAM';

-- index on char types:
-- predicate specific to character types
explain plan for
select ename from ftrsemps where ename like 'ADAM%';

-- AND: does recognize one index, but not two
explain plan for
select * from ftrsemps where deptno = 2 and ename = 'ADAM';

-- OR: does not use any index access
explain plan for
select * from ftrsemps where deptno = 2 or ename = 'ADAM';

!set outputformat table

drop table ftrsemps cascade;

-------------------------------------------------------
-- Part 1. index created on empty column store table --
-------------------------------------------------------
drop table lbmemps cascade;
create table lbmemps(
    empno integer,
    ename varchar(40),
    deptno integer)
    server sys_column_store_data_server
create index deptno_ix on lbmemps(deptno)
create index ename_ix on lbmemps(ename)
create index ename_ix on lbmemps(ename)
create index empno_ix2 on lbmemps(empno)
;

-- create index on existing column store table does not work yet
-- create index ename_ix on lbmemps(ename);

-- two indexes on the same column
-- create index on exisitng column store table does not work yet
-- create index empno_ix2 on lbmemps(empno);

insert into lbmemps select empno, name, deptno from sales.emps;
insert into lbmemps select empno, name, deptno from sales.emps;

!set outputformat csv

-- no sarg pred
explain plan for
select * from lbmemps order by empno;

select * from lbmemps order by empno;

-- the most simple case
explain plan for
select * from lbmemps where deptno = 20 order by empno;

select * from lbmemps where deptno = 20 order by empno;

-- negative number in index search
explain plan for
select * from lbmemps where deptno = -2;

-- range predicate uses index access
explain plan for
select * from lbmemps where deptno > 20 order by empno;

-- Merge ranges on the same index key
explain plan for
select * from lbmemps where deptno = 10 or deptno = 20 order by empno;

-- Should have only one range
explain plan for
select * from lbmemps where deptno = 20 or deptno > 10 order by empno;

-- recognize AND on the same key
explain plan for
select * from lbmemps where deptno > 10 and deptno <= 20 order by empno;

-- make sure NULL range from sarg analysis is working
explain plan for
select * from lbmemps where deptno = 20 and deptno = 10 order by empno;

select * from lbmemps where deptno = 20 and deptno = 10 order by empno;

-- TODO implement index only access.
explain plan for
select deptno from lbmemps where deptno = 20 order by deptno;

-- index on char types:
-- simple comparison predicate
explain plan for
select ename from lbmemps where ename = 'ADAM' order by ename;

-- index on char types:
-- predicate specific to character types
explain plan for
select ename from lbmemps where ename like 'ADAM%' order by ename;

-- TODO AND: currently does recognize one index, but not two
-- TODO: this is currently not working(not even one index)
explain plan for
select * from lbmemps where deptno = 2 and ename = 'ADAM' order by empno;

-- TODO OR: currently does not use any index access
explain plan for
select * from lbmemps where deptno = 2 or ename = 'ADAM' order by empno;

!set outputformat table

-----------------------------------------------------------
-- Part 2. index created on non-empty column store table --
-----------------------------------------------------------

--------------
-- Clean up --
--------------
drop schema lbm cascade;


-- End index.sql
