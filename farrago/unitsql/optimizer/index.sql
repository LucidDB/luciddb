-- $Id$
-- Test queries which make use of indexes

set schema sales;

-- search unique clustered index
select name from depts where deptno=20;

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

-- can only explain plan for dynamic parameter search
explain plan for
select name from depts where deptno=?;

