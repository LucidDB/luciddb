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
