-- $Id$
-- Test queries which require Cartesian products

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- simple cross product
select emps.name as ename,depts.name as dname 
from emps,depts
order by 1,2;

-- cross product with a non-restartable Java plan for the right-hand input
select emps.name as ename,d.dnoplus 
from emps,(select deptno+1 as dnoplus from depts) d
order by 1,2;

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
