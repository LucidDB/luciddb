-- $Id$
-- Test aggregate queries

set schema 'sales';

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- verify plans
!set outputformat csv

explain plan for
select count(*) from sales.depts;

explain plan for
select count(deptno) from sales.depts;

explain plan for
select sum(deptno) from sales.depts;

explain plan for
select max(deptno) from sales.depts;

explain plan for
select min(deptno) from sales.depts;
