-- $Id$
-- Test queries which execute row-by-row filters

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- test an ORDER BY for which a sort is required
select city from emps order by 1;

-- verify plans
!set outputformat csv

explain plan for
select city from emps order by 1;

explain plan for
select city from emps order by emps.city;

explain plan for
select emps.city from emps order by emps.city;

explain plan for
select emps.city from emps order by city;

explain plan for
select emps.city a from emps order by a;

-- order on non select list items
explain plan for select city from emps order by empno;

-- correct plan and correct result
-- using the user provided alias to disambiguate the columns with same names
-- but from different tables.
explain plan for
select emps.deptno a, depts.deptno b from emps, depts order by a, b;

select emps.deptno a, depts.deptno b from emps, depts order by a, b;

-- order by columns with same alias
explain plan for
select emps.deptno, depts.deptno from emps, depts
order by emps.deptno, depts.deptno;

-- end sort.sql
