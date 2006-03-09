-- $Id$
-- Test queries which abuse the implementation of conversion between Fennel 
-- and Iterator calling convention.

set schema 'sales';

-- NOTE: many of the union.sql and cartesians.sql queries are good tests
--       of Fennel/Iterator conversion as well.

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- Planner will use a single rel for both references to depts.  Make sure
-- we keep them straight.  (Yes this query is highly contrived.)
explain plan for 
select * from 
    (select deptno + 1, name from depts) as d,
    (select deptno as id, name from depts union select empno as id, name from emps) as n
where
    d.name = n.name;

-- Execute to verify output.
select * from 
    (select deptno + 1 as dnoplus, name from depts) as d,
    (select deptno as id, name from depts union select empno as id, name from emps) as n
where
    d.name = n.name;
