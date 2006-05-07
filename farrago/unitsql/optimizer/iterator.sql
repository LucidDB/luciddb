-- $Id$
-- Test queries which abuse the implementation of conversion between Fennel 
-- and Iterator calling convention.

set schema 'sales';

-- NOTE: many of the union.sql and cartesians.sql queries are good tests
--       of Fennel/Iterator conversion as well.

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- Planner will use a single FennelToIteratorConverter instance for both 
-- references to depts. Make sure we keep them straight.  (Yes this query
-- is highly contrived.)
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
    d.name = n.name
order by dnoplus;

-- Planner will use a single IteratorToFennelConverter instance for both
-- references to depts.  Make sure we keep them straight as well.
explain plan for 
select t1.deptno, t1.name, t2.deptno as deptno2, t2.name as name2
from (select deptno + 1 as deptno, name from sales.depts) as t1,
     (select deptno + 1 as deptno, name from sales.depts) as t2;

-- Execute to verify output.
select t1.deptno, t1.name, t2.deptno as deptno2, t2.name as name2
from (select deptno + 1 as deptno, name from sales.depts) as t1,
     (select deptno + 1 as deptno, name from sales.depts) as t2
order by deptno,deptno2;

-- Similar to previous, but without any underlying Fennel rels.
-- Simplified from the query given in FRG-82.
explain plan for 
select "View1"."Foo", "View2"."Foo" as "Foo0"
from (select * from (select cast(null as VARCHAR(1024)) as "Foo" from (values(0))) as c) as "View1",
     (select * from (select cast(null as VARCHAR(1024)) as "Foo" from (values(0))) as c) as "View2";

-- Execute to verify output.
select "View1"."Foo", "View2"."Foo" as "Foo0"
from (select * from (select cast(null as VARCHAR(1024)) as "Foo" from (values(0))) as c) as "View1",
     (select * from (select cast(null as VARCHAR(1024)) as "Foo" from (values(0))) as c) as "View2";
