-- $Id$
-- Test distinct aggregate queries

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

--------------------------
-- Test Sort Aggreagtes --
--------------------------
alter session implementation set default;
!set outputformat table

select count(distinct city) from emps;

select count(distinct city) from emps where empno > 100000;

-- mixed distinct and non-distinct aggs
select sum(distinct empno), sum(empno) from emps;

-- mixed distinct and non-distinct aggs
select count(distinct manager), count(distinct gender),
   sum(distinct empno), sum(distinct empno), sum(empno) from emps;

-- expressions of distinct-aggs of expressions
select count(distinct sal + empno) + deptno, sum(distinct sal) + deptno
 from emps group by deptno order by 1,2;

------------
-- group bys
------------

explain plan with type for
select deptno, count(distinct empno) from emps group by deptno order by 1,2;

select deptno, count(distinct empno) from emps group by deptno order by 1,2;

-- group by boolean, nullable key
explain plan with type for
select slacker, count(distinct deptno) from emps
 group by slacker order by slacker;

select slacker, count(distinct deptno) from emps
 group by slacker order by slacker;

-- Test where input stream is empty
select deptno, count(distinct empno), count(*) from emps
 where deptno < 0 group by deptno order by 1,2,3;

-- group, but don't project group column
select count(distinct e.slacker and e.manager) from emps as e group by deptno
order by 1;

-- query without any non-distinct aggs
select deptno, sum(distinct age), count(distinct gender)
from emps
group by deptno
order by 1,2,3;

-- this is what the previous query expands to (make sure it returns the same)
SELECT de.deptno, adage.sum_age, adgender.count_gender
FROM (
   SELECT deptno
   FROM Emps AS e
   GROUP BY deptno) AS de
JOIN (
   SELECT deptno, COUNT(gender) AS count_gender
   FROM (
     SELECT DISTINCT deptno, gender
     FROM Emps) AS dgender
   GROUP BY deptno) AS adgender
ON de.deptno = adgender.deptno
JOIN (
   SELECT deptno, SUM(age) AS sum_age
   FROM (
      SELECT DISTINCT deptno, age
      FROM Emps) AS dage
      GROUP BY deptno) AS adage
ON de.deptno = adage.deptno
order by 1,2,3;

--------
-- joins
--------

-- cartesian
select count(distinct emps.empno) from emps as e, emps;

select count(distinct emps.empno + e.empno) from emps as e, emps;

-- count of a nullable column is not null
explain plan with type for
select d.name, count(distinct e.empid), e.empid
from depts as d join emps as e on e.deptno = d.deptno
group by d.name, e.empid;

select d.name, count(distinct e.empid), count(distinct e.gender), e.empid
from depts as d join emps as e on e.deptno = d.deptno
group by d.name, e.empid
order by 1,2,3,4;

select d.name, count(distinct e.empno), count(distinct e.slacker or e.manager)
from emps as e join depts as d on e.deptno = d.deptno
group by d.name
order by 1,2,3;


-- ------------
-- verify plans
-- ------------

!set outputformat csv

explain plan for
select count(distinct city) from emps;

explain plan for
select count(distinct city) from emps where empno > 100000;

explain plan for
select count(distinct manager), count(distinct gender),
   sum(distinct empno), sum(distinct empno), sum(empno) from emps;

explain plan for
select count(distinct sal + empno) + deptno, sum(distinct sal) + deptno
 from emps group by deptno;

-- verify plans for group bys

explain plan for
select deptno, count(distinct empno) from emps group by deptno;

explain plan for
select slacker, count(distinct deptno) from emps
 group by slacker order by slacker;

explain plan for
select deptno, count(distinct empno), count(*) from emps
 where deptno < 0 group by deptno;

explain plan for
select count(distinct e.slacker and e.manager) from emps as e group by deptno;

explain plan for
select deptno, sum(distinct age), count(distinct gender)
from emps
group by deptno;

-- verify plans for joins

explain plan for
select count(distinct emps.empno) from emps as e, emps;

explain plan for
select count(distinct emps.empno + e.empno) from emps as e, emps;

explain plan for
select d.name, count(distinct e.empid), e.empid
from depts as d join emps as e on e.deptno = d.deptno
group by d.name, e.empid;

explain plan for
select d.name, count(distinct e.empno), count(distinct e.slacker or e.manager)
from emps as e join depts as d on e.deptno = d.deptno
group by d.name;

--------------------------
-- Test Hash Aggreagtes --
--------------------------
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';
call sys_boot.mgmt.flush_code_cache();
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
!set outputformat table

select count(distinct city) from emps;

select count(distinct city) from emps where empno > 100000;

-- mixed distinct and non-distinct aggs
select sum(distinct empno), sum(empno) from emps;

-- mixed distinct and non-distinct aggs
select count(distinct manager), count(distinct gender),
   sum(distinct empno), sum(distinct empno), sum(empno) from emps;

-- expressions of distinct-aggs of expressions
select count(distinct sal + empno) + deptno, sum(distinct sal) + deptno
 from emps group by deptno order by 1,2;

------------
-- group bys
------------

explain plan with type for
select deptno, count(distinct empno) from emps group by deptno order by deptno;

select deptno, count(distinct empno) from emps group by deptno order by deptno;

-- group by boolean, nullable key
explain plan with type for
select slacker, count(distinct deptno) from emps
 group by slacker order by slacker;

select slacker, count(distinct deptno) from emps
 group by slacker order by slacker;

-- Test where input stream is empty
select deptno, count(distinct empno), count(*) from emps
 where deptno < 0 group by deptno order by 1,2,3;

-- group, but don't project group column
select count(distinct e.slacker and e.manager) from emps as e group by deptno
order by 1;

-- query without any non-distinct aggs
select deptno, sum(distinct age), count(distinct gender)
from emps
group by deptno
order by 1,2,3;

-- this is what the previous query expands to (make sure it returns the same)
SELECT de.deptno, adage.sum_age, adgender.count_gender
FROM (
   SELECT deptno
   FROM Emps AS e
   GROUP BY deptno) AS de
JOIN (
   SELECT deptno, COUNT(gender) AS count_gender
   FROM (
     SELECT DISTINCT deptno, gender
     FROM Emps) AS dgender
   GROUP BY deptno) AS adgender
ON de.deptno = adgender.deptno
JOIN (
   SELECT deptno, SUM(age) AS sum_age
   FROM (
      SELECT DISTINCT deptno, age
      FROM Emps) AS dage
      GROUP BY deptno) AS adage
ON de.deptno = adage.deptno
order by 1,2,3;

--------
-- joins
--------

-- cartesian
select count(distinct emps.empno) from emps as e, emps;

select count(distinct emps.empno + e.empno) from emps as e, emps;

-- count of a nullable column is not null
explain plan with type for
select d.name, count(distinct e.empid), e.empid
from depts as d join emps as e on e.deptno = d.deptno
group by d.name, e.empid;

select d.name, count(distinct e.empid), count(distinct e.gender), e.empid
from depts as d join emps as e on e.deptno = d.deptno
group by d.name, e.empid
order by 1,2,3,4;

select d.name, count(distinct e.empno), count(distinct e.slacker or e.manager)
from emps as e join depts as d on e.deptno = d.deptno
group by d.name
order by 1,2,3;

select e.name, d.deptno, sum(e.age), count(distinct d.name) 
from emps e, depts d where e.deptno = d.deptno
group by e.name, d.deptno
order by 1;

-- ------------
-- verify plans
-- ------------

!set outputformat csv

explain plan for
select count(distinct city) from emps;

explain plan for
select count(distinct city) from emps where empno > 100000;

explain plan for
select count(distinct manager), count(distinct gender),
   sum(distinct empno), sum(distinct empno), sum(empno) from emps;

explain plan for
select count(distinct sal + empno) + deptno, sum(distinct sal) + deptno
 from emps group by deptno;

-- verify plans for group bys

explain plan for
select deptno, count(distinct empno) from emps group by deptno;

explain plan for
select slacker, count(distinct deptno) from emps
 group by slacker order by slacker;

explain plan for
select deptno, count(distinct empno), count(*) from emps
 where deptno < 0 group by deptno;

explain plan for
select count(distinct e.slacker and e.manager) from emps as e group by deptno;

explain plan for
select deptno, sum(distinct age), count(distinct gender)
from emps
group by deptno;

-- verify plans for joins

explain plan for
select count(distinct emps.empno) from emps as e, emps;

explain plan for
select count(distinct emps.empno + e.empno) from emps as e, emps;

explain plan for
select d.name, count(distinct e.empid), e.empid
from depts as d join emps as e on e.deptno = d.deptno
group by d.name, e.empid;

explain plan for
select d.name, count(distinct e.empno), count(distinct e.slacker or e.manager)
from emps as e join depts as d on e.deptno = d.deptno
group by d.name;

explain plan for
select e.name, d.deptno, sum(e.age), count(distinct d.name) 
from emps e, depts d where e.deptno = d.deptno
group by e.name, d.deptno;

-- End aggDistinct.sql
