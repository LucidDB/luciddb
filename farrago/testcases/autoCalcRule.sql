-- $Id$

explain plan for select jplus(1, 1) from values(true);

explain plan for select cplus(1, 1) from values(true);

explain plan for select cplus(1, 1), jplus(2, 2) from values(true);

explain plan for select jplus(2, 2), cplus(1, 1) from values(true);

explain plan for select cplus(jplus(1, 1), 2) from values(true);

explain plan for select jplus(cplus(1, 1), 2) from values(true);

explain plan for select empno, cplus(jplus(deptno, empid), age), jplus(cplus(deptno, empid), age), age from sales.emps;

explain plan for select * from sales.emps where jplus(deptno, 1) = 100;

explain plan for select * from sales.emps where jplus(cplus(deptno, 1), 2) = 100;

explain plan for select * from sales.emps where cplus(jplus(deptno, 1), 2) = 100;

explain plan for select cplus(jplus(deptno, 1), 2), jplus(cplus(deptno, 1), 2) from sales.emps where cplus(jplus(deptno, 1), 2) = 100 or jplus(cplus(deptno, 1), 2) = 100;

explain plan for select jplus(cplus(deptno, 1), 2), cplus(jplus(deptno, 1), 2) from sales.emps where cplus(jplus(deptno, 1), 2) = 100 or jplus(cplus(deptno, 1), 2) = 100;

explain plan for select jplus(cplus(deptno, 1), 2), cplus(jplus(deptno, 1), 2) from sales.emps where slacker;

-- Test top-most level can be implemented in any calc and last
-- expression isn't a RexCall.  dtbug 210
explain plan for select deptno + jplus(cplus(deptno, 1), 2), empno from sales.emps;

-- Equivalent to dtbug 210
explain plan for select cplus(t.r."second", 1) from (select jrow(deptno, empno) as r from sales.emps) as t;

-- Found a bug related to this expression while debugging dtbug 210.
explain plan for select t.r."second" from (select jrow(deptno, cplus(empno, 1)) as r from sales.emps) as t;

