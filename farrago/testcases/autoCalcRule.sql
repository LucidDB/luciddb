-- $Id$

explain plan for select jplus(1, 1) from (values(true));

explain plan for select cplus(1, 1) from (values(true));

explain plan for select cplus(1, 1), jplus(2, 2) from (values(true));

-- Dtbug 446: The plan should compute start in fennel, then go to iter
-- convention (because it needs to end up in iter convention).
explain plan for select jplus(2, 2), cplus(1, 1) from (values(true));

explain plan for select cplus(jplus(1, 1), 2) from (values(true));

-- Dtbug 446: This plan is sub-optimal -- there should be no
-- back-to-back FennelCalcRel's.
explain plan for select jplus(cplus(1, 1), 2) from (values(true));

explain plan for select empno, cplus(jplus(deptno, empid), age), jplus(cplus(deptno, empid), age), age from sales.emps;

explain plan for select * from sales.emps where jplus(deptno, 1) = 100;

-- Dtbug 446: This plan is sub-optimal -- there should be no
-- back-to-back FennelCalcRel's.
explain plan for select * from sales.emps where jplus(cplus(deptno, 1), 2) = 100;

explain plan for select * from sales.emps where cplus(jplus(deptno, 1), 2) = 100;

-- TODO zfong 4/28/06 - reenable the following two queries once FRG-90 is
-- resolved.  With the addition of the new PushProjectPastFilterRule, the
-- two queries below take even longer to run, so they have been temporarily
-- disabled.
--
-- explain plan for select cplus(jplus(deptno, 1), 2), jplus(cplus(deptno, 1), 2) from sales.emps where cplus(jplus(deptno, 1), 2) = 100 or jplus(cplus(deptno, 1), 2) = 100;
--
-- explain plan for select jplus(cplus(deptno, 1), 2), cplus(jplus(deptno, 1), 2) from sales.emps where cplus(jplus(deptno, 1), 2) = 100 or jplus(cplus(deptno, 1), 2) = 100;

explain plan for select jplus(cplus(deptno, 1), 2), cplus(jplus(deptno, 1), 2) from sales.emps where slacker;

-- Test top-most level can be implemented in any calc and last
-- expression isn't a RexCall.  dtbug 210
-- Dtbug 446: Plan should not have back-to-back FennelCalcRel's.
explain plan for select deptno + jplus(cplus(deptno, 1), 2), empno, city from sales.emps;

-- Equivalent to dtbug 210
explain plan for select cplus(t.r."second", 1) from (select jrow(deptno, empno) as r from sales.emps) as t;

-- Found a bug related to this expression while debugging dtbug 210.
-- Dtbug 446: Plan should not have back-to-back FennelCalcRel's.
explain plan for select t.r."second", city from (select jrow(deptno, cplus(empno, 1)) as r, city from sales.emps) as t;

# End autoCalcRule.sql
