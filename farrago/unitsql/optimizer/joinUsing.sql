-- $Id$
-- Testing the USING clause of the JOIN statement; also CROSS JOIN and
-- NATURAL JOIN.

set schema 'sales';
!set outputformat csv

-- USING is syntactic candy for an ON condition.  Plan and output for the two
-- queries should be the same.

explain plan for select * from emps join depts on emps.deptno = depts.deptno;
explain plan for select * from emps join depts using (deptno);


select * from emps join depts on emps.deptno = depts.deptno
order by empno;
select * from emps join depts using (deptno)
order by empno;

-- NATURAL JOIN is equivalent to USING(deptno,name) and gives empty result
select * from emps natural join depts;
-- Rename NAME and it's equivalent to USING(deptno)
select * from emps natural join (select deptno, name as dname from depts)
order by empno;
-- Rename DEPTNO too and it's equivalent to CROSS JOIN
select * from emps natural join (select deptno, name as dname from depts)
order by empno;
-- CROSS JOIN
select * from emps cross join depts
order by empno,deptno;
-- End joinUsing.sql

