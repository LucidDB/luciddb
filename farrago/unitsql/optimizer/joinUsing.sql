-- $Id$
-- Testing the USING clause of the JOIN statement; also CROSS JOIN and
-- NATURAL JOIN.

set schema 'sales';
!set outputformat csv

-- USING is syntactic candy for an ON condition.  Plan and output for the two
-- queries should be the same.

explain plan for select * from emps join depts on emps.deptno = depts.deptno;
explain plan for select * from emps join depts using (deptno);


select * from emps join depts on emps.deptno = depts.deptno;
select * from emps join depts using (deptno);

-- NATURAL JOIN is equivalent to USING(deptno,name) and gives empty result
select * from emps natural join depts;
-- Rename NAME and it's equivalent to USING(deptno)
select * from emps natural join (select deptno, name as dname from depts);
-- Rename DEPTNO too and it's equivalent to CROSS JOIN
select * from emps natural join (select deptno, name as dname from depts);
-- CROSS JOIN
select * from emps cross join depts;
-- End joinUsing.sql

