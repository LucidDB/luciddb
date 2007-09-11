-- $Id$
-- Testing the USING clause of the JOIN statement.

set schema 'sales';
!set outputformat csv

-- USING is syntactic candy for an ON condition.  Plan and output for the two
-- queries should be the same.

explain plan for select * from emps join depts on emps.deptno = depts.deptno;
explain plan for select * from emps join depts using (deptno);


select * from emps join depts on emps.deptno = depts.deptno;
select * from emps join depts using (deptno);

-- End joinUsing.sql

