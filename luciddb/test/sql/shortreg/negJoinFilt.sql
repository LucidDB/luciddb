--
-- negJoinFilt.sql - join filter tests for queries where join Filter should
-- NOT be used
--

set schema 's';

-- a case where it's not worth it to join filter
explain plan for select * from emp,dept
where emp.deptno=dept.deptno and dept.dname<'Marketing';

-- this isn't worth it since the rows from emp are limited
explain plan for select * from emp,dept
where emp.deptno=dept.deptno
and dept.dname='Marketing'
and emp.empno=100;

-- this shouldn't do it because the filter condition's on the big table
explain plan for select * from emp,dept
where emp.deptno=dept.deptno and emp.fname='Bill';

-- this shouldn't do it because it's not an equi-join
explain plan for select * from emp,dept
where emp.deptno>dept.deptno and dept.dname='Marketing';

-- this shouldn't do it because the equi join is not accessible top level
explain plan for select * from emp,dept
where (emp.deptno=dept.deptno or dept.deptno > 5) and dept.dname='Marketing';

-- this shouldn't do it because the filter condition is not accessible top level
explain plan for select * from emp,dept
where emp.deptno=dept.deptno and (emp.fname='Bill' or dept.dname='Marketing');

-- this shouldn't do it since emp does not have an index on column hobby
explain plan for select * from emp,dept
where emp.hobby=dept.dname and dept.dname='Marketing';

-- pre filter should happen here, but we still need to join to location since
-- location.state is not unique
explain plan for select emp.* from emp,location
where emp.fname=location.state and location.locid='00';

-- can't handle non-simple equi join case
explain plan for select * from emp,dept
where emp.deptno+1=dept.deptno+1 and dept.dname='Marketing';
