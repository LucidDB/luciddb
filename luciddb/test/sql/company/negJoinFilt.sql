--
-- negJoinFilt.sql - join filter tests for queries where join Filter should
-- NOT be used
--

set schema 's';

--alter session set optimizerjoinfilterthreshold=4;

-- a case where it's not worth it to join filter
select * from emp,dept
where emp.deptno=dept.deptno and dept.dname>'A'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- this isn't worth it since the rows from emp are limited
select * from emp,dept
where emp.deptno=dept.deptno
and dept.dname='Marketing'
and emp.empno=100
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- this shouldn't do it because the filter condition's on the big table
select * from emp,dept
where emp.deptno=dept.deptno and emp.fname>'Bill'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- this shouldn't do it because it's not an equi-join
select * from emp,dept
where emp.deptno>dept.deptno and dept.dname='Marketing'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- this shouldn't do it because the equi join is not accessible top level
select * from emp,dept
where (emp.deptno=dept.deptno or dept.deptno > 5) and dept.dname='Marketing'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- this shouldn't do it because the filter condition is not accessible top level
select * from emp,dept
where emp.deptno=dept.deptno and (emp.fname='Bill' or dept.dname='Marketing')
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- this shouldn't do it since emp does not have an index on column hobby
select * from emp,dept
where emp.hobby=dept.dname and dept.dname='Marketing'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;

-- pre filter should happen here, but we still need to join to location since
-- location.state is not unique
select emp.* from emp,location
where emp.fname=location.state and location.locid='00'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10;

-- can't handle non-simple equi join case
select * from emp,dept
where emp.deptno+1=dept.deptno+1 and dept.dname='Marketing'
--order by *;
order by 1,2,3,4,5,6,7,8,9,10,11,12,13;
