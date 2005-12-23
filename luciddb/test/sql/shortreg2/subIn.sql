--
-- Sub query tests: IN
--

set schema 's';

-- Uncorrelated

select LNAME from emp
where deptno in (select deptno from dept where dname='Marketing')
order by 1;

select LNAME from emp
where deptno in (select deptno from dept where dname='Bogus')
order by 1;

select LNAME from emp
where deptno in (select deptno from dept)
order by 1;

select LNAME from emp
where deptno not in (select deptno from dept where dname='Marketing')
order by 1;

select LNAME from emp
where deptno not in (select deptno from dept where dname='Bogus')
order by 1;

select LNAME from emp
where deptno not in (select deptno from dept)
order by 1;

-- Correlated

select LNAME from emp
where deptno IN (select deptno from dept where dname='Marketing'
and dept.deptno=emp.deptno)
order by 1;

select LNAME from emp
where deptno in (select deptno from dept where LNAME<dname)
order by 1;
