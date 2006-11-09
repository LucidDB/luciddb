--
-- Sub query tests: Exists
--

set schema 's';

-- Uncorrelated
select LNAME from emp
where EXISTS (select deptno from dept where dname='Marketing')
order by 1;

select LNAME from emp
where EXISTS (select deptno from dept where dname='Bogus')
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname='Marketing')
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname='Bogus')
order by 1;

-- Correlated
select LNAME from emp
where EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno=emp.deptno)
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno=emp.deptno)
order by 1;

select LNAME from emp
where EXISTS (select deptno from dept where dname in ('Marketing','Development')
  and dept.deptno=emp.deptno)
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname in ('Marketing','Development')
  and dept.deptno=emp.deptno)
order by 1;

-- non equality conditions

select LNAME from emp
where EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno>emp.deptno)
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno>=emp.deptno)
order by 1;

select LNAME from emp
where EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno<emp.deptno)
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno<=emp.deptno)
order by 1;

select LNAME from emp
where EXISTS (select locid from dept where dname='Marketing'
  and dept.deptno+10 < emp.deptno - 10)
order by 1;

select LNAME from emp
where NOT EXISTS (select deptno from dept where dname='Marketing'
  and dept.deptno+10>=emp.deptno-10)
order by 1;


-- multi variable correlation

select * from EMP O
where EXISTS
  (select * from EMP I
  where I.EMPNO=O.EMPNO+1
  and I.SEX=O.SEX)
order by 1;

select * from EMP O
where EXISTS
  (select * from EMP I
  where I.EMPNO>O.EMPNO
  and I.SEX=O.SEX)
order by 1;

select * from EMP O
where EXISTS
  (select * from EMP I
  where I.EMPNO>O.EMPNO
  and I.SEX<O.SEX)
order by 1;

select * from EMP O
where EXISTS
  (select * from EMP I
  where I.EMPNO>O.EMPNO
  and I.SEX<>O.SEX)
order by 1;

select * from EMP O
where NOT EXISTS
  (select * from EMP I
  where I.EMPNO>O.EMPNO
  and I.SEX<O.SEX)
order by 1;

select * from EMP O
where NOT EXISTS
  (select * from EMP I
  where I.EMPNO>O.EMPNO
  and I.SEX<>O.SEX)
order by 1;

-- multi variable correlation with aggregate on top

select count(*),sum(SAL) from EMP O
where EXISTS
  (select * from EMP I
  where I.EMPNO=O.EMPNO+1
  and I.SEX=O.SEX);

select count(*),sum(SAL) from EMP O
where EXISTS
  (select * from EMP I
  where I.EMPNO=O.EMPNO+1
  and I.SEX=O.SEX) group by deptno order by 1;

-- Not Exists with an EquJoin under the Exists part
select deptno from emp
where not exists (
  select 1 from dept d1, dept d2
  where d1.deptno = d2.deptno and d1.deptno = emp.deptno);

-- Exists with an OR condition on outer Query Table ( Bugid 3086 )
select deptno from emp
where exists (
  select * from dept ) or emp.deptno > 10
order by 1;
