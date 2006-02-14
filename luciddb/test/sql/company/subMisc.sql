
--
-- sub query test for ANY, ALL
--

set schema 's';

-- basics

select LNAME from emp where DEPTNO > ANY (select DEPTNO from dept) order by 1;

select LNAME from emp where DEPTNO >= ALL (select DEPTNO-10 from dept) order by 1;

select LNAME from emp where DEPTNO = ANY
(select DEPTNO from dept where DEPTNO in (10,20)) order by 1;

select LNAME from emp where DEPTNO = ALL
(select DEPTNO from dept where DEPTNO in (10,20)) order by 1;

select LNAME from emp where DEPTNO = ALL
(select DEPTNO from dept where DEPTNO=10) order by 1;

-- null set returned from subquery

select LNAME from emp where DEPTNO = ALL
(select DEPTNO from dept where DEPTNO = -1) order by 1;

select LNAME from emp where DEPTNO = ANY
(select DEPTNO from dept where DEPTNO = -1) order by 1;

select LNAME from emp where DEPTNO > ANY
(select DEPTNO from dept where DEPTNO = -1) order by 1;

select LNAME from emp where DEPTNO <= ALL
(select DEPTNO from dept where DEPTNO = -1) order by 1;

-- constants (non correlated)

select LNAME from emp where 10 > ANY (select DEPTNO from DEPT) order by 1;

select LNAME from emp where 10 > ALL (select DEPTNO from DEPT) order by 1;

select LNAME from emp where 10 = ANY (select DEPTNO from DEPT) order by 1;

select LNAME from emp where 10 = ALL (select DEPTNO from DEPT) order by 1;


-- aggregation

select DEPTNO, count(*) from emp group by DEPTNO order by DEPTNO;

select LNAME from emp O
where 3 = ANY (select count(*) from emp I where I.DEPTNO=O.DEPTNO)
order by 1;

select LNAME from emp O
where 3 = ANY (select count(*) from emp I where I.DEPTNO>O.DEPTNO group by I.DEPTNO)
order by 1;

select LNAME from emp O
where 3 < ANY (select count(*) from emp I where I.DEPTNO=O.DEPTNO)
order by 1;

select LNAME from emp O
where 3 < ANY (select count(*) from emp I where I.DEPTNO>O.DEPTNO group by I.DEPTNO)
order by 1;

-- Hash-Semi-Join with Union 

select LNAME from emp where deptno in (select deptno from dept union all (select deptno from dept
union all (select deptno from dept union all (select deptno from dept union all (select deptno from dept)))))
order by 1;

-- Decrementing StageIds when Subquery has extra select list items that are not projected up the tree ( Bugid 2337 )

select LNAME from (select upper(LNAME) name1, LNAME from emp order by 1) order by 1;

-- Single-Row Select tests

-- Cause an error to be raised
select LNAME from emp where EMPNO = (select EMPNO from emp) order by 1;

select LNAME from emp where EMPNO > ( select EMPNO from emp) order by 1;

-- Normal cases
select LNAME from emp where EMPNO >= ( select max(EMPNO) from emp) order by 1;

select LNAME from emp where EMPNO = ( select max(EMPNO) from emp) order by 1;

select LNAME from emp O where EMPNO >= ( select EMPNO from emp I where I.EMPNO=O.EMPNO) order by 1;

select LNAME from emp O where EMPNO >= ( select EMPNO from emp I where 1=2) order by 1;

-- Test large number of columns in the HashSemiJoin (bugId 3792)

select * from TABLES minus select * from TABLES;
