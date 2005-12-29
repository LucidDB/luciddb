
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
