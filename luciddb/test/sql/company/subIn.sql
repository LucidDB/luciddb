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

-- Fix problem with context of nested In Rewrites ( Bugid 1859 )

select LNAME from emp where deptno in (select deptno from dept where deptno in (select deptno from dept where
LNAME < dname group by deptno) and LNAME<dname group by deptno) order by 1;

-- Fix problem with NOT ALL transformation collapsing two NOTs ( Bugid 1866 )
-- ALL not supported
-- select LNAME from emp where deptno in (select deptno from dept) and not (deptno !=ALL (select deptno from dept )) 
-- order by 1;

-- Fix problem with counting results of NOT IN  ( Bugid 2259)

select count(*) from emp
where deptno not in (select deptno from dept where dname='Marketing');

-- Add a test to test null in the in list
create table t_null(i integer);

insert into t_null values(5);
insert into t_null values(null);

select * from t_null where i in (select i from t_null); 

drop table t_null;
