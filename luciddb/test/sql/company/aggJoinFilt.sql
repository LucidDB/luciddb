--
-- aggJoinFilt.sql - join Filter tests relating to aggregates
--

set schema 's';

--alter session set optimizerjoinfilterthreshold=2;
 
-- original, as per bug 22419
select *
from (
    select emp.deptno, emp.sex, sum(emp.sal) sum_sal from emp
    group by emp.deptno, emp.sex) emp,
  dept
where dept.deptno = emp.deptno
and dept.dname = 'Development'
order by sex;

-- as above, after pulling up aggregation
select emp.deptno, emp.sex, sum(emp.sal)
from (
        select emp.deptno, emp.sex, emp.sal from emp) emp,
  dept
where dept.deptno = emp.deptno
and dept.dname = 'Development'
group by emp.deptno, emp.sex
order by sex;

-- as above, after join filter
-- select emp.deptno, emp.sex, sum(emp.sal) sum_sal
-- from (
--   select emp.deptno, emp.sex, emp.sal from emp where emp.deptno in (
--     select deptno from dept where dname = 'Development')) emp,
--   dept
-- where dept.deptno = emp.deptno
-- and dept.dname = 'Development'
-- group by emp.deptno, emp.sex;

-- -- as above, after push down
-- select emp.deptno, emp.sex, emp.sum_sal
-- from (
--   select emp.deptno, emp.sex, sum(emp.sal) sum_sal from emp 
--   where emp.deptno in (
--     select deptno from dept where dname = 'Development')
--   group by emp.deptno, emp.sex)
--   emp,
--   dept
-- where dept.deptno = emp.deptno
-- and dept.dname = 'Development';

-- Bug 22419, inputs to join reversed
select *
from dept
join (
   select emp.deptno, emp.sex, sum(emp.sal) sum_sal from emp
   group by emp.deptno, emp.sex) emp
on dept.deptno = emp.deptno
where dept.dname = 'Development'
order by sex;

-- Left-outer join.
select *
from (
   select emp.deptno, emp.sex, sum(emp.sal) sum_sal from emp
   group by emp.deptno, emp.sex) emp
left join dept
on dept.deptno = emp.deptno
where dept.dname = 'Development'
order by sex;

-- Right-outer join.
select *
from (
   select emp.deptno, emp.sex, sum(emp.sal) sum_sal from emp
   group by emp.deptno, emp.sex) emp
right join dept
on dept.deptno = emp.deptno
where dept.dname = 'Development'
order by 2;

-- Full-outer join.
select *
from (
   select emp.deptno, emp.sex, sum(emp.sal) sum_sal from emp
   group by emp.deptno, emp.sex) emp
full join dept
on dept.deptno = emp.deptno
where dept.dname = 'Development'
order by 2;

-- Pull-up aggregation should work through a projection (sum(emp.sal)
-- + 1).
select *
from (
   select emp.deptno, emp.sex, sum(emp.sal) + 1 sum_sal from emp
   group by emp.deptno, emp.sex) emp
join dept
on dept.deptno = emp.deptno
where dept.dname = 'Development'
order by sex;

-- Pull-up aggregation could work through a filter (having count(*) >
-- 1), but doesn't yet.
select *
from (
   select emp.deptno, emp.sex, sum(emp.sal) + 1 sum_sal from emp
   group by emp.deptno, emp.sex
   having count(*) > 1) emp
join dept
on dept.deptno = emp.deptno
where dept.dname = 'Development'
order by 2;

-- Don't try to pull-up aggregates if join is based upon summary
-- columns (count_emp).
select *
from (
   select emp.deptno, emp.sex, count(*) as count_emp, sum(emp.sal) as sum_sal
   from emp
   group by emp.deptno, emp.sex) as emp
join dept
on dept.deptno = emp.count_emp
where dept.dname = 'Development';

-- end aggJoinFilt.sql
