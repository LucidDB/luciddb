
--
-- subMult.sql - tests with multiple subqueries in them
--

set schema 's';

-- multiple subqueries that resolve to HHJ

select * from EMP
where 
  EXISTS (select dept.deptno from dept where dept.deptno=emp.deptno
    and dept.dname in ('Marketing', 'Development', 'Sales') )
and
  deptno IN (select deptno from DEPT where DNAME > 'E')
order by empno;


-- multiple subqueries that resolve to XOEXISTS, calculator mode

select * from EMP
where 
  EXISTS (select dept.deptno from dept where dept.deptno=emp.deptno
    and dept.dname in ('Development', 'Sales') )
or
  (
    ( deptno IN (select deptno from DEPT where DNAME > 'E') )
    AND
    ( deptno IN (select deptno from DEPT where DNAME < 'J') )
  )
order by empno;

