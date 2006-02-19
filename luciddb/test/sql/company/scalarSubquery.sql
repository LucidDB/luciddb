--
-- company/scalarSubquery.sql: scalar subqueries
--

set schema 's';

-- in select list
-- uncorrelated
select (select 1 from ONEROW) from DEPT order by DEPTNO;
-- correlated
select LNAME, (select DNAME from DEPT where DEPTNO = EMP.DEPTNO)
 from EMP order by EMPNO;
-- correlating variable in select list
select LNAME, (select EMPNO from ONEROW) from EMP order by EMPNO;
-- expression on correlating variable in select list
select LNAME, (select EMPNO + 5 from ONEROW) from EMP order by EMPNO;
-- agg in subquery
select DNAME, (select min(SAL) from EMP where DEPTNO = DEPT.DEPTNO)
 from DEPT order by DEPTNO;
-- non equi-join
select DNAME,
 (select COUNT(distinct LOCID) from EMP where DEPTNO < DEPT.DEPTNO)
 from DEPT order by DEPTNO;
-- subquery returns more than one row (gives error)
select DNAME, (select EMPNO from EMP where DEPTNO = DEPT.DEPTNO)
 from DEPT order by DEPTNO;
-- ditto, uncorrelated (gives error)
select DNAME, (select EMPNO from EMP)
 from DEPT order by DEPTNO;
-- subquery returns 0 rows, correlated
select DNAME, (select EMPNO from EMP
               where DEPTNO = DEPT.DEPTNO and FNAME = 'Frank')
 from DEPT order by DEPTNO;
-- subquery returns 0 rows, uncorrelated
select DNAME, (select EMPNO from EMP where FNAME = 'ZEBEDEE')
 from DEPT order by DEPTNO;
-- scalar subquery in expression
select FNAME || ' ' || LNAME || ' works in ' ||
       (select DNAME from DEPT where DEPTNO = EMP.DEPTNO) from EMP
where FNAME like 'F%' order by EMPNO;
-- scalar subquery in agg
select PRODUCTS.NAME, MIN((select LNAME from EMP where EMPNO = SALES.EMPNO))
 from SALES join PRODUCTS on SALES.PRODID = PRODUCTS.PRODID
 group by PRODUCTS.NAME
 order by LOWER(PRODUCTS.NAME);
-- scalar subquery with agg in agg
select DEPTNO, MIN((select COUNT(distinct PRODID) from SALES
                    where EMPNO = EMP.EMPNO))
 from EMP group by DEPTNO order by DEPTNO;
-- distinct in agg, uncorrelated
select DEPTNO, (select distinct 5 from EMP) from DEPT order by DEPTNO;

-- in where
-- uncorrelated
select * from EMP where EMPNO = (select min(EMPNO) from EMP);
-- correlated
select * from EMP E1 where EMPNO = (
  select MIN(EMPNO) from EMP where LNAME = (
    select MIN(LNAME) from DEPT where DEPTNO = E1.DEPTNO));
-- scalar subquery inside IN subquery
select * from EMP E1 where EMPNO in (
  select EMPNO from EMP where SAL = (
    select MIN(SAL) from DEPT where DEPTNO = E1.DEPTNO))
order by EMPNO;
-- scalar subquery on both sides of =
select * from DEPT
 where (select MIN(SAL + EMPNO) from EMP where DEPTNO = DEPT.DEPTNO)
     < (select MAX(SAL + EMPNO) from EMP where DEPTNO = DEPT.DEPTNO)
order by *;
-- with EXISTS
select * from DEPT
 where (select MIN(SAL + EMPNO) from EMP where DEPTNO = DEPT.DEPTNO)
     < (select MAX(SAL + EMPNO) from EMP where DEPTNO = DEPT.DEPTNO)
    or exists (select null from SALES, EMP
               where SALES.EMPNO = EMP.EMPNO
               and EMP.DEPTNO = DEPT.DEPTNO
               having SUM(PRICE) > 420)
    or exists (select null from SALES, EMP
               where SALES.EMPNO = EMP.EMPNO
               and EMP.DEPTNO = DEPT.DEPTNO
               and EMP.SEX = 'M'
               having COUNT(*) > 90)
order by *;
--
select EMP.DEPTNO, EMP.LNAME
from EMP right join DEPT on EMP.DEPTNO = DEPT.DEPTNO
where DNAME like 'S%' or (
  select FNAME from EMP E1 where EMPNO = (
    select MIN(EMPNO) from EMP
    where EMP.DEPTNO = DEPT.DEPTNO and EMP.DEPTNO = E1.DEPTNO)) like 'F%'
order by *;

-- in group by
select (
  select MIN(SEX) from EMP where DEPTNO = DEPT.DEPTNO and LOCID = DEPT.LOCID),
  COUNT(*)
from DEPT
group by (
  select MIN(SEX) from EMP where DEPTNO = DEPT.DEPTNO and LOCID = DEPT.LOCID)
order by *;
--
select distinct (select COUNT(*) from EMP where DEPTNO = DEPT.DEPTNO)
from DEPT order by *;
--
select DEPTNO, (select MIN(SAL) from EMP E2 where E2.DEPTNO = E1.DEPTNO)
 from EMP E1
group by DEPTNO order by *;
--
select DEPTNO
from EMP E1
group by DEPTNO
order by (select count(FNAME) from EMP E2 where E2.DEPTNO = E1.DEPTNO), DEPTNO;
--
select DEPTNO,
 MIN((select MIN(SEX) from EMP E2 where E2.MANAGER = E1.EMPNO)),
 SUM((select COUNT(*) from EMP E2 where E2.MANAGER = E1.EMPNO))
from EMP E1
group by DEPTNO
order by DEPTNO;
--
select DEPTNO,
 SUM(case when exists (
      select 1 from EMP E2 where E2.DEPTNO = E1.DEPTNO and E2.EMPNO < E1.EMPNO)
     then 1
     else 0
     end),
 COUNT(*)
from EMP E1
group by DEPTNO
order by DEPTNO;
--

-- in having
-- uncorrelated
select COUNT(*) from EMP group by DEPTNO
 having (select INTCOL from ONEROW where INTCOL = 5) is null
 order by *;
-- error: uncorrelated non-group sex
select COUNT(*) from EMP group by DEPTNO having exists (
 select 1 from ONEROW where SEX = 'F') order by deptno;
--
select EMP.DEPTNO, COUNT(*)
from EMP right join DEPT on EMP.DEPTNO = DEPT.DEPTNO
group by EMP.DEPTNO, DNAME
having DNAME like 'S%'
 or COUNT(DNAME) < 3
 or (
  select FNAME from EMP E1 where 1 = 0 and EMPNO = (
    select MIN(EMPNO) from EMP E2
    where E2.DEPTNO = E1.DEPTNO and EMP.DEPTNO = E1.DEPTNO)) like 'F%'
order by 1;

-- in order by
select * from DEPT
order by
 (select 1 from ONEROW),
 (select MIN(EMPNO) from EMP) asc,
 (select COUNT(DISTINCT sex) from EMP where DEPTNO = DEPT.DEPTNO) desc,
 -(select MAX(EMPNO) from EMP where DEPTNO = DEPT.DEPTNO) desc;
-- same thing
select DEPTNO,
 (select 1 from ONEROW),
 (select MIN(EMPNO) from EMP),
 (select COUNT(DISTINCT sex) from EMP where DEPTNO = DEPT.DEPTNO),
 -(select MAX(EMPNO) from EMP where DEPTNO = DEPT.DEPTNO)
from DEPT order by 2, 3, 4 desc, 5 desc;

-- subqueries with setops
-- uncorrelated
select DEPTNO, (select 1 from ONEROW intersect
                select intcol from ONEROW minus
                (select 2 from ONEROW union
                 select 3 from ONEROW))
from DEPT order by *;
-- correlated
select FNAME, ((select DEPTNO - 1 from DEPT intersect
                select DEPTNO - 1 from EMP where DEPTNO = E1.DEPTNO) + 2)
from EMP E1 order by FNAME;
-- fails, returns more than one row
select (select 1 from ONEROW union all select INTCOL from ONEROW) from ONEROW;

-- bug 6863: UPDATE using scalar select in SET statement fails
create table bug6863 (x integer);
insert into bug6863 values (1);
update bug6863 set x = (select 2 from onerow);
select * from bug6863;
-- similar: or exists fails in const-reduction
select intcol from onerow
 where exists (select 1 from onerow)
    or exists (select 1 from onerow);

-- bug 7191: agg in exists in select list doesn't make a agg query
select intcol,
 case when exists (select min(intcol) from onerow) then 6 else 0 end
from onerow;

-- End scalarSubquery.sql
