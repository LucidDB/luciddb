
--
-- functions, miscellaneous
--

set schema 's';

select EMP.LNAME, DEPT.DNAME
from EMP, DEPT
where EMP.DEPTNO + 10 = DEPT.DEPTNO - 10
order by EMP.EMPNO;

select EMP.LNAME, DEPT.DNAME
from EMP, DEPT
where substring(EMP.LNAME,2,1)=substring(DEPT.DNAME,2,1)
order by EMP.EMPNO, DEPT.DNAME;

-- one side functions only

select EMP.LNAME, DEPT.DNAME
from EMP, DEPT
where EMP.EMPNO * 2 / 5 > DEPT.DEPTNO
order by EMP.EMPNO, DEPT.DNAME;

-- full cartesian product
select EMP.LNAME, DEPT.DNAME from EMP,DEPT
order by EMP.EMPNO, DEPT.DEPTNO;

select DEPT.DNAME, LOCATION.CITY from DEPT, LOCATION
order by DEPT.DEPTNO, LOCATION.LOCID;

-- Equjoin Condition plus non-equjoin condition on the same tables with group by ( Bugid 2796 )
select P.DEPTNO, COUNT(*) from 
(select DEPTNO, AVG(EMPNO) AVGAMT from
EMP group by DEPTNO) A,
EMP P
where P.DEPTNO = A.DEPTNO
and EMPNO < AVGAMT
group by P.DEPTNO 
order by P.DEPTNO;
   
-- Equjoin Condition plus non-equjoin condition on the same tables with group by and non-staged Item in subquery
select P.DEPTNO, COUNT(*) from
(select DEPTNO, INC() , AVG(EMPNO) AVGAMT from
EMP group by DEPTNO) A,
EMP P
where P.DEPTNO = A.DEPTNO
and EMPNO < AVGAMT
group by P.DEPTNO
order by P.DEPTNO;

