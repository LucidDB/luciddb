
--
-- functions, miscellaneous
--

set schema 's';

select EMP.EMPNO, EMP.LNAME, DEPT.DNAME
from EMP, DEPT
where EMP.DEPTNO + 10 = DEPT.DEPTNO - 10
order by EMPNO;

select EMP.EMPNO, EMP.LNAME, DEPT.DNAME
from EMP, DEPT
where substring(EMP.LNAME,2,1)=substring(DEPT.DNAME,2,1)
order by EMPNO, DNAME;

-- one side functions only

select EMP.EMPNO, EMP.LNAME, DEPT.DNAME
from EMP, DEPT
where EMP.EMPNO * 2 / 5 > DEPT.DEPTNO
order by EMPNO, DNAME;

-- full cartesian product
select EMP.EMPNO, DEPT.DEPTNO, EMP.LNAME, DEPT.DNAME from EMP,DEPT
order by EMPNO, DEPTNO;

select DEPT.DEPTNO, LOCATION.LOCID, DEPT.DNAME, LOCATION.CITY from DEPT, LOCATION
order by DEPTNO, LOCID;
