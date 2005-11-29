
--
-- functions, miscellaneous
--

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
