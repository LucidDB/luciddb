
--
-- Equi-joins
--

set schema 's';

-- standard emp/dept
select EMP.EMPNO, EMP.LNAME, EMP.DEPTNO, DEPT.DEPTNO, DEPT.DNAME
from EMP, DEPT
where EMP.DEPTNO = DEPT.DEPTNO order by EMPNO;

-- this one used to fail in calculator due to column mapping stuff
select EMP.EMPNO, EMP.DEPTNO, DEPT.DNAME from EMP, DEPT
where EMP.DEPTNO = DEPT.DEPTNO order by EMPNO;

-- dept, loc
select DEPTNO, DNAME, CITY, LOCATION.LOCID from DEPT, LOCATION
where DEPT.LOCID=LOCATION.LOCID
order by DEPTNO, LOCID;
