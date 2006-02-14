--
-- Sub query in from list tests
--

set schema 's';

select LNAME, FNAME, DEPTNO from (select * from emp) bob order by 1,2;

select LNAME, FNAME, DEPTNO from (select DEPTNO,LNAME,FNAME from emp) bert order by 1,2;

select EEE, DDD from (select DEPTNO DDD, LNAME, FNAME EEE from emp) ernie order by 1,2;

select LNAME,FNAME, dname
from (select LNAME,FNAME, DEPTNO from emp) EMP,
  (select dname,deptno from dept) DEPT
where EMP.DEPTNO = DEPT.DEPTNO
order by 1,3,2;

select LNAME,FNAME, dname
from (select LNAME,FNAME, DEPTNO DDD from emp) E,
  (select dname,deptno from dept) D
where E.DDD = D.DEPTNO
order by 3,2,1;

-- sub query in from list, with aggregation

select C,D from (select count(*) C, DEPTNO D from emp group by DEPTNO) bart order by 1,2;

select count(*) from (select * from emp) frank;

select DEPTNO, count(*) from (select * from emp) john group by DEPTNO order by 1;

select count(*) from (select count(*) C, DEPTNO D from emp group by DEPTNO) fred;
