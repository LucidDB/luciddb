--
-- order.sql - test order by
--

set schema 's';

-- simple
select EMPNO from EMP order by EMPNO;

-- multiple cols
select LNAME, EMPNO from EMP order by LNAME,SEX;
select LNAME, EMPNO from EMP order by SEX,LNAME;
select LNAME, SEX, EMPNO from EMP order by SEX,EMPNO,LNAME;

-- ASC/DESC
select LNAME, EMPNO from EMP order by LNAME ASC , LOCID;
select LNAME, EMPNO from EMP order by MANAGER,LOCID DESC;
select LNAME, SEX, EMPNO from EMP order by MANAGER DESC,LNAME DESC;

-- numeric descriptors of columns in select list
select LNAME, SEX from EMP order by 1,2;
select LNAME, SEX from EMP order by 2,1;
select MANAGER, SEX,LNAME from EMP order by 1,2,3;
select MANAGER, SEX,LNAME from EMP order by 2,1,3;
select MANAGER, SEX,LNAME from EMP order by 2,1,3;

-- nulls
select lname, commission from EMP order by 1,2;
select lname, commission from EMP order by 2,1;

-- constants and expressions in order by
select empno+3, lname, commission from EMP order by 1;
select 'hello', lname, commission from EMP order by 1,2;
select 'hello', lname, commission from EMP order by 2,1;
select 'hello', lname, commission from EMP order by 1,lname;
select 'hello', lname, commission from EMP order by lname,1;

select empno, floor(empno/2),
  case when empno<105 then empno
    else empno/2 end
from emp
order by 3 DESC, 1 ASC
/
