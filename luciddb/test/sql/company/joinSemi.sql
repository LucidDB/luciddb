
--
-- semi joins
--

set schema 's';

-- a bunch of equi-joins
-- Workarounds added for LER-92 & LER-93, and filters added so semijoins happen

-- two way
select EMP.LNAME 
, EMP.EMPNO
from EMP, DEPT
where EMP.DEPTNO = DEPT.DEPTNO 
  and DEPT.DEPTNO > 100
order by EMP.EMPNO;
--order by EMPNO;

select DEPT.DNAME 
from EMP, DEPT
where EMP.DEPTNO = DEPT.DEPTNO 
  and DEPT.DEPTNO > 100
order by DEPT.DNAME;
--order by DNAME;

-- three way
SELECT EMP.LNAME
from EMP, LOCATION, DEPT
where EMP.DEPTNO=DEPT.DEPTNO and DEPT.LOCID=LOCATION.LOCID
 and LOCATION.ZIP > 94000
order by EMP.LNAME;
--order by LNAME;

SELECT DEPT.DNAME
from EMP, DEPT, LOCATION
where EMP.DEPTNO=DEPT.DEPTNO and DEPT.LOCID=LOCATION.LOCID
  and LOCATION.ZIP > 94000
order by DEPT.DNAME;
--order by DNAME;

SELECT LOCATION.CITY
, LOCATION.STREET
from EMP, DEPT, LOCATION
where EMP.DEPTNO=DEPT.DEPTNO and DEPT.LOCID=LOCATION.LOCID
  and LOCATION.ZIP > 94000
order by LOCATION.STREET;
--order by STREET;

-- semi joins of a self join
select M.LNAME 
, M.EMPNO
from EMP M, EMP R
where M.EMPNO = R.MANAGER
 and R.SEX='M'
order by M.EMPNO;
--order by EMPNO;

select R.LNAME 
, R.EMPNO
from EMP M, EMP R
where M.EMPNO = R.MANAGER 
and R.SEX='M'
order by R.EMPNO;
--order by EMPNO;

-- double reference of a table
select EMP.LNAME, DEPT.DNAME
from LOCATION EL, LOCATION DL, EMP, DEPT
where EL.LOCID = EMP.LOCID and DL.LOCID=DEPT.LOCID
order by EMP.LNAME, DEPT.DNAME;
--order by LNAME, DNAME;

select EL.CITY, DL.CITY
from LOCATION EL, LOCATION DL, EMP, DEPT
where EL.LOCID = EMP.LOCID and DL.LOCID=DEPT.LOCID
order by 1, 2;
--order by CITY, CITY;

-- many to many self join semi join variations
select F.FNAME
FROM CUSTOMERS M, CUSTOMERS F
WHERE M.LNAME = F.LNAME
AND M.SEX = 'M'
AND F.SEX = 'F'
order by F.FNAME;
--order by FNAME;

select M.FNAME, M.LNAME
FROM CUSTOMERS M, CUSTOMERS F
WHERE M.LNAME = F.LNAME
AND M.SEX = 'M'
AND F.SEX = 'F'
order by M.FNAME, M.LNAME;
--order by FNAME, LNAME;

-- ** SLOW, add in a bit
-- a few ranges
-- a big ol' join
--select PRODUCTS.PRICE
--from SALES, PRODUCTS
--where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
--and SALES.PRODID = PRODUCTS.PRODID
--order by  PRODUCTS.PRICE;

-- non join conditions
--select SALES.CUSTID
--from SALES, PRODUCTS
--where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
--and ( PRODUCTS.NAME LIKE 'C%' OR PRODUCTS.NAME LIKE 'P%')
--order by SALES.CUSTID;

-- equality and non equality in one
-- select SALES.PRICE
-- from SALES, PRODUCTS, CUSTOMERS
-- where SALES.PRICE - PRODUCTS.PRICE < 0.5
-- and PRODUCTS.PRICE - SALES.PRICE < 0.25
-- and SALES.CUSTID = CUSTOMERS.CUSTID
-- order by SALES.PRICE;

-- select PRODUCTS.NAME, CUSTOMERS.FNAME, CUSTOMERS.LNAME, PRODUCTS.PRICE
-- from SALES, PRODUCTS, CUSTOMERS
-- where SALES.PRICE - PRODUCTS.PRICE < 0.5
-- and PRODUCTS.PRICE - SALES.PRICE < 0.25
-- and SALES.CUSTID = CUSTOMERS.CUSTID
-- order by PRODUCTS.NAME, CUSTOMERS.CUSTID;
