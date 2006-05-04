
--
-- semi joins
--

set schema 's';
!set outputformat csv

-- a bunch of equi-joins
-- Workarounds added for LER-92 & LER-93, and filters added so semijoins happen
explain plan excluding attributes for
-- two way
select EMP.LNAME 
, EMP.EMPNO
from EMP, DEPT
where EMP.DEPTNO = DEPT.DEPTNO 
  and DEPT.DEPTNO > 100
--order by EMP.EMPNO;
order by EMPNO;

explain plan excluding attributes for
select DEPT.DNAME 
from EMP, DEPT
where EMP.DEPTNO = DEPT.DEPTNO 
  and DEPT.DEPTNO > 100
--order by DEPT.DNAME;
order by DNAME;

-- three way
EXPLAIN PLAN EXCLUDING ATTRIBUTES FOR
SELECT EMP.LNAME
from EMP, LOCATION, DEPT
where EMP.DEPTNO=DEPT.DEPTNO and DEPT.LOCID=LOCATION.LOCID
 and LOCATION.ZIP > 94000
--order by EMP.LNAME;
order by LNAME;

EXPLAIN PLAN EXCLUDING ATTRIBUTES FOR
SELECT DEPT.DNAME
from EMP, DEPT, LOCATION
where EMP.DEPTNO=DEPT.DEPTNO and DEPT.LOCID=LOCATION.LOCID
  and LOCATION.ZIP > 94000
--order by DEPT.DNAME;
order by DNAME;

EXPLAIN PLAN EXCLUDING ATTRIBUTES FOR
SELECT LOCATION.CITY
, LOCATION.STREET
from EMP, DEPT, LOCATION
where EMP.DEPTNO=DEPT.DEPTNO and DEPT.LOCID=LOCATION.LOCID
  and LOCATION.ZIP > 94000
--order by LOCATION.STREET;
order by STREET;

-- semi joins of a self join
explain plan excluding attributes for
select M.LNAME 
, M.EMPNO
from EMP M, EMP R
where M.EMPNO = R.MANAGER
 and R.SEX='M'
--order by M.EMPNO;
order by EMPNO;

explain plan excluding attributes for
select R.LNAME 
, R.EMPNO
from EMP M, EMP R
where M.EMPNO = R.MANAGER 
and R.SEX='M'
--order by R.EMPNO;
order by EMPNO;

-- double reference of a table
explain plan excluding attributes for
select EMP.LNAME, DEPT.DNAME
from LOCATION EL, LOCATION DL, EMP, DEPT
where EL.LOCID = EMP.LOCID and DL.LOCID=DEPT.LOCID
--order by EMP.LNAME, DEPT.DNAME;
order by LNAME, DNAME;

explain plan excluding attributes for
select EL.CITY, DL.CITY
from LOCATION EL, LOCATION DL, EMP, DEPT
where EL.LOCID = EMP.LOCID and DL.LOCID=DEPT.LOCID
--order by EL.CITY, DL.CITY;
order by CITY, CITY;

-- many to many self join semi join variations
explain plan excluding attributes for
select F.FNAME
FROM CUSTOMERS M, CUSTOMERS F
WHERE M.LNAME = F.LNAME
AND M.SEX = 'M'
AND F.SEX = 'F'
--order by F.FNAME;
order by FNAME;

explain plan excluding attributes for
select M.FNAME, M.LNAME
FROM CUSTOMERS M, CUSTOMERS F
WHERE M.LNAME = F.LNAME
AND M.SEX = 'M'
AND F.SEX = 'F'
--order by M.FNAME, M.LNAME;
order by FNAME, LNAME;

-- a few ranges
-- a big ol' join
explain plan excluding attributes for
select PRODUCTS.PRICE
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and SALES.PRODID = PRODUCTS.PRODID
order by  PRODUCTS.PRICE;

-- non join conditions
explain plan excluding attributes for
select SALES.CUSTID
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and ( PRODUCTS.NAME LIKE 'C%' OR PRODUCTS.NAME LIKE 'P%')
order by SALES.CUSTID;

-- equality and non equality in one
explain plan excluding attributes for
select SALES.PRICE
from SALES, PRODUCTS, CUSTOMERS
where SALES.PRICE - PRODUCTS.PRICE < 0.5
and PRODUCTS.PRICE - SALES.PRICE < 0.25
and SALES.CUSTID = CUSTOMERS.CUSTID
order by SALES.PRICE;

explain plan excluding attributes for
select PRODUCTS.NAME, CUSTOMERS.FNAME, CUSTOMERS.LNAME, PRODUCTS.PRICE
, CUSTOMERS.CUSTID
from SALES, PRODUCTS, CUSTOMERS
where SALES.PRICE - PRODUCTS.PRICE < 0.5
and PRODUCTS.PRICE - SALES.PRICE < 0.25
and SALES.CUSTID = CUSTOMERS.CUSTID
order by PRODUCTS.NAME, CUSTOMERS.CUSTID;

!set outputformat table