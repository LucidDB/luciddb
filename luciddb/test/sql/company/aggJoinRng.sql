 
--
-- range joins with aggregation
--

set schema 's';

-- a big ol' join
select SALES.CUSTID, SALES.PRODID, sum(SALES.PRICE), floor(avg(PRODUCTS.PRICE))
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and SALES.PRODID = PRODUCTS.PRODID
group by SALES.CUSTID, SALES.PRODID
order by  SALES.PRODID, SALES.CUSTID;

-- non join conditions
select PRODUCTS.NAME, floor(avg(SALES.CUSTID))
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and ( PRODUCTS.NAME LIKE 'C%' OR PRODUCTS.NAME LIKE 'P%')
group by PRODUCTS.NAME
order by PRODUCTS.NAME;

-- semi join cases (as above, but not selecting certain columns)
select PRODUCTS.NAME
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and ( PRODUCTS.NAME LIKE 'C%' OR PRODUCTS.NAME LIKE 'P%')
group by PRODUCTS.NAME
order by PRODUCTS.NAME;

select floor(avg(SALES.CUSTID))
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and ( PRODUCTS.NAME LIKE 'C%' OR PRODUCTS.NAME LIKE 'P%')
group by PRODUCTS.NAME
order by PRODUCTS.NAME;

select 'HELLO'
from SALES, PRODUCTS
where SALES.PRICE between PRODUCTS.PRICE - 1 and PRODUCTS.PRICE + 1
and ( PRODUCTS.NAME LIKE 'C%' OR PRODUCTS.NAME LIKE 'P%')
group by PRODUCTS.NAME
order by PRODUCTS.NAME;

-- equality and non equality in one
select customers.custid, count(customers.custid), max(PRODUCTS.NAME),
min(CUSTOMERS.FNAME), max(CUSTOMERS.LNAME), sum(PRODUCTS.PRICE-SALES.PRICE)
from SALES, PRODUCTS, CUSTOMERS
where SALES.PRICE - PRODUCTS.PRICE < 0.5
and PRODUCTS.PRICE - SALES.PRICE < 0.5
and SALES.CUSTID = CUSTOMERS.CUSTID
group by CUSTOMERS.CUSTID
order by CUSTOMERS.CUSTID;

-- TODO: date ranges
-- select S1.CUSTID, S1.EMPNO, S1.TS, S2.CUSTID, S2.EMPNO, S2.TS,
-- from SALES S1, SALES S2
-- where S1.TS between S2.TS -1 and S2.TS +1
-- order by S1.CUSTID, S1.EMPNO, S1.TS, S2.CUSTID, S2.EMPNO, S2.TS;

