
--
-- agg.sql -- test aggregation
--

set schema 's';

-- plain aggregates

-- TODO:
-- FNL-42
select floor(sum(PRICE)) from SALES;

select count(PRICE) from SALES;

select floor(avg(PRICE)) from SALES;

--select floor(to_number(min(PRICE))), floor(to_number(max(PRICE))) from SALES;

-- with group by

select PRODID, floor(sum(PRICE)) from SALES group by PRODID order by PRODID;

select count(PRICE)
, PRODID
from SALES group by PRODID order by PRODID;

select floor(avg(PRICE)), PRODID, floor(avg(PRICE)) from SALES group by PRODID order by PRODID;

--select PRODID, to_number(min(PRICE)), to_number(max(PRICE)) from SALES group by PRODID order by PRODID;

-- having

-- TODO: FRG-115
select sum(PRICE)
, PRODID
from SALES group by PRODID having PRODID < 10010 order by PRODID;

select count(PRICE)
, PRODID
from SALES group by PRODID having PRODID > 10010 and PRODID/2<5007 order by PRODID;
-- END FRG-115

-- TODO: FRG-165
select floor(avg(PRICE))
, PRODID
from SALES group by PRODID having PRODID in (10005, 10007, 10009) order by PRODID;

--select to_number(min(PRICE)), to_number(max(PRICE)), PRODID from SALES group by PRODID having PRODID between 10005 and 10010 order by PRODID;


-- where clauses restricting the rows aggregated

select sum(PRICE)
, PRODID 
from SALES  where PRODID in (10005, 10010) group by PRODID order by PRODID;

select count(PRICE)
, PRODID
from SALES where PRICE+0 < 5.00 group by PRODID order by PRODID;

select floor(avg(PRICE))
, PRODID
from SALES where EMPNO < 100 or EMPNO > 107 group by PRODID order by PRODID;

--select to_number(min(PRICE)), to_number(max(PRICE)) from SALES where EMPNO*2 between 204 and 212 group by PRODID order by PRODID;

-- having and where clauses

-- TODO: FRG-115
select sum(PRICE)
, PRODID
from SALES
where custid>50
group by PRODID having PRODID < 10010
order by PRODID;

select count(PRICE)
, PRODID
from SALES
where custid>50
group by PRODID having PRODID > 10010 and PRODID/2<5007
order by PRODID;
-- END FRG-115

-- TODO: FRG-165
select floor(avg(PRICE))
, PRODID
from SALES
where custid>50
group by PRODID
having PRODID in (10005, 10007, 10009)
order by PRODID;

-- select to_number(min(PRICE)), to_number(max(PRICE)), PRODID from SALES
-- where custid>50
-- group by PRODID
-- having PRODID between 10005 and 10010
-- order by PRODID;

-- multiple group by's

select min(FNAME), LNAME, SEX from CUSTOMERS group by LNAME, SEX order by LNAME, SEX;

select FNAME, LNAME, count(SEX) from CUSTOMERS group by FNAME, LNAME order by LNAME, FNAME;

select FNAME, LNAME, SEX from CUSTOMERS group by FNAME, LNAME, SEX order by LNAME, SEX, FNAME;

-- expressions

select sum(PRICE), PRODID * 2 from PRODUCTS group by PRODID order by 2;

select sum(PRICE+3), count(PRODID/2) from PRODUCTS;


-- text columns

select min(LNAME), max(LNAME) from EMP;
select DEPTNO, min(LNAME), max(LNAME) from EMP group by DEPTNO order by DEPTNO;
