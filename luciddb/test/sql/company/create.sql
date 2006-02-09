
-- create tablespace bspace datafile 'bill.dat' size 12800K
-- ;

-- EMP: employees in the company

create schema s;
set schema 's';

create table EMP (
  EMPNO numeric(5,0),
-- integer,
  FNAME varchar(20),
  LNAME varchar(20),
  SEX char(1),
  DEPTNO integer,
  MANAGER numeric(5,0),
-- integer,
  LOCID CHAR(2),
  SAL integer,
  COMMISSION integer,
  HOBBY varchar(20)
) 
-- tablespace bspace
;

-- DEPT: Departments in the company
create table DEPT (
  DEPTNO integer,
  DNAME varchar(20),
  LOCID CHAR(2)
) 
-- tablespace bspace
;

create table LOCATION(
  LOCID char(2),
  STREET varchar(50),
  CITY varchar(20),
  STATE char(2),
  ZIP numeric(5,0)
) 
-- tablespace bspace
;

create table CUSTOMERS(CUSTID integer, FNAME varchar(30), LNAME varchar(30),
  SEX char(1)) 
-- tablespace bspace
;

create table PRODUCTS(PRODID integer, NAME varchar(30), PRICE numeric(3,2))
-- tablespace bspace
;

create table SALES(
-- id number of the customer
  CUSTID integer,
-- employee that made the sale
  EMPNO integer,
-- time of sale (uncomment when dates work); 
-- note: TS is not in the tables, so leaving it commented out
--  TS timestamp,
-- product id
  PRODID integer,
-- possible discount price we sold it for
  PRICE numeric(3,2)
) 
-- tablespace bspace
;
