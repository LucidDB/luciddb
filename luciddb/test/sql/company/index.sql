--
-- index.sql - create company tables with indices
--


-- create tablespace bspace datafile 'bill.dat' size 12800K
-- ;

create schema s;
set schema 's';

create table LOCATION(
  LOCID char(2) primary key,
  STREET varchar(50),
  CITY varchar(20),
  STATE char(2),
  ZIP numeric(5,0)
)
-- tablespace bspace
;

-- DEPT: Departments in the company
create table DEPT (
  DEPTNO integer primary key,
  DNAME varchar(20) unique,
  LOCID CHAR(2)
-- references location
)
-- tablespace bspace
;

-- EMP: employees in the company
create table EMP (
  EMPNO numeric(5,0) primary key,
  FNAME varchar(20) not null,
  LNAME varchar(20) not null,
  SEX char(1),
  DEPTNO integer,
-- references dept,
  MANAGER numeric(5,0),
  LOCID CHAR(2),
-- references location,
  SAL integer,
  COMMISSION integer,
  HOBBY varchar(20)
)
-- tablespace bspace
;

create table CUSTOMERS(
  CUSTID integer primary key,
  FNAME varchar(30),
  LNAME varchar(30) not null,
  SEX char(1))
-- tablespace bspace
;

create table PRODUCTS(
  PRODID integer primary key,
  NAME varchar(30) unique,
  PRICE numeric(3,2))
-- tablespace bspace
;

create table SALES(
  CUSTID integer,
-- references customers,
  EMPNO integer,
-- references emp,
--  TS timestamp,
  PRODID integer,
-- references products,
  PRICE numeric(3,2)
-- possible discount price we sold it for
)
-- tablespace bspace
;

-- Additional indices not implicitly created by above constraints
-- NOTE: don't forget the analyze the table after the rows are loaded
create index EMP_DEPTNO on EMP(DEPTNO);
create index EMP_MANAGER on EMP(MANAGER);
create index EMP_LOCID on EMP(LOCID);
create index EMP_SEX on EMP(SEX);
create index EMP_COMMISSION on EMP(COMMISSION);

create index PRODUCTS_PRICE on PRODUCTS(PRICE);

create index SALES_PRICE on SALES(PRICE);
create index SALES_EMPNO on SALES(EMPNO);
create index SALES_PRODID on SALES(PRODID);
create index SALES_CUSTID on SALES(CUSTID);

create index CUSTOMERS_NAME on CUSTOMERS(LNAME, FNAME);
