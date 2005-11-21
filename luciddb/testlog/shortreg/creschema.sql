--
-- index.sql - create company tables with indices
--

create schema s;

create table s.LOCATION(
LOCID char(2) primary key,
STREET varchar(50),
CITY varchar(20),
STATE char(2),
ZIP integer)
server sys_ftrs_data_server;

-- DEPT: Departments in the company
create table s.DEPT(
DEPTNO integer primary key,
DNAME varchar(20) unique,
LOCID CHAR(2))
server sys_ftrs_data_server;

-- EMP: employees in the company
create table s.EMP(
EMPNO integer primary key,
FNAME varchar(20) not null,
LNAME varchar(20) not null,
SEX char(1),
DEPTNO integer,
MANAGER integer,
LOCID CHAR(2),
SAL integer,
COMMISSION integer,
HOBBY varchar(20))
server sys_ftrs_data_server;

create table s.CUSTOMERS(
CUSTID integer primary key,
FNAME varchar(30),
LNAME varchar(30) not null,
SEX char(1))
server sys_ftrs_data_server;

create table s.PRODUCTS(
PRODID integer primary key,
NAME varchar(30) unique,
PRICE float)
server sys_ftrs_data_server;

-- TS timestamp,
create table s.SALES(
CUSTID integer,
EMPNO integer,
PRODID integer,
PRICE float,
primary key(CUSTID, EMPNO, PRODID, PRICE))
server sys_ftrs_data_server;


-- Additional indices not implicitly created by above constraints

create index EMP_DEPTNO on s.EMP(DEPTNO);
create index EMP_MANAGER on s.EMP(MANAGER);
create index EMP_LOCID on s.EMP(LOCID);
create index EMP_SEX on s.EMP(SEX);
create index EMP_COMMISSION on s.EMP(COMMISSION);

create index PRODUCTS_PRICE on s.PRODUCTS(PRICE);

create index SALES_PRICE on s.SALES(PRICE);
create index SALES_EMPNO on s.SALES(EMPNO);
create index SALES_PRODID on s.SALES(PRODID);
create index SALES_CUSTID on s.SALES(CUSTID);

create index CUSTOMERS_NAME on s.CUSTOMERS(LNAME, FNAME);


-- /creschema.sql

create foreign data wrapper test_jdbc library '../farrago/plugin/FarragoMedJdbc.jar' language java;
 
create server csv_server
foreign data wrapper test_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver',
    url 'jdbc:relique:csv:testlog/shortreg',
    schema_name 'TESTDATA');

create schema csv_schema;
