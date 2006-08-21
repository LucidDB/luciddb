create schema mergetest;
set schema 'mergetest';

create table EMP (
  EMPNO integer not null,
  FNAME varchar(15),
  LNAME varchar(15),
  SEX char(1),
  DEPTNO integer,
  MANAGER integer,
  LOCID char(2) not null,
  SAL decimal(10, 2),
  COMMISION decimal(10, 2),
  HOBBY varchar(25)
);


create table SALES (
  CUSTID integer not null,
  EMPNO integer,
  PRODID integer,
  PRICE decimal(6, 2)
);

create table PRODUCTS (
  PRODID integer not null,
  NAME varchar(25),
  PRICE decimal(6, 2)
);

create table CUSTOMERS (
  CUSTID integer not null,
  FNAME varchar(15) not null,
  LNAME varchar(15),
  SEX char(1)
);

create table DEPT (
  DEPTNO integer not null,
  DNAME varchar(20),
  LOCID char(2)
);

create table LOCATION (
  LOCID char(2) not null,
  STREET varchar(30),
  CITY varchar(20),
  STATE char(2),
  ZIP decimal(5,0)
);


----------------------------------------------------------------------

-- large data set from bench tables

create schema s;
set schema 's';

create table BENCH10K (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);

create table BENCH100K (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);

create table BENCH1M (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);
