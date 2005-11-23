-- $Id$
-- This script creates a simple schema used by some of the unit tests.

!set verbose true

-- create local sales schema
create schema sales;
set schema 'sales';

create table depts(
    deptno integer not null primary key,
    name varchar(128) not null constraint depts_unique_name unique);

create table emps(
    empno integer not null,
    name varchar(128) not null,
    deptno integer not null,
    gender char(1) default 'M',
    city varchar(128),
    empid integer not null unique,
    age integer,
    public_key varbinary(50),
    slacker boolean,
    manager boolean not null,
    primary key(deptno,empno))
    create index emps_ux on emps(name);

create global temporary table temps(
    empno integer not null,
    name varchar(128) not null,
    deptno integer not null,
    gender char(1),
    city varchar(128),
    empid integer default 999 not null,
    age integer,
    public_key varbinary(50),
    slacker boolean,
    manager boolean not null,
    primary key(deptno,empno)) on commit delete rows
    create clustered index temps_cx on temps(empno);

create view empsview as
select empno, name from emps;

create view tempsview as
select empno, name from temps;

create view joinview as
select depts.name as dname,emps.name as ename
from emps inner join depts
on emps.deptno=depts.deptno;

create function decrypt_public_key(k varbinary(50))
returns varchar(25)
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.decryptPublicKey';

create function maybe_female(gender char(1))
returns boolean
contains sql
return not((gender = 'F') is false);

insert into depts values
    (10,trim('Sales')),
    (20,'Marketing'),
    (30,'Accounts');

insert into emps values
    (100,'Fred',10,null,null,30,25, x'41626320',true,false),
    (110,trim('Eric'),20,'M',trim('San Francisco'),3,80,x'416263',null,false),
    (110,'John',40,'M','Vancouver',2,null,x'58797A',false,true),
    (120,'Wilma',20,'F',null,1,50,null,null,true);

-- define foreign server for hsqldb sample data
create server hsqldb_demo
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    table_types 'TABLE,VIEW');
