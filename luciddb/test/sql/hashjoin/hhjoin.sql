--set echo=on

create schema s;
set schema 's';
set path 's';

--loadjava testhash1.java
--loadjava testhash2.java

create function testhash1(int1 int)
returns table(
table1_id int,
sex varchar(1)
)
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.HashJoinTestUdx.testhash1';

create function testhash2(int1 int)
returns table(
table2_id int,
sex varchar(1)
)
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.HashJoinTestUdx.testhash2';

-- create instance h1(4) of class testhash1;

-- create instance h2(4) of class testhash2;

explain plan for 
select * from table(testhash1(4)) h1, table(testhash2(4)) h2 
where h1.sex = h2.sex
--order by *;
order by 1,2;

-- select * from h1, h2 where h1.sex = h2.sex order by *;
select * from table(testhash1(4)) h1, table(testhash2(4)) h2 
where h1.sex = h2.sex
--order by *;
order by 1,2,3,4;

-- select * from h1, h2 where h1.sex <> h2.sex order by *;

-- drop instance h1;

-- drop instance h2;

-- create instance h1(50000) of class testhash1;
-- create instance h2(50000) of class testhash2;

explain plan for 
select count(*) from table(testhash1(50000)) h1, table(testhash2(50000))h2
where h1.sex = h2.sex;

-- select count(*) from h1, h2 where h1.sex = h2.sex;
select count(*) from table(testhash1(50000)) h1, table(testhash2(50000))h2
where h1.sex = h2.sex;

-- drop instance h1;

-- drop instance h2;

--create table x (c1 integer, L1 long varchar, L2 long varchar, L3 long varchar);

-- insert into x values (1,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L1')
-- ;
-- insert into x values (2,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L2')
-- ;
-- insert into x values (3,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3')
-- ;
-- insert into x values (4,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L4')
-- ;
-- insert into x values (5,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L5')
-- ;
-- insert into x values (6,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L6')
-- ;
-- insert into x values (7,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L7')
-- ;
-- insert into x values (8,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L8')
-- ;
-- insert into x values (9,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L9')
-- ;

--create table y (c1 integer, L1 long varchar, L2 long varchar, L3 long varchar);

-- insert into y values (13,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L1')
-- ;
-- insert into y values (14,
-- 'L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1L1',
-- 'L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2L2',
-- 'L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L3L2')
-- ;

-- select * from x, y where x.l3 = y.l3
-- ;

-- drop table x;
-- drop table y;

create table x (xid integer, xchar char(10));

insert into x values (1, 'abc');
insert into x values (2, 'defgh');

create table y (yid integer, ychar varchar(10));

insert into y values (1, 'abc');
insert into y values (2, 'ijkl');

select * from x, y where x.xchar = y.ychar 
order by 1,2,3,4;
--order by *;

drop table x;
drop table y;

create table x (xid integer, xnum numeric (3, 2));

insert into x values (1, 1.20);
insert into x values (2, 1.25);

create table y (yid integer, ynum numeric(4, 3));

insert into y values (1, 1.2);
insert into y values (2, 1.200);

select * from x, y where x.xnum = y.ynum 
order by 1,2,3,4;
--order by *;

drop table x;
drop table y;

-- create table x (xid integer, xdate date);

-- insert into x values (1, applib.convert_date('1997-09-20', 'YYYY-MM-DD'));
-- insert into x values (2, applib.convert_date('1997-09-22', 'YYYY-MM-DD'));

-- create table y (yid integer, yts timestamp);

-- insert into y values (1, '1997-09-20 12:34:34');
-- insert into y values (2, '1999-09-20 2:34:34');

-- select * from x, y where year(x.xdate) = year(y.yts) order by *;

-- select * from x, y where x.xdate = to_date(to_char(y.yts)) order by *;

-- select * from x, y where day(x.xdate) = day(to_date(to_char(y.yts))) order by *;

-- drop table x;
-- drop table y;

create table x (xid integer, xvchar varchar(255));

insert into x values (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

insert into x values (2, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');

create table y (yid integer, yvchar varchar(255));

insert into y values (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

insert into y values (2, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');

select * from x, y where x.xvchar = y.yvchar 
order by 1,2,3,4;
--order by *;

drop table x;
drop table y;

-- loadjava hashdistrib1.java;
-- loadjava hashdistrib2.java;

-- create instance x of class hashdistrib1;
-- create instance y of class hashdistrib2;

create function hashdistrib1()
returns table(
id integer,
val integer,
name varchar(10)
)
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.HashJoinTestUdx.hashdistrib1';

create function hashdistrib2()
returns table(
id integer,
val integer,
name varchar(10)
)
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.HashJoinTestUdx.hashdistrib2';

select * from table(hashdistrib1()) x, table(hashdistrib2()) y 
where x.val = y.val and x.id = y.id 
--order by *;
order by 1,2,3,4,5,6;

select * from table(hashdistrib1()) x, table(hashdistrib2()) y 
where x.id = y.val 
-- order by *;
order by 1,2,3,4,5,6;

select * from table(hashdistrib1()) x, table(hashdistrib2()) y  
where x.val = y.id and x.id = y.val 
--order by *;
order by 1,2,3,4,5,6;

-- select * from x, y where x.id = y.val and x.id <> 50 and x.id <> 1 order by *;
select * from table(hashdistrib1()) x, table(hashdistrib2()) y  
where x.id = y.id and x.val = y.val and x.name = y.name 
--order by *;
order by 1,2,3,4,5,6;

-- select count(*) from x, y where x.val <> y.val and x.id <> y.id and x.name = y.name order by *;

--drop instance x;
--drop instance y;

-- Remove below tests. Company data related queries exist in company test suite

-- create source products_source 
-- using link odbc_sqlserver defined by
-- 'select prodid, name, price from BENCHMARK.dbo.products'
-- ;

-- create source sales_source
-- using link odbc_sqlserver defined by
-- 'select custid, empno, prodid, price from BENCHMARK.dbo.sales'
-- ;

-- create source dept_source
-- using link odbc_sqlserver defined by
-- 'select deptno, dname, locid from BENCHMARK.dbo.dept'
-- ;

-- create source emp_source
-- using link odbc_sqlserver defined by
-- 'select empno, fname, lname, sex, deptno, manager, locid, sal, commission, hobby from BENCHMARK.dbo.emp'
-- ;

-- select * from dept_source, emp_source where dept_source.deptno = emp_source.deptno
-- ORDER BY *
-- ;

-- select * from dept_source, emp_source where dept_source.locid = emp_source.locid
-- ORDER BY *
-- ;

-- select * from dept_source right outer join emp_source on dept_source.locid = emp_source.locid
-- ORDER BY *
-- ;
-- select * from emp_source left outer join dept_source on dept_source.locid = emp_source.locid
-- ORDER BY *
-- ;

-- select * from dept_source, emp_source where dept_source.locid = emp_source.locid and dept_source.deptno = emp_source.deptno
-- ;

-- select sum(sal) from dept_source, emp_source where dept_source.deptno = emp_source.deptno
-- ;

-- select sum(sal) from dept_source, emp_source where dept_source.deptno = emp_source.deptno and dept_source.locid = emp_source.locid
-- ;

-- select sum(sal) from dept_source full outer join emp_source on dept_source.locid = emp_source.locid and dept_source.deptno = emp_source.deptno
-- ;

-- select avg(p1.price) from products_source p1, sales_source s1 where p1.price = s1.price
-- ;
-- select avg(p1.price) from products_source p1 full outer join sales_source s1 on p1.price = s1.price
-- ;
-- select avg(p1.price) from products_source p1 left outer join sales_source s1 on p1.price = s1.price
-- ;
-- select avg(p1.price) from products_source p1 right outer join sales_source s1 on p1.price = s1.price
-- ;
-- select avg(p1.price) from products_source p1 where p1.price = ANY (select price from sales_source where price > 4)
-- ;
