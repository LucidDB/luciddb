-------------------------------------------------
-- LucidDB SQL test for Hash Join and Hash Agg --
-------------------------------------------------

------------------------------------------------
-- non LDB personality uses cartesian product --
------------------------------------------------
create schema lhx;
set schema 'lhx';
set path 'lhx';

create table lhxemps(
    empno integer not null,
    ename varchar(40),
    deptno integer)
server sys_column_store_data_server;

create table lhxdepts(
    deptnoA integer)
server sys_column_store_data_server;

insert into lhxemps select empno, name, deptno from sales.emps;
insert into lhxemps select empno, name, deptno from sales.emps;

insert into lhxdepts select deptno from sales.emps;

!set outputformat csv
select * from lhxemps order by 1;
select * from lhxdepts order by 1;

explain plan for 
select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA
order by empno, ename;

select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA
order by empno, ename;

-- Clean up
!set outputformat table
drop schema lhx cascade;


------------------------------------
-- LDB personality uses hash join --
------------------------------------
create schema lhx;
set schema 'lhx';
set path 'lhx';

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table lhxemps(
    empno integer not null,
    ename varchar(40),
    deptno integer);

create table lhxdepts(
    deptnoA integer,
    deptnoB integer);

create table lhxdepts2(
    deptnoC integer,
    deptnoD integer);

create table lhxdepts3(
    deptnoE integer,
    deptnoF integer);

create table lhxdepts4(
    deptnoG integer,
    deptnoH integer);

insert into lhxemps select empno, name, deptno from sales.emps;
insert into lhxemps select empno, name, deptno from sales.emps;

insert into lhxdepts select deptno, deptno from sales.emps;
insert into lhxdepts2 select deptno, deptno from sales.emps;
insert into lhxdepts3 select deptno, deptno from sales.emps;
insert into lhxdepts4 select deptno, deptno from sales.emps;


!set outputformat csv
select * from lhxemps order by 1;
select * from lhxdepts order by 1;
select * from lhxdepts2 order by 1;

explain plan for 
select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA
order by empno, ename;

select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA
order by empno, ename;

-- test hash join implementation for big IN
explain plan for 
select ename from lhxemps
where empno in 
(110, 110, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
order by ename;

select ename from lhxemps
where empno in 
(110, 110, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
order by ename;

-- this query still uses cartesian product
explain plan for 
select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA + 1
order by empno, ename;

-- multiple conditions between the same pair of tables
explain plan for 
select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA and lhxemps.deptno = lhxdepts.deptnoB
order by empno, ename;

select * from lhxemps, lhxdepts
where lhxemps.deptno = lhxdepts.deptnoA and lhxemps.deptno = lhxdepts.deptnoB
order by empno, ename;

-- now try two joins
explain plan for 
select * from lhxemps, lhxdepts, lhxdepts2
where lhxemps.deptno = lhxdepts.deptnoB and lhxdepts.deptnoA = lhxdepts2.deptnoC
order by empno, ename;

select * from lhxemps, lhxdepts, lhxdepts2
where lhxemps.deptno = lhxdepts.deptnoB and lhxdepts.deptnoA = lhxdepts2.deptnoC
order by empno, ename;

-- four joins
explain plan for 
select * from lhxemps, lhxdepts, lhxdepts2, lhxdepts3, lhxdepts4
where lhxemps.deptno = lhxdepts.deptnoB and lhxemps.deptno = lhxdepts2.deptnoC
    and lhxemps.deptno = lhxdepts3.deptnoF and lhxemps.deptno = lhxdepts4.deptnoG
order by empno, ename;

select * from lhxemps, lhxdepts, lhxdepts2, lhxdepts3, lhxdepts4
where lhxemps.deptno = lhxdepts.deptnoB and lhxemps.deptno = lhxdepts2.deptnoC
    and lhxemps.deptno = lhxdepts3.deptnoF and lhxemps.deptno = lhxdepts4.deptnoG
order by empno, ename;

-- only identical types are recognized and can use hash join
create table lhxemps2(
    enameA varchar(40))
server sys_column_store_data_server;

insert into lhxemps2 select name from sales.emps;

select * from lhxemps2 order by 1;

explain plan for
select * from lhxemps, lhxemps2
where lhxemps.ename = lhxemps2.enameA
order by empno, ename;

select * from lhxemps, lhxemps2
where lhxemps.ename = lhxemps2.enameA
order by empno, ename;

-- trailing blanks are insignificant when joining two
-- character types.
-- currently since hash join does not do type casting,
-- if join keys have different types, do not use hash join
create table lhxemps3(
    enameB char(20))
server sys_column_store_data_server;

insert into lhxemps3 select name from sales.emps;

select * from lhxemps3 order by 1;

explain plan for
select * from lhxemps, lhxemps3
where lhxemps.ename = lhxemps3.enameB
order by empno, ename;

select * from lhxemps, lhxemps3
where lhxemps.ename = lhxemps3.enameB
order by empno, ename;

-- column values containing null
-- nulls should not join with nulls of a different type
-- note a join with keys of different types does not use hash join
insert into lhxemps2 values(null);
insert into lhxemps3 values(null);

explain plan for
select * from lhxemps2, lhxemps3
where lhxemps2.enameA = lhxemps3.enameB
order by 1, 2;

select * from lhxemps2, lhxemps3
where lhxemps2.enameA = lhxemps3.enameB
order by 1, 2;

-- nulls should not join with nulls of the same type either
create table lhxemps4(
    enameC char(20))
server sys_column_store_data_server;

insert into lhxemps4 select name from sales.emps;

insert into lhxemps4 values(null);

select * from lhxemps4 order by 1;

explain plan for
select * from lhxemps3, lhxemps4
where lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

select * from lhxemps3, lhxemps4
where lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

insert into lhxemps3 values('Leo');

insert into lhxemps4 values('Adel');

-- outer join types
explain plan for
select * from lhxemps3 left outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

select * from lhxemps3 left outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

explain plan for
select * from lhxemps3 right outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

select * from lhxemps3 right outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

explain plan for
select * from lhxemps3 full outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

select * from lhxemps3 full outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

-- FIXME: currently only join conditions referencing input fields are
-- recognized by hash join; cast() is produced by the input so the join
-- condition is not recognized.
-- In future, using an explicit cast operator will make the joinkeys
-- have same data types. HashJoin can be used.

explain plan for
select * from lhxemps3 inner join lhxemps4
on lhxemps3.enameB = cast(lhxemps4.enameC as char(20))
order by 1, 2;

-- not null types are compatible with hash outer joins
create table lhxemps5(
    enameB char(20) not null)
server sys_column_store_data_server;

create table lhxemps6(
    enameC char(20) not null)
server sys_column_store_data_server;

insert into lhxemps5 values('Leo');

insert into lhxemps6 values('Adel');

explain plan for
select * from lhxemps5 full outer join lhxemps6
on lhxemps5.enameB = lhxemps6.enameC
order by 1, 2;

select * from lhxemps5 full outer join lhxemps6
on lhxemps5.enameB = lhxemps6.enameC
order by 1, 2;

--------------------
-- hash aggregate --
--------------------

truncate table lhxemps;
insert into lhxemps select empno, name, deptno from sales.emps;

explain plan for
select deptno from lhxemps group by deptno order by 1;

select deptno from lhxemps group by deptno order by 1;

explain plan for
select distinct deptno from lhxemps order by 1;

select distinct deptno from lhxemps order by 1;

explain plan for
select count(deptno) from lhxemps;

select count(deptno) from lhxemps;

explain plan for
select count(empno) from lhxemps group by deptno order by 1;

select count(empno) from lhxemps group by deptno order by 1;

explain plan for
select count(distinct empno) from lhxemps group by deptno order by 1;

select count(distinct empno) from lhxemps group by deptno order by 1;

explain plan for
select count(*) from lhxemps;

select count(*) from lhxemps;

explain plan for
select count(*) from lhxemps group by deptno order by 1;

select count(*) from lhxemps group by deptno order by 1;

-- make sure empty single group produces correct result
truncate table lhxemps;

explain plan for
select count(*) from lhxemps;

select count(*) from lhxemps;

explain plan for
select count(*) from lhxemps group by deptno order by 1;

select count(*) from lhxemps group by deptno order by 1;

-- make sure the hash table handles partial aggregate of increasing size
truncate table lhxemps;
insert into lhxemps values(100, 'a', 20);
insert into lhxemps values(100, 'ab', 20);
insert into lhxemps values(100, 'abc', 20);
insert into lhxemps values(100, 'abcd', 20);
insert into lhxemps values(100, 'abcde', 20);
insert into lhxemps values(100, 'abcdef', 20);

explain plan for select max(ename) from lhxemps group by deptno order by 1;
select max(ename) from lhxemps group by deptno order by 1;

-- testing run time error message for row length
drop table lhxemps;
create table lhxemps(empno char(2000), ename char(2000));

insert into lhxemps values('abc', 'abc');

explain plan for
select empno, min(ename), max(ename) from lhxemps group by empno;

select empno, min(ename), max(ename) from lhxemps group by empno;

------------------------------------
-- the following are from agg.sql --
------------------------------------
set schema 'sales';

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

select count(*) from depts;

select count(city) from emps;

select count(city) from emps where empno > 100000;

select sum(deptno) from depts;

select sum(deptno) from depts where deptno > 100000;

select max(deptno) from depts;

select min(deptno) from depts;

select avg(deptno) from depts;

------------
-- group bys
------------

select deptno, count(*) from emps group by deptno order by 1;

-- Issue the same statement again to make sure LhxAggStream
-- is in good state when reopened
select deptno, count(*) from emps group by deptno order by 1;

select d.name, count(*) from emps e, depts d
    where d.deptno = e.deptno group by d.name order by 1;

-- Test group by key where key value could be NULL
select deptno, gender, min(age), max(age) from emps
    group by deptno, gender order by 1, 2;

select sum(age) from emps group by deptno order by 1;

-- Test where input stream is empty
select deptno, count(*) from emps where deptno < 0 group by deptno order by 1;


-- verify plans
!set outputformat csv

explain plan for
select count(*) from depts;

explain plan for
select count(city) from emps;

explain plan for
select sum(deptno) from depts;

explain plan for
select max(deptno) from depts;

explain plan for
select min(deptno) from depts;

explain plan for
select avg(deptno) from depts;

explain plan without implementation for
select deptno,max(name) from sales.emps group by deptno order by 1;

-----------------------------
-- verify plans for group bys
-----------------------------

explain plan for 
select deptno, count(*) from emps group by deptno order by 1;

explain plan for
select d.name, count(*) from emps e, depts d
    where d.deptno = e.deptno group by d.name order by 1;

explain plan for
select deptno, gender, min(age), max(age) from emps
    group by deptno, gender order by 1, 2;

explain plan for
select sum(age) from emps group by deptno order by 1;

--------------
-- Clean up --
--------------
!set outputformat table
alter session implementation set default;
drop schema lhx cascade;
