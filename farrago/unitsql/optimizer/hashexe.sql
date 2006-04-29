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

create jar luciddb_plugin library 'class com.lucidera.farrago.LucidDbSessionFactory' options(0);

alter session implementation set jar luciddb_plugin;

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
order by 1;

select * from lhxemps2, lhxemps3
where lhxemps2.enameA = lhxemps3.enameB
order by 1;

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
order by 1;

select * from lhxemps3, lhxemps4
where lhxemps3.enameB = lhxemps4.enameC
order by 1;

insert into lhxemps3 values('Leo');

insert into lhxemps4 values('Adel');

-- outer join types
explain plan for
select * from lhxemps3 left outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1;

select * from lhxemps3 left outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1;

explain plan for
select * from lhxemps3 right outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1;

select * from lhxemps3 right outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1;

explain plan for
select * from lhxemps3 full outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1;

select * from lhxemps3 full outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1;

-- FIXME: currently only join conditions referencing input fields are
-- recognized by hash join; cast() is produced by the input so the join
-- condition is not recognized.
-- In future, using an explicit cast operator will make the joinkeys
-- have same data types. HashJoin can be used.

explain plan for
select * from lhxemps3 inner join lhxemps4
on lhxemps3.enameB = cast(lhxemps4.enameC as char(20))
order by 1;

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
order by 1;

select * from lhxemps5 full outer join lhxemps6
on lhxemps5.enameB = lhxemps6.enameC
order by 1;

--------------
-- Clean up --
--------------
!set outputformat table
alter session implementation set default;
drop jar luciddb_plugin options(0);
drop schema lhx cascade;
