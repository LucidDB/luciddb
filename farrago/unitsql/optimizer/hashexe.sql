-------------------------------------------------
-- LucidDB SQL test for Hash Join and Hash Agg --
-------------------------------------------------

-- TODO jvs 11-Sept-2006:  Get rid of this once leak is investigated.
alter system set "codeCacheMaxBytes" = min;

------------------------------------------------
-- non LDB personality uses cartesian product --
------------------------------------------------
create schema lhx;
set schema 'lhx';
set path 'lhx';

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

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

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

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

-- this query uses hash join after pushing down the RHS expression
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

-- explicit type casting is added because hash join requires 
-- join keys have identical types
create table lhxemps2(
    enameA varchar(20))
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

-- try both explicit and implicit casting
explain plan for
select * from lhxemps, lhxemps2
where lhxemps.ename = cast(lhxemps2.enameA as varchar(40))
      and lhxemps.ename = lhxemps2.enameA
order by empno, ename;

select * from lhxemps, lhxemps2
where lhxemps.ename = cast(lhxemps2.enameA as varchar(40))
      and lhxemps.ename = lhxemps2.enameA
order by empno, ename;

explain plan for
select * from lhxemps, lhxemps2
where cast(lhxemps.ename as varchar(20)) = lhxemps2.enameA
      and lhxemps.ename = cast(lhxemps2.enameA as varchar(40))
order by empno, ename;

select * from lhxemps, lhxemps2
where cast(lhxemps.ename as varchar(20)) = lhxemps2.enameA
      and lhxemps.ename = cast(lhxemps2.enameA as varchar(40))
order by empno, ename;

-- make sure casting in both directions work
explain plan for
select * from lhxemps2, lhxemps
where lhxemps2.enameA = lhxemps.ename
order by empno, ename;

select * from lhxemps2, lhxemps
where lhxemps2.enameA = lhxemps.ename
order by empno, ename;

drop table lhxemps2;
create table lhxemps2(
    enameA varchar(60) not null)
server sys_column_store_data_server;

insert into lhxemps2 select name from sales.emps;

explain plan for
select * from lhxemps2, lhxemps
where lhxemps2.enameA = lhxemps.ename
order by empno, ename;

select * from lhxemps2, lhxemps
where lhxemps2.enameA = lhxemps.ename
order by empno, ename;


-- trailing blanks are insignificant when joining two
-- character types.
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
drop table lhxemps2;
create table lhxemps2(
    enameA varchar(60))
server sys_column_store_data_server;

insert into lhxemps2 select name from sales.emps;

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

-- put a filter on the RHS of the outer join to force the RHS to remain on
-- the RHS
explain plan for
select * from lhxemps3 right outer join
    (select * from lhxemps4 where enameC > 'A' or enameC is null) as lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

select * from lhxemps3 right outer join
    (select * from lhxemps4 where enameC > 'A' or enameC is null) as lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

explain plan for
select * from lhxemps3 full outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

select * from lhxemps3 full outer join lhxemps4
on lhxemps3.enameB = lhxemps4.enameC
order by 1, 2;

-- semi/anti join types
explain plan for
select * from lhxemps3 intersect select * from lhxemps4
order by 1;

select * from lhxemps3 intersect select * from lhxemps4
order by 1;

explain plan for
select * from lhxemps3 except select * from lhxemps4
order by 1;

select * from lhxemps3 except select * from lhxemps4
order by 1;

-- Use an explicit cast operator will make the joinkeys
-- have same data types. HashJoin can be used.

explain plan for
select * from lhxemps3 inner join lhxemps4
on lhxemps3.enameB = cast(lhxemps4.enameC as char(20))
order by 1, 2;

select * from lhxemps3 inner join lhxemps4
on lhxemps3.enameB = cast(lhxemps4.enameC as char(20))
order by 1, 2;

explain plan for
select * from lhxemps3 inner join lhxemps4
on upper(lhxemps3.enameB) = upper(cast(lhxemps4.enameC as char(20)))
order by 1, 2;

select * from lhxemps3 inner join lhxemps4
on upper(lhxemps3.enameB) = upper(cast(lhxemps4.enameC as char(20)))
order by 1, 2;

explain plan for
select * from lhxemps3 inner join lhxemps4
on upper(cast(lhxemps3.enameB as char(20))) = upper(cast(lhxemps4.enameC as char(20)))
order by 1, 2;

select * from lhxemps3 inner join lhxemps4
on upper(cast(lhxemps3.enameB as char(20))) = upper(cast(lhxemps4.enameC as char(20)))
order by 1, 2;

explain plan for
select * from lhxemps3 inner join lhxemps4
on upper(cast(lhxemps3.enameB as char(20))) = cast(lhxemps4.enameC as char(20))
order by 1, 2;

select * from lhxemps3 inner join lhxemps4
on upper(cast(lhxemps3.enameB as char(20))) = cast(lhxemps4.enameC as char(20))
order by 1, 2;

-- test ON clause filter conditions for outer joins
create table lhxemps5(
    empnoA integer)
server sys_column_store_data_server;

create table lhxemps6(
    empnoB integer)
server sys_column_store_data_server;

insert into lhxemps5 values(1);
insert into lhxemps5 values(2);

insert into lhxemps6 values(2);
insert into lhxemps6 values(3);

explain plan for
select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB
order by 1, 2;

select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB
order by 1, 2;

-- outer join with filter predicates
-- filter pushed down
explain plan for
select * from lhxemps5 join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 2
order by 1, 2;

-- filter not pushed down
-- outer join on filter can not be evaluated as post filter
explain plan for
select * from lhxemps5 left outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 2
order by 1, 2;

select * from lhxemps5 left outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 2
order by 1, 2;

select * from lhxemps5 left outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 3
order by 1, 2;

-- filter pushed down
explain plan for
select * from lhxemps5 right outer join
    (select * from lhxemps6 where empnoB > 0) as lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 2
order by 1, 2;

-- filter not pushed down
-- outer join on filter can not be evaluated as post filter
explain plan for
select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 2
order by 1, 2;

select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA <> 2
order by 1, 2;

-- this can use hash outer join also
explain plan for
select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA > lhxemps5.empnoA
order by 1, 2;

select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA > lhxemps5.empnoA
order by 1, 2;

explain plan for
select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA * lhxemps5.empnoA > 10
order by 1, 2;

select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA * lhxemps5.empnoA > 10
order by 1, 2;

-- this will use a post-join filter
explain plan for
select * from lhxemps5 join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA > lhxemps6.empnoB
order by 1, 2;

-- outer join on filter can not be evaluated as post filter
-- this should report an error
explain plan without implementation for
select * from lhxemps5 left outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA > lhxemps6.empnoB
order by 1, 2;

-- outer join on filter can not be evaluated as post filter
-- this should report an error
explain plan without implementation for
select * from lhxemps5 right outer join
    (select * from lhxemps6 where empnoB > 0) as lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA > lhxemps6.empnoB
order by 1, 2;

-- outer join on filter can not be evaluated as post filter
-- this should report an error
explain plan without implementation for
select * from lhxemps5 full outer join lhxemps6
on lhxemps5.empnoA = lhxemps6.empnoB and
   lhxemps5.empnoA > lhxemps6.empnoB
order by 1, 2;

--------------------
-- hash aggregate --
--------------------

-- make sure nulls are in the same group
truncate table lhxemps;
insert into lhxemps values (10, NULL, null);
insert into lhxemps values (20, 'Lance', null);
insert into lhxemps values (30, NULL, null);

explain plan for
select ename from lhxemps group by ename order by 1;

select ename from lhxemps group by ename order by 1;

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

-----------------------------
-- aggregating NULL values --
-----------------------------
create table emps1(
    ename1 varchar(20))
server sys_column_store_data_server;

insert into emps1 values(NULL);
insert into emps1 values(NULL);

explain plan for select ename1 from emps1 group by ename1;
explain plan for select distinct ename1 from emps1;
explain plan for select count(*) from emps1 group by ename1;
explain plan for select count(ename1) from emps1 group by ename1;
explain plan for select count(distinct ename1) from emps1;

select ename1 from emps1 group by ename1;
select distinct ename1 from emps1;
select count(*) from emps1 group by ename1;
select count(ename1) from emps1 group by ename1;
select count(distinct ename1) from emps1;

--------------------
-- Hash Semi Join --
--------------------
create table emps2(
    ename2 varchar(20))
server sys_column_store_data_server;

insert into emps1 values('abc');
insert into emps1 values('abc');
insert into emps1 values('def');
insert into emps2 values(NULL);
insert into emps2 values('abc');
insert into emps2 values('abc');

explain plan for
select ename1 from emps1
where ename1 in (select ename2 from emps2)
order by 1;

select ename1 from emps1
where ename1 in (select ename2 from emps2)
order by 1;

explain plan for
select upper(ename1) from emps1
where ename1 in (select ename2 from emps2)
order by 1;

select upper(ename1) from emps1
where ename1 in (select ename2 from emps2)
order by 1;

explain plan for
select ename1 from emps1
where ename1 in (select upper(ename2) from emps2)
order by 1;

explain plan for
select ename1 from emps1
where upper(ename1) in (select upper(ename2) from emps2)
order by 1;

------------------------------------------------------
-- LDB-144
-- http://jirahost.eigenbase.org:8081/browse/LDB-144
------------------------------------------------------
create table A(a int not null);
create table B(b int not null);
create table C(c int not null);
insert into A values (1), (2), (3);
insert into B values (2), (3), (4);
insert into C values (3), (4), (5);

explain plan for
select * from A right outer join (select * from B inner join C on b = c) on a = b and a = c order by a;

select * from A right outer join (select * from B inner join C on b = c) on a = b and a = c order by a;

drop table A;
drop table B;
drop table C;

--------------
-- Clean up --
--------------
!set outputformat table
alter session implementation set default;
drop schema lhx cascade;
