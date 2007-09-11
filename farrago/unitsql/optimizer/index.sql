-- $Id$
-- Test queries which make use of indexes

-- use Volcano for these because Hep can't do a very good job
-- with many of the patterns, and it's a good workout for Volcano
alter session implementation add jar sys_boot.sys_boot.volcano_plugin;

create schema oj;

create table oj.t1(i int not null primary key, j int unique);

create table oj.t2(i int not null primary key, j int unique);

insert into oj.t1 values (1,null), (2, 2), (3, 3);

insert into oj.t2 values (1,null), (2, 2), (4, 4);

create table oj.t3(v varchar(15) not null primary key);
insert into oj.t3 
values ('Mesmer'), ('Houdini'), ('Copperfield'), ('Mandrake');

create table oj.t4(i int not null primary key, j boolean unique);
insert into oj.t4 values (1, null), (2, true), (3, false);

set schema 'sales';

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- search unique clustered index
select name from depts where deptno=20;

select name from depts where deptno > 20 order by name;

select name from depts where deptno >= 20 order by name;

select name from depts where deptno < 20 order by name;

select name from depts where deptno <= 20 order by name;

select name from depts where deptno between 20 and 30 order by name;

select name from depts where deptno > 20 and deptno < 30 order by name;

select name from depts where deptno < 20 or deptno between 30 and 40 
order by name;

select name from depts where deptno in (20,30) order by name;

-- scaling
select name from depts where deptno=20.00;

-- scaling with rounding:  strictness change
select name from depts where deptno > 19.6 order by name;

-- scaling with rounding:  strictness change the other way
select name from depts where deptno >= 20.1 order by name;

-- no match:  overflow
select name from depts where deptno=20000000000000;

-- no match:  overflow
select name from depts where deptno>20000000000000;

-- all match:  negative overflow
select name from depts where deptno>-20000000000000 order by name;

-- no match:  make sure truncation doesn't make it look like one
select v from oj.t3 where v='Houdini                 xyz';

-- no match for Houdini:  make sure truncation doesn't make it look like one
select v from oj.t3 where v >= 'Houdini                 xyz'
order by v;

-- match for Houdini:  make sure truncation doesn't obscure that
select v from oj.t3 where v <= 'Houdini                 xyz'
order by v;

-- contradiction:  empty range
select name from depts where deptno=20 and deptno=30;

-- search beyond end
select name from depts where deptno > 50 order by name;

-- search before start
select name from depts where deptno < 5 order by name;

-- search unique clustered index with a prefix key
select name from emps where deptno=20 order by 1;

-- search unique clustered index with a prefix key and a non-indexable condition
select name from emps where deptno=20 and gender='M';

-- search unique unclustered index
select name from emps where empid=3;

-- project columns covered by clustered index
select gender from emps order by 1;

-- project columns covered by an unclustered index
select name from emps order by 1;
select upper(name) from emps order by 1;

-- sort multiple columns covered by an unclustered index
select name,gender,deptno,empno from emps order by 3,4;

-- unique inner join via clustered index
select depts.name as dname,emps.name as ename
from emps inner join depts
on emps.deptno=depts.deptno
order by 1,2;

-- unique left outer join via clustered index
select depts.name as dname,emps.name as ename
from emps left outer join depts
on emps.deptno=depts.deptno
order by 1,2;

-- left outer join via clustered index prefix
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.deptno
order by 2,1;

-- inner join via unclustered index
select emps.name as ename,depts.name as dname
from depts inner join emps
on depts.deptno=emps.empid
order by 2,1;

-- left outer join via unclustered index
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.empid
order by 2,1;

-- inner join on nullable key
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
inner join depts on e.age = depts.deptno;

-- outer join on nullable key
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
left outer join depts on e.age = depts.deptno;

-- outer join with keys on both sides nullable
select *
from oj.t1 left outer join oj.t2
on t1.j = t2.j;

-- index join which requires swapped join inputs
select 
    depts.name as dname,e.name as ename
from 
    depts 
inner join 
    (select name,age - 20 as age from emps) e
on 
    e.age=depts.deptno
order by 1,2;

-- is null predicate
select * from oj.t1 where j is null;
select * from oj.t1 where not(j is null);

-- predicates on boolean
select * from oj.t4 where j is true;
select * from oj.t4 where j is false;
select * from oj.t4 where j is unknown;

-- csv format is nicest for query plans
!set outputformat csv

-- Note that we explain some queries both with and without order by;
-- with to make sure what we executed above was using the correct plan
-- without to make sure that the order by doesn't affect other
-- aspects of optimization.

explain plan for
select name from depts where deptno=20;

explain plan for
select name from depts where deptno > 20;

explain plan for
select name from depts where deptno >= 20;

explain plan for
select name from depts where deptno < 20;

explain plan for
select name from depts where deptno <= 20;

explain plan for
select name from depts where deptno between 20 and 30;

explain plan for
select name from depts where deptno > 20 and deptno < 30;

explain plan for
select name from depts where deptno < 20 or deptno between 30 and 40;

explain plan for
select name from depts where deptno in (20,30) order by name;

explain plan for
select name from depts where deptno=20.00;

explain plan for
select name from depts where deptno > 19.6 order by name;

explain plan for
select name from depts where deptno >= 20.1 order by name;

explain plan for
select name from depts where deptno=20000000000000;

explain plan for
select name from depts where deptno>20000000000000;

explain plan for
select name from depts where deptno>-20000000000000 order by name;

explain plan for
select v from oj.t3 where v='Houdini                 xyz';

explain plan for
select v from oj.t3 where v >= 'Houdini                 xyz'
order by v;

explain plan for
select v from oj.t3 where v <= 'Houdini                 xyz'
order by v;

explain plan for
select name from depts where deptno=20 and deptno=30;

explain plan for
select name from emps where deptno=20 order by 1;

explain plan for
select name from emps where deptno=20 and gender='M';

explain plan for
select name from emps where empid=3;

explain plan for
select gender from emps;

explain plan for
select gender from emps order by 1;

explain plan for
select name from emps;

explain plan for
select upper(name) from emps;

explain plan for
select name from emps order by 1;

explain plan for
select name,gender,deptno,empno from emps order by 3,4;

explain plan for
select depts.name as dname,emps.name as ename
from emps inner join depts
on emps.deptno=depts.deptno
order by 1,2;

explain plan for
select depts.name as dname,emps.name as ename
from emps left outer join depts
on emps.deptno=depts.deptno
order by 1,2;

explain plan for
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.deptno
order by 2,1;

explain plan for
select emps.name as ename,depts.name as dname
from depts inner join emps
on depts.deptno=emps.empid
order by 2,1;

explain plan for
select emps.name as ename,depts.name as dname
from depts left outer join emps
on depts.deptno=emps.empid
order by 2,1;

explain plan for
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
inner join depts on e.age = depts.deptno;

explain plan for
select e.name as ename,e.age,depts.name as dname
from
(select name,age - 20 as age from emps) e
left outer join depts on e.age = depts.deptno;

explain plan for
select 
    depts.name as dname,e.name as ename
from 
    depts 
inner join 
    (select name,age - 20 as age from emps) e
on 
    e.age=depts.deptno
order by 1,2;

explain plan for
select *
from oj.t1 left outer join oj.t2
on t1.j = t2.j;

explain plan for select * from oj.t1 where j is null;
explain plan for select * from oj.t1 where not(j is null);

explain plan for select * from oj.t4 where j is true;
explain plan for select * from oj.t4 where j is false;
explain plan for select * from oj.t4 where j is unknown;

-- can only explain plan for dynamic parameter search
explain plan for
select name from depts where deptno=?;

-- can't yet support usage of index when predicate on dynamic param
-- is combined with another predicate on same column;
-- at least make sure we recompose it correctly (FRG-72)
explain plan for
select name from depts where deptno > ? and deptno < 30;

explain plan for
select name from depts where deptno > ? and deptno < ?;

-- this one should work because predicates are on different columns
explain plan for
select name from depts where deptno > ? and name='Hysteria';

-- FRG-198
create table oj.frg198(a char(5) primary key, b char(5));
insert into oj.frg198 values ('t1a1', 't1a1'), ('t2a2', 't2a2');
select * from oj.frg198 where a > 't1a1';

----------------------------------------------
-- LucidDB column store bitmap indexes test --
----------------------------------------------

create schema lbm;
set schema 'lbm';
set path 'lbm';

-------------------------------------------------
-- Some ftrs tests to compare behavior against --
-------------------------------------------------

create table ftrsemps(
    empno integer not null constraint empno_pk primary key,
    ename varchar(40),
    deptno integer);

create index deptno_ix on ftrsemps(deptno);
create index ename_ix on ftrsemps(ename);

insert into ftrsemps 
select empno+deptno*1000, name, deptno from sales.emps;

!set outputformat csv

-- no sarg pred
explain plan for
select * from ftrsemps;

-- the most simple case
explain plan for
select * from ftrsemps where deptno = 2;

-- negative number in index search
explain plan for
select * from ftrsemps where deptno = -2;

-- range predicate uses index access
explain plan for
select * from ftrsemps where deptno > 2;

-- Merge ranges on the same index key
explain plan for
select * from ftrsemps where deptno = 2 or deptno = 10;

-- Should have only one range
explain plan for
select * from ftrsemps where deptno = 10 or deptno > 2;

-- recognize AND on the same key
explain plan for
select * from ftrsemps where deptno > 2 and deptno < 10;

-- make sure NULL range from sarg analysis is working
explain plan for
select * from ftrsemps where deptno = 2 and deptno = 10;

-- Index only access:
-- It seems index only access is not used here.
explain plan for
select deptno from ftrsemps where deptno = 2;

-- index on char types:
-- simple comparison predicate
explain plan for
select ename from ftrsemps where ename = 'ADAM';

-- index on char types:
-- predicate specific to character types
explain plan for
select ename from ftrsemps where ename like 'ADAM%';

-- AND: does recognize one index, but not two
explain plan for
select * from ftrsemps where deptno = 2 and ename = 'ADAM';

-- OR: does not use any index access
explain plan for
select * from ftrsemps where deptno = 2 or ename = 'ADAM';

!set outputformat table

drop table ftrsemps cascade;

-------------------------------------------------------
-- Part 1. index created on empty column store table --
-------------------------------------------------------
-- Use LucidDB personality
alter session implementation set jar
    sys_boot.sys_boot.luciddb_index_only_plugin;

create table lbmemps(
    empno integer not null,
    ename varchar(40),
    deptno integer)
    server sys_column_store_data_server
create index empno_ix on lbmemps(empno, deptno)
create index ename_ix on lbmemps(ename)
create index deptno_ix on lbmemps(deptno)
;

create table lbmdepts(
    deptno integer)
server sys_column_store_data_server;

-- create index on existing column store table does not work yet
-- create index ename_ix on lbmemps(ename);

-- two indexes on the same column
-- create index on exisitng column store table does not work yet
-- create index empno_ix2 on lbmemps(empno);

insert into lbmemps select empno, name, deptno from sales.emps;
insert into lbmemps select empno, name, deptno from sales.emps;

!set outputformat csv

-- no sarg pred
explain plan for
select * from lbmemps order by empno;

select * from lbmemps order by empno;

-- fake row count so that index access is considered
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'LBMEMPS', 100);

-- the most simple case
explain plan for
select * from lbmemps where deptno = 20 order by empno;

select * from lbmemps where deptno = 20 order by empno;

-- negative number in index search
explain plan for
select * from lbmemps where deptno = -2;

select * from lbmemps where deptno = -2;

-- range predicate uses index access
explain plan for
select * from lbmemps where deptno > 20 order by empno;

select * from lbmemps where deptno > 20 order by empno;

-- Merge ranges on the same index key
explain plan for
select * from lbmemps where deptno = 10 or deptno = 20 order by empno;

select * from lbmemps where deptno = 10 or deptno = 20 order by empno;

-- Should have only one range
explain plan for
select * from lbmemps where deptno = 20 or deptno > 10 order by empno;

select * from lbmemps where deptno = 20 or deptno > 10 order by empno;

-- recognize AND on the same key
explain plan for
select * from lbmemps where deptno > 10 and deptno <= 20 order by empno;

select * from lbmemps where deptno > 10 and deptno <= 20 order by empno;

-- make sure NULL range from sarg analysis is working
explain plan for
select * from lbmemps where deptno = 20 and deptno = 10 order by empno;

select * from lbmemps where deptno = 20 and deptno = 10 order by empno;

-- make sure Merge is allocated on top of index search if 
-- partial key is used in search
explain plan for
select * from lbmemps where empno = 10 order by empno;

select * from lbmemps where empno = 10 order by empno;

explain plan for
select * from lbmemps where empno = 10 and empno = 20 order by empno;

select * from lbmemps where empno = 10 and empno = 20 order by empno;

-- IN on small values list
explain plan for
select ename from lbmemps where deptno in (20, 30)
order by ename;

select ename from lbmemps where deptno in (20, 30)
order by ename;

-- OR on same column is supported
explain plan for
select *
from lbmemps
where (empno = 110 or empno = 120) and (deptno = 10 or deptno = 20)
order by empno;

select *
from lbmemps
where (empno = 110 or empno = 120) and (deptno = 10 or deptno = 20)
order by empno;

-- index only access.
explain plan for
select deptno from lbmemps where deptno = 20 order by deptno;

select deptno from lbmemps where deptno = 20 order by deptno;

-- index on char types:
-- simple comparison predicate
explain plan for
select ename from lbmemps where ename = 'ADAM' order by ename;

select ename from lbmemps where ename = 'ADAM' order by ename;

-- index on char types:
-- predicate specific to character types
explain plan for
select ename from lbmemps where ename like 'ADAM%' order by ename;

select ename from lbmemps where ename like 'ADAM%' order by ename;

explain plan for
select * from lbmemps where deptno = 10 and ename = 'Fred' order by empno;

select * from lbmemps where deptno = 10 and ename = 'Fred' order by empno;

-- test composite key indexes
explain plan for
select * from lbmemps where empno = 100 and deptno = 10 order by empno;

select * from lbmemps where empno = 100 and deptno = 10 order by empno;

explain plan for
select * from lbmemps where deptno = 10 and empno = 100 order by empno;

select * from lbmemps where deptno = 10 and empno = 100 order by empno;

explain plan for
select * from lbmemps where empno = 100 and deptno >= 10 order by empno;

select * from lbmemps where empno = 100 and deptno >= 10 order by empno;

-- test "not null" data type
explain plan for
select * from lbmemps where empno between 100 and 200 and deptno = 20 order by empno;

select * from lbmemps where empno between 100 and 200 and deptno = 20 order by empno;

-- test multiple inputs to Intersect
explain plan for
select * 
from lbmemps
where empno between 100 and 200 and deptno = 20 and ename = 'Eric'
order by empno;

select * 
from lbmemps
where empno between 100 and 200 and deptno = 20 and ename = 'Eric'
order by empno;

-- TODO OR: currently does not use any index access
explain plan for
select * from lbmemps where deptno = 2 or ename = 'Fred' order by empno;

select * from lbmemps where deptno = 2 or ename = 'Fred' order by empno;

----------------------------------------------------------------------
-- Tests using multiple index keys with range searches on the last key
----------------------------------------------------------------------
create table multikey(a int, b int) server sys_column_store_data_server;
insert into multikey values (0, 0);
insert into multikey values (1, 1);
insert into multikey values (1, 2);
insert into multikey values (1, 3);
insert into multikey values (1, 4);
insert into multikey values (2, 2);
create index imultikey on multikey(a, b);

-- fake row count so that index access is considered
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'MULTIKEY', 100);

!set outputformat csv
explain plan for select * from multikey where a = 1 and b > 1;
explain plan for select * from multikey where a = 1 and b <= 3;
explain plan for select * from multikey where a = 1 and b >= 2 and b < 4;

!set outputformat table
select * from multikey where a = 1 and b > 1;
select * from multikey where a = 1 and b <= 3;
select * from multikey where a = 1 and b >= 2 and b < 4;

----------------------------
-- Tests of index only scans
----------------------------
create table person(
    id int primary key,
    age int)
server sys_column_store_data_server
create index age_idx on person(age);

-- fake row count so that index access is considered
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'PERSON', 100);

!set outputformat csv

-- index search
explain plan for
select id from person where id = 30;

-- index search with merge
explain plan for
select id from person where id > 30;

-- histogram type aggregate
explain plan for 
select age, count(*) from person group by age order by age;

-- cardinality type aggregate
explain plan for 
select count(distinct(age)) from person;

-- agg with index search
explain plan for 
select avg(age) from person where age > 30;

-- multikey
explain plan for 
select a from multikey where a = 1 and b > 1 group by a;

explain plan for 
select b from multikey where a = 1;

explain plan for 
select b from multikey where a = 1 group by b;

-- widening of an index search 
create index multikey_a on multikey(a);

explain plan for
select a, b from multikey where a = 1;

-- negative test: row scan with child can't be converted
explain plan for 
select age, count(*) from person where id > 5 group by age;

-- negative test: index only scan produces wrong sort order for agg
explain plan for 
select a, b from multikey where a = 1 and b > 1 group by b, a;

-- TODO: implement interposed calc when we have sort order propagation
explain plan for 
select avg(age+1) from person;

explain plan for 
select avg(age+1) from person where age = 30;

----------------------------------------------------------
-- Tests to exercise using startrid in bitmap index search
----------------------------------------------------------

create server test_data
foreign data wrapper sys_file_wrapper
options (
    directory 'unitsql/optimizer/data',
    file_extension 'csv',
    with_header 'yes',
    log_directory 'testlog');

create foreign table ridsearchdata(a int, b int, c int)
    server test_data
    options (filename 'ridsearchdata');

create table ridsearchtable(fakerid int, a int, b int)
    server sys_column_store_data_server;
create index i_a on ridsearchtable(a);
create index i_b on ridsearchtable(b);
insert into ridsearchtable select * from ridsearchdata;

!set outputformat csv
explain plan for select * from ridsearchtable where b = 0 and a = 2;
explain plan for select * from ridsearchtable where b = 0 and a = 3;
explain plan for select * from ridsearchtable where b = 0 and a = 1;

!set outputformat table
select * from ridsearchtable order by fakerid;
select * from ridsearchtable where b = 0 and a = 2;
select * from ridsearchtable where b = 0 and a = 3;
-- no rows
select * from ridsearchtable where b = 0 and a = 1;

-- reverse the order of the index creation and repeat

drop table ridsearchtable;
create table ridsearchtable(fakerid int, a int, b int)
    server sys_column_store_data_server;
create index i_b on ridsearchtable(b);
create index i_a on ridsearchtable(a);
insert into ridsearchtable select * from ridsearchdata;

!set outputformat csv
explain plan for select * from ridsearchtable where b = 0 and a = 2;
explain plan for select * from ridsearchtable where b = 0 and a = 3;
explain plan for select * from ridsearchtable where b = 0 and a = 1;

!set outputformat table
select * from ridsearchtable order by fakerid;
select * from ridsearchtable where b = 0 and a = 2;
select * from ridsearchtable where b = 0 and a = 3;
-- no rows
select * from ridsearchtable where b = 0 and a = 1;

---------------------
-- Some join tests --
---------------------
-- Filter above join gets pushed down so index can be used.
!set outputformat csv
explain plan for 
select * from lbmdepts, lbmemps where lbmemps.deptno = 2;

select * from lbmdepts, lbmemps where lbmemps.deptno = 2;

-- Filter in an inline view is already in the right place for indexing.
explain plan for 
select * from lbmdepts, 
              (select * from lbmemps where lbmemps.deptno = 2) view;

select * from lbmdepts, 
              (select * from lbmemps where lbmemps.deptno = 2) view;

!set outputformat table

------------
-- Misc bugs
------------
-- FRG-83
create table t(a int, b int, c int) server sys_column_store_data_server;
create index ita on t(a);
create index itb on t(b);
insert into t values(1, 2, 3);
create view v as select * from t where a = 1 and b = 2;
select * from v v1, v v2; 
drop table t cascade;

----------------------------------------
-- Test for cost based index access   --
-- 1.1 Single table index access path --
--     with stats                     --
----------------------------------------
!set outputformat csv

-- analyze table
create table test(a int, b int, c int, d int);

insert into test values(10,20,30,40);
insert into test values(11,21,31,41);
insert into test values(12,22,32,42);
insert into test values(13,23,33,43);
insert into test values(14,24,34,44);
insert into test values(15,25,35,45);
insert into test values(10,20,30,40);
insert into test values(11,21,31,41);
insert into test values(12,22,32,42);
insert into test values(13,23,33,43);
insert into test values(14,24,34,44);
insert into test values(15,25,35,45);
insert into test values(10,20,30,40);
insert into test values(11,21,31,41);
insert into test values(12,22,32,42);
insert into test values(13,23,33,43);
insert into test values(14,24,34,44);
insert into test values(15,25,35,45);

-- create index test_ab on test(a, b);
create index test_cb on test(c, b);
create index test_b on test(b);
create index test_ba on test(b, a);

-- plan without analyze
explain plan for
select * from test
where a = 10 and b = 20 and c > 30;

explain plan for
select * from test
where b = 20;

select * from test
where a = 10 and b = 20 and c > 10
order by a;

select * from test
where b = 20
order by a;

-- plan with analyze
analyze table test compute statistics for all columns;

explain plan for
select * from test
where a = 10 and b = 20 and c > 30;

explain plan for
select * from test
where b = 20;

select * from test
where a = 10 and b = 20 and c > 10
order by a;

select * from test
where b = 20
order by a;

drop table test cascade;

----------------------------------------
-- Test for cost based index access   --
-- 1.2 Single table index access path --
--     with fake stats                --
----------------------------------------
create table t(a varchar(20), b char(20), c varchar(20), d varchar(20));

create index t_abcd on t(a,b,c,d);
create index t_a on t(a);
create index t_b on t(b);

call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'T', 100000);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'T', 'A', 2, 100, 2, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'T', 'B', 3, 100, 3, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'T', 'C', 200, 100, 200, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'T', 'D', 300, 100, 300, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

-- deletion index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$DELETION_INDEX$T', 2);

-- clustered index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$A', 2);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$B', 3);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$C', 200);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$D', 300);

-- unclustered index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'T_ABCD', 1000);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'T_A', 2);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'T_B', 2);


explain plan for
select * from t
where a= 'a' and b= 'b';

-- change stats and try again
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'T', 100000);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$C', 20);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$D', 30);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'T_ABCD', 20);

explain plan for
select * from t
where a= 'a' and b= 'b';

drop table t cascade;

----------------------------------------
-- Test for cost based index access   --
-- 2. Semijoin index access path      --
--     with fake stats                --
----------------------------------------
create table t(b char(20), d varchar(20) not null);

create index it_b on t(b);
create index it_bd on t(b, d);
create index it_d on t(d);
create index it_db on t(d, b);

insert into t values('abcdef', 'this is row 1');
insert into t values('abcdef', 'this is row 2');
insert into t values('abcdef', 'this is row 3');
insert into t values(null, 'this is row 4');
insert into t values(null, 'no match');

-- although this table has the same number of rows as t, we will force this
-- to be the dimension table in the semijoin by putting a dummy filter on
-- the table

create table smalltable(s1 varchar(128) not null, s3 varchar(128) not null);

insert into smalltable values('this is row 1', 'abcdef');
insert into smalltable values('this is row 2', 'abcdef');
insert into smalltable values('this is row 3', 'abcdef');
insert into smalltable values('this is row 4', 'abcdef');
insert into smalltable values('this is row 5', 'abcdef');

-- plan without stats
explain plan for
select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by d;

explain plan for 
select *
    from t inner join smalltable s
    on t.d = s.s1 where s.s3 = 'abcdef' order by d;

select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by d;

select *
    from t inner join smalltable s
    on t.d = s.s1 where s.s3 = 'abcdef' order by d;

-- Create fake statistics.  The stats do not match the actual data in the
-- tables and are meant to force the optimizer to choose semijoins

call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'T', 10000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'SMALLTABLE', 10);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'T', 'B', 10, 100, 10, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'T', 'D', 10, 100, 10, 1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'SMALLTABLE', 'S1', 10, 100, 10, 1,
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'LBM', 'SMALLTABLE', 'S3', 10, 100, 10, 1,
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

-- deletion index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$DELETION_INDEX$T', 2);

-- clustered index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$B', 2);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$T$D', 2);

-- unclustered index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'IT_B', 10);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'IT_BD', 20);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'IT_D', 1);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'IT_DB', 2);

-- deletion index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$DELETION_INDEX$SMALLTABLE', 2);

-- clustered index
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$SMALLTABLE$S1', 2);
call sys_boot.mgmt.stat_set_page_count('LOCALDB', 'LBM', 'SYS$CLUSTERED_INDEX$SMALLTABLE$S3', 2);

explain plan for
select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by d;

explain plan for 
select *
    from t inner join smalltable s
    on t.d = s.s1 where s.s3 = 'abcdef' order by d;

select *
    from t inner join smalltable s
    on t.b = s.s3 and s.s1 = 'this is row 1' 
    order by d;

select *
    from t inner join smalltable s
    on t.d = s.s1 where s.s3 = 'abcdef' order by d;

drop table t cascade;
drop table smalltable cascade;

-- Test to make sure index only scans aren't enabled by default.  Earlier, we
-- verified that when index only scans are enabled, it is used with the
-- following select query.
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
explain plan for
    select deptno from lbmemps where deptno = 20 order by deptno;

--------------
-- Clean up --
--------------
drop server test_data cascade;
drop schema lbm cascade;

-- End index.sql
