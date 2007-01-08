-----------------------------------
-- LucidDB Column Store SQL test --
-----------------------------------

create schema lcs;
set schema 'lcs';
set path 'lcs';

-- set session personality to LucidDB so all tables
-- will be column-store by default
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;


---------------------------------
-- Part 1. Single Cluster test --
---------------------------------
--
-- 1.1 One cluster of a single column.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(empno int);

-- verify creation of system-defined clustered index
!indexes LCSEMPS

!set outputformat csv
-- verify that explain plan works
explain plan for insert into lcsemps values(10);

explain plan for 
insert into lcsemps select empno from sales.emps;
!set outputformat table

-- verify that insert values works
insert into lcsemps values(10);

-- verify that insert select works
insert into lcsemps select empno from sales.emps;

select * from lcsemps order by empno;

-- verify truncate works
truncate table lcsemps;

select * from lcsemps;

-- verify that cached insert plans work (Jira Issue LDB-4)

insert into lcsemps values(20);
truncate table lcsemps;
insert into lcsemps values(20);
select * from lcsemps order by empno;
truncate table lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.2 One cluster of three columns.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(empno int, name varchar(128), empid int) 
create clustered index explicit_lcsemps_all on lcsemps(empno, name, empid);

-- verify creation of system-defined clustered indices
!indexes LCSEMPS

-- verify that explain plan works
!set outputformat csv
explain plan for insert into lcsemps values(10, 'Selma', 10000);

explain plan for 
insert into lcsemps select empno, name, empid from sales.emps;
!set outputformat table

-- verify that insert values works
insert into lcsemps values(10, 'Selma', 10000);

-- verify that insert select works
insert into lcsemps select empno, name, empid from sales.emps;

select * from lcsemps order by empno, name;

-- test some projections
select empno, name, empid from lcsemps order by empno;
select name from lcsemps order by name;
select name, name from lcsemps order by name;
select empid, empno from lcsemps order by empno;
select name, empno, empid from lcsemps order by empno;
select empno + empid as empno_plus_empid from lcsemps order by empno_plus_empid;

-- verify truncate works
truncate table lcsemps;

select * from lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.3 One cluster of a single column.
--     Testing inserting NULLs and empty strings.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(city varchar(20));

-- verify creation of system-defined clustered index
!indexes LCSEMPS

-- verify that explain plan works
!set outputformat csv
explain plan for insert into lcsemps values(NULL);
explain plan for insert into lcsemps values('');
explain plan for insert into lcsemps values('Pescadero');

-- Plans with NULL in the populating stream
explain plan for 
insert into lcsemps select city from sales.emps;

explain plan for 
insert into lcsemps select city from sales.emps where empno = 100;
!set outputformat table

-- Locate a NULL value from the source table.
!set outputformat table
select city from sales.emps where empno = 100;

-- verify that insert values works
insert into lcsemps values(NULL);

-- verify that executing the same exec stream also works
insert into lcsemps values(NULL);

-- verify that insert values works
insert into lcsemps values('');

-- verify that insert values works
insert into lcsemps values('Pescadero');

-- verify that insert select works
insert into lcsemps select city from sales.emps where empno = 100;

-- verify that insert select works
insert into lcsemps select city from sales.emps;

-- verify that executing the same exec stream also works
insert into lcsemps select city from sales.emps;

select * from lcsemps order by city;

-- verify truncate works
truncate table lcsemps;

select * from lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.4 Bug case
--
create table lcsemps(city varchar(20));

insert into lcsemps select city from sales.emps;

insert into lcsemps select city from sales.emps;

-- NOTE: 2005-12-06(rchen) this used to fail with:
-- java: SXMutex.cpp:144: bool fennel::SXMutex::tryUpgrade(): Assertion `!nExclusive' failed.
-- It's now fixed by allocating a brand new LcsClusterNodeWriter for every 
-- LcsClusterAppendExecStream::open()
insert into lcsemps values(NULL);

select * from lcsemps order by city;

-- issue the select again to make sure state is correctly reset when the
-- stream is re-executed
select * from lcsemps order by city;

-- verify truncate works
truncate table lcsemps;

select * from lcsemps;

drop table lcsemps;


--
-- 1.5 Bugcase
--
create table lcsemps(city varchar(20));

insert into lcsemps select city from sales.emps;

insert into lcsemps select city from sales.emps;

select * from lcsemps order by city;

-- verify truncate works
truncate table lcsemps;

select * from lcsemps;

-- NOTE: 2005-12-06(rchen) this used to fail with:
-- java: ../../fennel/cache/CacheMethodsImpl.h:299: void fennel::CacheImpl<PageT, VictimPolicyT>::discardPage:Assertion `page->nReferences == 1' failed.
-- It's now fixed by allocating a brand new LcsClusterNodeWriter for every LcsClusterAppendExecStream::open()
drop table lcsemps;


--------------------------------
-- Part 2. Multi-cluster test --
--------------------------------
--
-- 2.1 Two clusters of a single column each.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(empno int, name varchar(128));

-- verify creation of system-defined clustered indices
!indexes LCSEMPS

-- verify that explain plan works
!set outputformat csv
explain plan for insert into lcsemps values(10, 'Selma');

explain plan for 
insert into lcsemps select empno, name from sales.emps;
!set outputformat table

-- verify that insert values works
insert into lcsemps values(10, 'Selma');

-- verify that insert select works
insert into lcsemps select empno, name from sales.emps;

select * from lcsemps order by empno, name;

-- verify truncate works
truncate table lcsemps;

select * from lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 2.2 Four clusters.
--
create table multicluster(
    c0 int, c1 varchar(128), c2 int, c3 int, c4 int, c5 int, c6 varchar(128),
    c7 int)
create clustered index i_c0 on multicluster(c0)
create clustered index i_c1_c2 on multicluster(c1, c2)
create clustered index i_c3_c4_c5 on multicluster(c3, c4)
create clustered index i_c6_c7_c8_c9 on multicluster(c5, c6, c7);

!set outputformat csv
explain plan for
insert into multicluster values(1, 'am', 100, 50, -10, 1000, NULL, 0);

explain plan for
select * from multicluster order by c0, c1;
!set outputformat table

insert into multicluster values(1, 'am', 100, 50, -10, 1000, NULL, 0);
insert into multicluster values(1, 'am', 100, 50, -10, 1000, NULL, 0);
insert into multicluster values(1, NULL, 101, 51, -11, 1001, NULL, 1);
insert into multicluster values(2, '',   102, 52, -12, 1002, NULL, 2);

select c7, c1, c4, c0 from multicluster order by c0, c1;
select c7, c1, c4, c0 from multicluster order by c0, c1;

select c3, c1, c1, c4, c7 from multicluster order by c1, c3;

select * from multicluster order by c0, c1;

-- A parser bug
-- This sometimes does not work:
-- !set outputformat csv
-- explain plan with implementation for
-- insert into multicluster select empno, name, 0, 10, 100, 1000, city, deptno from sales.emps;
!set outputformat csv
explain plan for insert into multicluster select empno, name, 0, 10, 100, 1000, city, deptno from sales.emps; 

explain plan for
select * from multicluster order by c0, c1;
!set outputformat table

insert into multicluster select empno, name, 0, 10, 100, 1000, city, deptno from sales.emps; 

select c7, c1, c4, c0 from multicluster order by c0, c1;
select c7, c1, c4, c0 from multicluster order by c0, c1;

select c3, c1, c1, c4, c7 from multicluster order by c1, c3;

select * from multicluster order by c0, c1;

truncate table multicluster;

select * from multicluster;

drop table multicluster;

--
-- 2.3 Try a different source
--
create table threeclusters(c0 int, c1 varchar(128), c2 char(2));

create server flatfile_server
foreign data wrapper sys_file_wrapper
options (
    with_header 'yes', 
    file_extension '',
    log_directory 'testlog');

create foreign table flatfile_table(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'unitsql/med/example.csv');

!set outputformat csv
explain plan for
insert into threeclusters select * from flatfile_table;
!set outputformat table

insert into threeclusters select * from flatfile_table;

insert into threeclusters select * from flatfile_table;

select * from threeclusters order by c0, c1;

truncate table threeclusters;

select * from threeclusters;

drop table threeclusters;
drop table flatfile_table;

-------------------------------------
-- Part 3. Cluster Projection test --
-------------------------------------
create table tencols(c0 int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int,
                        c7 int, c8 int, c9 int)
create clustered index i_c0 on tencols(c0)
create clustered index i_c1_c2 on tencols(c1, c2)
create clustered index i_c3_c4_c5 on tencols(c3, c4, c5)
create clustered index i_c6_c7_c8_c9 on tencols(c6, c7, c8, c9);
create index i_c9 on tencols(c9);
insert into tencols values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
insert into tencols values(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1);

!set outputformat csv
-- should select all clusters
explain plan for select * from tencols;
explain plan for select c4, c1, c8, c0 from tencols;

-- should select only one cluster
explain plan for select c7, c9 from tencols;
explain plan for select c5, c4, c3 from tencols;

-- should select two clusters
explain plan for select c0, c4, c5 from tencols;
explain plan for select c2, c6, c1, c9 from tencols;

-- should select three clusters
explain plan for select c8, c7, c3, c6, c2 from tencols;
explain plan for select c0, c1, c4, c5, c9 from tencols;

-- projection with filtering
explain plan for select c2, c4, c8, c0 from tencols
    where c0 = 0 and c2 = 2 and c4 = 4 and c8 = 8;
explain plan for select c7, c0, c5, c1 from tencols
    where c1 + c5 = 6;
explain plan for select c0, c5, c7, c1 from tencols
    where c0 + c2 = 2;
explain plan for select c0 + c1 from tencols where c0 = 0;
-- column referenced in filter is processed by the index scan so it should
-- not be projected
explain plan for select c0 from tencols where c9 = 9;

!set outputformat table

select c2, c8, c4, c0 from tencols
    where c0 = 0 and c2 = 2 and c4 = 4 and c8 = 8;
select c7, c0, c5, c1 from tencols
    where c1 + c5 = c6;
select c7, c0, c5, c1 from tencols
    where c0 + c2 = 2;
select c0 + c1 from tencols where c0 = 0;
select c0 from tencols where c9 = 9;

truncate table tencols;

select * from tencols;

drop table tencols;

-------------------------------------
-- Part 4. UDT test --
-------------------------------------

create type rectangle as (
    width double default 2.0,
    height double default 4.0
) final;

create table rectangles(name varchar(128), r rectangle, id int not null);

-- verify that three indexes are created (not four)
!indexes RECTANGLES

insert into rectangles values('Default', new rectangle(), 1);

-- can't execute this because we don't support returning UDT instances
-- via JDBC yet; but at least explain plan below should work
-- select * from rectangles t;

select id from rectangles t;

select t.r.width from rectangles t;

select t.r.height, name from rectangles t;

!set outputformat csv

explain plan for
insert into rectangles values('Default', new rectangle(), 1);

explain plan for
select * from rectangles t;

explain plan for
select id from rectangles t;

explain plan for
select t.r.width from rectangles t;

explain plan for
select t.r.height, name from rectangles t;

!set outputformat table

truncate table rectangles;

select count(*) from rectangles;

drop table rectangles;
drop type rectangle;

----------------------------------
-- Part 5. Rowcounts on lcs tables
----------------------------------
create server test_data
foreign data wrapper sys_file_wrapper
options (
    directory 'unitsql/optimizer/data',
    file_extension 'csv',
    with_header 'yes',
    log_directory 'testlog');

create foreign table matrix3x3(
    a tinyint,
    b integer,
    c bigint)
server test_data
options (filename 'matrix3x3');

create table rowcount(a tinyint, b int, c bigint);
create index irc1 on rowcount(a);
insert into rowcount select * from matrix3x3;
select * from rowcount order by 1;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

-- make sure create index doesn't wipe out the rowcount
create index irc2 on rowcount(b);
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

insert into rowcount values(-1, -2, -3);
select * from rowcount order by 1;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

delete from rowcount where a = 11;
select * from rowcount order by 1;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

-- make sure rebuild maintains the same rowcount but resets the deletion count
alter table rowcount rebuild;
select * from rowcount order by 1;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

-- make sure drop index doesn't wipe out rowcounts
drop index irc1;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

-- make sure truncate resets the rowcounts
truncate table rowcount;
select * from rowcount order by 1;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1 
    order by 1;

drop table rowcount;
drop server test_data cascade;

-------------------------------------
-- Part 6. Misc tests for bugfixes
-------------------------------------
-- Tests LER-312 -- should not create empty cluster pages when no data
-- is inserted into table
create table empty(a int);
create view v(a) as values(1);
insert into empty select a from v where a = 2;
select * from empty;
drop view v;
drop table empty;

-- Tests LER-31 -- Not handling appends to existing table when the records
-- are variable length and stored in either a compressed of variable mode
-- batch.  Prior testcases do exercise this, but the column size isn't long
-- enough to cause attempts to read invalid memory.  Note that the "null"
-- is what causes the testcase below to result in there being enough variation
-- in the record sizes to cause problems.

create table test_large_chars (a int, i char(100));

insert into test_large_chars values(1, null);
insert into test_large_chars values(2, 'asdf');
insert into test_large_chars values(3,
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');

select * from test_large_chars;
select * from test_large_chars where i is null;

drop table test_large_chars;

create table test_large_varchars (a int, i varchar(100));

insert into test_large_varchars values(1, null);
insert into test_large_varchars values(2, 'asdf');
insert into test_large_varchars values(3,
    'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');

select * from test_large_varchars;
select * from test_large_varchars where i is null;

drop table test_large_varchars;

-- LER-1402 -- the length of the data needs to be 510 to exercise this bug;
-- in the repro for LER_1402, the crash occurs on the scan; in this testcase,
-- the crash occurs on the insert
create table test_large_varchars (a int, i varchar(1000));
insert into test_large_varchars values(1,
'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');
insert into test_large_varchars values(1,
'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy');
select char_length(i), * from test_large_varchars order by a;
drop table test_large_varchars;

-- LER-3681 (verify that self-insert with a UDX in the middle does not
-- hang due to lockout)

create function stringify(c cursor, delimiter varchar(128))
returns table(v varchar(65535))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.stringify';

create table self_insert_udx(a char(10));
insert into self_insert_udx values('abc');
insert into self_insert_udx 
select * from table(stringify(cursor(select * from self_insert_udx), '|'));

-- End lcs.sql
