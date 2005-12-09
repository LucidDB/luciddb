-----------------------------------
-- LucidDB COlumn Store SQL test --
-----------------------------------

create schema lcs;
set schema 'lcs';
set path 'lcs';

----------------------------------------
-- Part 1. Single Cluster Loader test --
----------------------------------------
--
-- 1.1 One cluster of a single column.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(empno int) server sys_column_store_data_server;

-- verify creation of system-defined clustered index
!indexes LCSEMPS

-- verify that explain plan works
explain plan with implementation for insert into lcsemps values(10);

explain plan with implementation for 
insert into lcsemps select empno from sales.emps;

-- verify that insert values works
insert into lcsemps values(10);

-- verify that insert select works
insert into lcsemps select empno from sales.emps;

-- TODO: 2005-12-06(rchen) this would fail since query is not supported yet
-- select * from lcsemps;

-- verify truncate works
truncate table lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.2 Two clusters of a single column each.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(empno int, name varchar(128)) server sys_column_store_data_server;

-- verify creation of system-defined clustered indices
!indexes LCSEMPS

-- verify that explain plan works
explain plan with implementation for insert into lcsemps values(10, 'Selma');

explain plan with implementation for 
insert into lcsemps select empno, name from sales.emps;

-- verify that insert values works
-- TODO: 2005-12-06(rchen) This does not work yet.
-- insert into lcsemps values(10, 'Selma');

-- verify that insert select works
-- TODO: 2005-12-06(rchen) This does not work yet.
-- insert into lcsemps select empno, name from sales.emps;

-- TODO: 2005-12-06(rchen) this would fail since query is not supported yet
-- select * from lcsemps;

-- verify truncate works
truncate table lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.3 One cluster of three columns.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(empno int, name varchar(128), empid int) 
server sys_column_store_data_server
create clustered index explicit_lcsemps_all on lcsemps(empno, name, empid);

-- verify creation of system-defined clustered indices
!indexes LCSEMPS

-- verify that explain plan works
explain plan with implementation for insert into lcsemps values(10, 'Selma', 10000);

explain plan with implementation for 
insert into lcsemps select empno, name, empid from sales.emps;

-- verify that insert values works
insert into lcsemps values(10, 'Selma', 10000);

-- verify that insert select works
insert into lcsemps select empno, name, empid from sales.emps;

-- TODO: 2005-12-06(rchen) this would fail since query is not supported yet
-- select * from lcsemps;

-- verify truncate works
truncate table lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.4 One cluster of a single column.
--     Testing inserting NULLs and empty strings.
--
-- Without specifying the clustered index clause in create table, a default 
-- index will be created for each column.
-- Also, LCS tables do not require primary keys.
create table lcsemps(city varchar(20)) server sys_column_store_data_server;

-- verify creation of system-defined clustered index
!indexes LCSEMPS

-- alter system set "calcVirtualMachine" = 'CALCVM_JAVA';
-- alter system set "calcVirtualMachine" = 'CALCVM_AUTO';

-- verify that explain plan works
explain plan with implementation for insert into lcsemps values(NULL);
explain plan with implementation for insert into lcsemps values('');
explain plan with implementation for insert into lcsemps values('Pescadero');

-- Locate a NULL value from the source table.
select city from sales.emps where empno = 100;

-- Plans with NULL in the populating stream
explain plan with implementation for 
insert into lcsemps select city from sales.emps;

explain plan with implementation for 
insert into lcsemps select city from sales.emps where empno = 100;

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

-- TODO: 2005-12-06(rchen) this would fail since query is not supported yet
-- select * from lcsemps;

-- verify truncate works
truncate table lcsemps;

-- drop lcsemps
drop table lcsemps;


--
-- 1.5 Bug case
--
create table lcsemps(city varchar(20)) server sys_column_store_data_server;

insert into lcsemps select city from sales.emps;

insert into lcsemps select city from sales.emps;

-- NOTE: 2005-12-06(rchen) this used to fail with:
-- java: SXMutex.cpp:144: bool fennel::SXMutex::tryUpgrade(): Assertion `!nExclusive' failed.
-- It's now fixed by allocating a brand new LcsClusterNodeWriter for every LcsClusterAppendExecStream::open()
insert into lcsemps values(NULL);

-- TODO: 2005-12-06(rchen) this would fail since query is not supported yet
-- select * from lcsemps;

-- verify truncate works
truncate table lcsemps;

drop table lcsemps;


--
-- 1.6 Bugcase
--
create table lcsemps(city varchar(20)) server sys_column_store_data_server;

insert into lcsemps select city from sales.emps;

insert into lcsemps select city from sales.emps;

-- TODO: 2005-12-06(rchen) this would fail since query is not supported yet
-- select * from lcsemps;

-- verify truncate works
truncate table lcsemps;

-- NOTE: 2005-12-06(rchen) this used to fail with:
-- java: ../../fennel/cache/CacheMethodsImpl.h:299: void fennel::CacheImpl<PageT, VictimPolicyT>::discardPage:Assertion `page->nReferences == 1' failed.
-- It's now fixed by allocating a brand new LcsClusterNodeWriter for every LcsClusterAppendExecStream::open()
drop table lcsemps;


---------------------------------------
-- Part 2. Multi-cluster Loader test --
---------------------------------------


---------------------------------
-- Part 3. Cluster Reader test --
---------------------------------


--
-- Clean up
--
-- drop schema
drop schema lcs;

-- End lcs.sql
