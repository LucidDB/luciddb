-- $Id$

-----------------------------
-- Delete tests on lcs tables
-----------------------------

create schema lcsdel;
set schema 'lcsdel';

create server test_data
foreign data wrapper sys_file_wrapper
options (
    directory 'unitsql/optimizer/data',
    file_extension 'csv',
    with_header 'yes', 
    log_directory 'testlog');

create foreign table matrix9x9(
    a1 tinyint,
    b1 integer,
    c1 bigint,
    a2 tinyint,
    b2 integer,
    c2 bigint, 
    a3 tinyint,
    b3 integer,
    c3 bigint) 
server test_data
options (filename 'matrix9x9');

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table deltab(
    a1 tinyint,
    b1 integer,
    c1 bigint,
    a2 tinyint,
    b2 integer,
    c2 bigint, 
    a3 tinyint,
    b3 integer,
    c3 bigint);
create index i on deltab(b1);

-- delete on a real empty table
delete from deltab;
select * from deltab;

insert into deltab select * from matrix9x9;
insert into deltab select * from matrix9x9;
select lcs_rid(a1), * from deltab order by 1;

-- fake stats to ensure that index is used
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LCSDEL', 'DELTAB', 1000);

delete from deltab where a1 < 30 and lcs_rid(a1) >= 9;
select lcs_rid(a1), * from deltab order by 1;

-- delete and select via index
delete from deltab where b1 in (12, 42);
select lcs_rid(a1), * from deltab where b1 > 0 order by 1;

delete from deltab where b1 > 50 and b1 < 70 and lcs_rid(a1) < 9;
select lcs_rid(a1), * from deltab where b1 > 0 order by 1;

-- input into the delete is a join
delete from deltab where c1 in
    (select max(c1) from deltab union select min(c1) from deltab);
select lcs_rid(a1), * from deltab order by 1;

-- empty delete
delete from deltab where a2 > 100;
select lcs_rid(a1), * from deltab order by 1;

-- delete what's left in the table
delete from deltab;
select * from deltab;

-- empty delete on an empty table
delete from deltab;
select * from deltab;

-- Insert into a table that contains deleted rows.
-- First, try the empty case.
insert into deltab select * from deltab;
select * from deltab;
select * from deltab where b1 >= 0;

-- Then, try the case where there are rows
insert into deltab select * from matrix9x9;
select * from deltab order by a1;
select * from deltab where b1 >= 0 order by a1;

-- no-op deletes
delete from deltab where 1 = 0;
delete from deltab where false;
select count(*) from deltab;
delete from deltab where (select count(*) from deltab) = 0;
select count(*) from deltab;
delete from deltab where 1 = 0 and a1 in (select b1 from deltab);
select count(*) from deltab;

!set outputformat csv
explain plan for
    delete from deltab where c1 in 
        (select max(c1) from deltab union select min(c1) from deltab);
explain plan for
    delete from deltab where 1 = 0;
explain plan for
    delete from deltab where (select count(*) from deltab) = 0;
explain plan for
    delete from deltab where 1 = 0 and a1 in (select b1 from deltab);

drop server test_data cascade;

-- test rejected rows at deletion
alter system set "calcVirtualMachine"='CALCVM_JAVA';
alter session set "logDir" = 'testlog';
create schema TEST_REJECTED_ROWS;
set schema 'TEST_REJECTED_ROWS';
alter session set "errorMax" = 10;
create table t(a int);
insert into t values(100000);
delete from t where cast(a as smallint) = 1; 
select * from sys_boot.mgmt.session_parameters_view
     where param_name = 'lastRowsRejected';
drop schema TEST_REJECTED_ROWS cascade;

