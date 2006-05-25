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

!set outputformat csv
explain plan for
    delete from deltab where c1 in 
        (select max(c1) from deltab union select min(c1) from deltab);

drop server test_data cascade;
