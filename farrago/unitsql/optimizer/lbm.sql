-- $Id$

-------------------------------------
-- Sql level test for Bitmap Index --
-------------------------------------

create schema lbm;
set schema 'lbm';
set path 'lbm';

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

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

-----------------------------------------------------
-- Part 1. Indexes based on single column clusters --
-----------------------------------------------------

--
-- 1.1 One index on a table without a primary key
--
create table single(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server;

create index single_one 
on single(b);

insert into single values (1,2,3);
insert into single values (0,0,0);

insert into single 
select * from matrix3x3;

--
-- 1.1.1 Create index on existing data
--
drop index single_one;

create index single_one_recreated
on single(b);

--
-- 1.1.2 Try a few range selections
--

select * from single where b < 10;

insert into single 
select * from matrix3x3;

select * from single where b > 15;
select * from single where b < 30;
select * from single where b > 15 and b < 30;
select * from single where b > 50;

drop table single;

--
-- 1.2 One multi-column index on a table without a primary key
-- TODO: once we support delete, we will not need to drop table every time
--
create table single(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server;

create index single_one_multi 
on single(b, c);

insert into single 
select * from matrix3x3;

drop table single;

--
-- 1.3 Several single column indexes on a table without a primary key
--
create table single(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server;

set schema 'lbm';

create index single_two_b
on single(b);

create index single_two_c
on single(c);

insert into single 
select * from matrix3x3;

drop table single;

--
-- 1.4 A table with a primary key (the constraint is itself an index)
--
create table single(
    a tinyint,
    b integer primary key,
    c bigint) 
server sys_column_store_data_server;

insert into single 
select * from matrix3x3;

drop table single;


----------------------------------------------------
-- Part 2. Indexes based on multi column clusters --
----------------------------------------------------

--
-- 2.1 An index with multiple columns, ordered
--
create table multi(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server
create clustered index multi_all on multi(a, b, c);

create index multi_multikey on multi(a, b);

insert into multi 
select * from matrix3x3;

insert into multi 
select * from matrix3x3;

insert into multi 
select * from matrix3x3;

insert into multi 
select * from matrix3x3;

drop table multi;

--
-- 2.2 An index with multiple columns, rearranged
--
create table multi(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server
create clustered index multi_all on multi(a, b, c);

create index multi_multikey on multi(c, a, b);

insert into multi 
select * from matrix3x3;

drop table multi;

--
-- 2.3 Multiple single columns indexes
--
create table multi(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server
create clustered index multi_all on multi(a, b, c);

create index multi_singlekey_b on multi(b);
create index multi_singlekey_c on multi(c);

insert into multi 
select * from matrix3x3;

drop table multi;

--
-- 2.4 Multiple multi columns indexes
--
create table multi(
    a tinyint,
    b integer,
    c bigint) 
server sys_column_store_data_server
create clustered index multi_all on multi(a, b, c);

create index multi_multikey_cb on multi(c, b);
create index multi_multikey_ba on multi(b, a);

insert into multi 
select * from matrix3x3;

-- try some nulls, and reverse data
create foreign table matrix3x3_alt(
    a tinyint,
    b integer,
    c bigint)
server test_data
options (filename 'matrix3x3_alt');

insert into multi
select * from matrix3x3_alt;

-- some more data
insert into multi 
select * from matrix3x3;

drop table multi;


-------------------------------------------------------------
-- Part 3. Indexes based on multiple multi column clusters --
-------------------------------------------------------------

--
-- 3.1 Indexes based on subsets of clusters
--
create table multimulti(
    a1 tinyint,
    b1 integer,
    c1 bigint,
    a2 tinyint,
    b2 integer,
    c2 bigint, 
    a3 tinyint,
    b3 integer,
    c3 bigint) 
server sys_column_store_data_server
create clustered index multi_1 on multimulti(a1, b1, c1)
create clustered index multi_2 on multimulti(a2, b2, c2)
create clustered index multi_3 on multimulti(a3, b3, c3);

create index multimulti_subset_a on multimulti(b1);
create index multimulti_subset_b on multimulti(a2,b2);
create index multimulti_subset_c on multimulti(c2,a2);

insert into multimulti
select * from matrix9x9;

drop table multimulti;

--
-- 3.2 Indexes based on multiple clusters
--
create table multimulti(
    a1 tinyint,
    b1 integer,
    c1 bigint,
    a2 bigint,
    b2 integer,
    c2 tinyint, 
    a3 bigint,
    b3 tinyint,
    c3 integer) 
server sys_column_store_data_server
create clustered index multi_1 on multimulti(a1, b1, c1)
create clustered index multi_2 on multimulti(a2, b2, c2)
create clustered index multi_3 on multimulti(a3, b3, c3);

create index multimulti_mixed_a on multimulti(a1,a2,a3);
create index multimulti_mixed_b on multimulti(b1,b2,b3);
create index multimulti_mixed_c on multimulti(c1,c2,b2);

insert into multimulti
select * from matrix9x9;

-- some alternate data (descending, nulls)
create foreign table matrix9x9_alt(
    a1 tinyint,
    b1 integer,
    c1 bigint,
    a2 bigint,
    b2 integer,
    c2 tinyint, 
    a3 bigint,
    b3 tinyint,
    c3 integer) 
server test_data
options (filename 'matrix9x9_alt');

insert into multimulti
select * from matrix9x9_alt;

insert into multimulti
select * from matrix9x9;

insert into multimulti
select * from matrix9x9_alt;

insert into multimulti
select * from matrix9x9;

insert into multimulti (a2, b2, c2)
select * from matrix3x3;

select * from multimulti order by a1, b1, c1, a2, b2, c2, a3, b3, c3;
-- insert target is same as source target
insert into multimulti
    select -a1, -b1, -c1, -a2, -b2, -c2, -a3, -b3, -c3 from multimulti
        where a1 = 11 and b1 = 12;
select * from multimulti order by a1, b1, c1, a2, b2, c2, a3, b3, c3;

-- Test for residual filter bug
-- force plan without index access
-- and use only residual filters
drop index multimulti_mixed_a;
drop index multimulti_mixed_b;
drop index multimulti_mixed_c;

!set outputformat csv
explain plan for
select count(*) from multimulti where a1 = 11 and b1 = 12;
!set outputformat table
select count(*) from multimulti where a1 = 11 and b1 = 12;

!set outputformat csv
explain plan for
select count(*) from multimulti where a1 = 21 and a2 = 24;
!set outputformat table
select count(*) from multimulti where a1 = 21 and a2 = 24;

!set outputformat csv
explain plan for
select count(*) from multimulti where a1 = 31 and a3 = 37;
!set outputformat table
select count(*) from multimulti where a1 = 31 and a3 = 37;

drop table multimulti;


-------------------------------------------------------------
-- Part 4. Data types, null values                         --
-------------------------------------------------------------
-- NOTE: cannot input a binary field from a flat file yet
create foreign table typed_src(
    a int,
    d char(10),
    e decimal(6,2),
    f smallint,
    g real,
    h double,
    i varchar(256),
    j boolean,
    k date,
    l time,
    m timestamp,
    n numeric(10,2)) 
server test_data
options (filename 'typed');

create table typed(
    a int,
    b varbinary(256),
    c binary(10),
    d char(10),
    e decimal(6,2),
    f smallint,
    g real,
    h double,
    i varchar(256),
    j boolean,
    k date,
    l time,
    m timestamp,
    n numeric(10,2)) 
server sys_column_store_data_server;

create index typed_a on typed(a);
create index typed_b on typed(b);
create index typed_c on typed(c);
create index typed_d on typed(d);
create index typed_e on typed(e);
create index typed_f on typed(f);
create index typed_g on typed(g);
create index typed_h on typed(h);
create index typed_i on typed(i);
create index typed_j on typed(j);
create index typed_k on typed(k);
create index typed_l on typed(l);
create index typed_m on typed(m);
create index typed_n on typed(n);

-- LER-422: merging with singleton 
-- http://jira.lucidera.com/browse/LER-422
insert into typed values(
    1,X'deadbeef',null,'first',
    0.16,1,1,1,
    '1st',true,DATE'2001-11-11',TIME'23:11:08',
    TIMESTAMP'2001-11-11 23:11:08',0.02);

-- the singleton bitmap entry on index(b)
-- key(b) = null, RID == 1
insert into typed values(
    2,null,null,null,
    null,null,null,null,
    null,null,null,null,
    null,null);
insert into typed values(
    3,X'1ead2006',null,'third',
    0.16,1,1,1,
    '3rd',false,DATE'2001-12-12',TIME'23:11:08',
    TIMESTAMP'2001-12-12 23:11:08',0.06);

-- the new bitmap entry with three RIDs set
-- key(b) = null RID == 2,3,4
-- the new bitmap will be merged with the singleton inserted above.
insert into typed (a,d,e,f,g,h,i,j,k,l,m,n)
select * from typed_src;


-------------------------------------------------------------
-- Part 5. Minus stream                                    --
-------------------------------------------------------------

alter session implementation set jar
    sys_boot.sys_boot.luciddb_index_only_plugin;

-- LER-3491
create table t(a int);
create index it on t(a);
insert into t values (10), (11), (12), (13), (14), (15), (16), (17);
insert into t values(0);
delete from t where a = 10;

select a, count(*) from t group by a having a = 10;

-------------------------------------------------------------
-- Part 6. Misc tests for bugfixes
-------------------------------------------------------------

-- FNL-63 -- multiple nulls in a unique constraint column
create table null_src(
  pkey int,
  colbigint bigint,
  colvar varchar(20),
  colchar char(20),
  colint int
);

insert into null_src values
(null, null, null, null, null),
(3, null, 'three2', 'three2', 32),
(1, 10000, 'one', 'ten-thousand', 10000),
(2, 30, 'two', 'thirty', 60),
(3, null, 'three', null, null),
(2, 30, 'two', 'forty', 80),
(null, 10, null, 'ten', null),
(4, 40, 'four', 'forty', 160);

alter session set "errorMax"=10;
alter session set "logDir" = 'testlog';

create table null_uc_sk(
  pkey int,
  colbigint bigint,
  colvar varchar(20),
  colchar char(20),
  colint int,
  constraint n_pkey_unique UNIQUE(pkey, colbigint)
);

!set showwarnings true
insert into null_uc_sk select * from null_src;
select * from null_uc_sk order by pkey, colbigint, colint;
select * from null_uc_sk where pkey is null order by pkey, colbigint, colint;
select * from null_uc_sk where pkey = 3 order by pkey, colbigint, colint;

-- verify that minus stream restart works correctly when doing a keyonly scan
-- on a composite index where only a partial key is read
create table minus(a int, b int, c int);
create index iminus on minus(a, b);
insert into minus values(0,0,0);
insert into minus values(0,1,1);
insert into minus values(0,2,2);
insert into minus values(0,3,3);
insert into minus values(1,0,4);
insert into minus values(1,1,5);
insert into minus values(1,2,6);
insert into minus values(1,3,7);
insert into minus values(0,0,8);
insert into minus values(0,1,9);
insert into minus values(0,2,10);
insert into minus values(0,3,11);
insert into minus values(1,0,12);
insert into minus values(1,1,13);
insert into minus values(1,2,14);
insert into minus values(1,3,15);
insert into minus values(0,0,16);
delete from minus where c in (1,16);
-- fake stats so index is chosen
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'MINUS', 100);
!set outputformat csv
explain plan for select a, count(*) from minus group by a;
!set outputformat table
select a, count(*) from minus group by a;

truncate table minus;
insert into minus values(0,1,0);
insert into minus values(0,1,1);
insert into minus values(0,1,2);
insert into minus values(0,1,3);
insert into minus values(0,1,4);
insert into minus values(0,1,5);
insert into minus values(0,1,6);
insert into minus values(0,1,7);
insert into minus values(0,2,8);
insert into minus values(0,2,9);
insert into minus values(0,2,10);
insert into minus values(0,2,11);
insert into minus values(0,2,12);
insert into minus values(0,2,13);
insert into minus values(0,2,14);
insert into minus values(0,2,15);
insert into minus values(0,3,16);
insert into minus values(0,3,17);
insert into minus values(0,3,18);
insert into minus values(0,3,19);
insert into minus values(0,3,20);
insert into minus values(0,3,21);
insert into minus values(0,3,22);
insert into minus values(0,3,23);
insert into minus values(0,1,24);
insert into minus values(0,4,25);
insert into minus values(0,4,26);
insert into minus values(0,4,27);
insert into minus values(0,4,28);
insert into minus values(0,4,29);
insert into minus values(0,4,30);
insert into minus values(0,4,31);
insert into minus values(1,4,32);
delete from minus where c = 24 or c = 16;
-- fake stats so index is chosen
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'MINUS', 100);
!set outputformat csv
explain plan for select a, count(*) from minus group by a;
!set outputformat table
select a, count(*) from minus group by a;

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- LER-5800
create table t1(a int);
create index it1 on t1(a);
create table t2(a int);
insert into t2 values(2), (2), (2);
insert into t1 values(null), (1), (1), (1), (2);
insert into t1 select * from t2;
insert into t1 values(2);
select * from t1 order by a;

-- incur the same unique constraint violation multiple times
-- error logging has already been enabled above
create table u(a int unique);
insert into u values(1);
insert into u values(1);
insert into u values(1);

-- LER-8002
insert into u values (2),(3),(4),(5),(6),(7),(8),(9),(10);
delete from u where a = 10;
insert into u select * from u;
select * from u order by a;

-- LER-9058
-- A varchar column size of 16380 multipled by 2 equals 32K; a size of 16382
-- multipled by 2 will exceed 32K
create table vc(
    a varchar(16380), b varchar(16382), c varchar(32768), d varchar(65535));
create index ivc1 on vc(a);
create index ivc2 on vc(b);
create index ivc3 on vc(c);
create index ivc4 on vc(d);
insert into vc values
    ('a1', 'b1', 'c1', 'd1'),
    ('a2', 'b2', 'c2', 'd2'),
    ('a3', 'b3', 'c3', 'd3'),
    ('a4', 'b4', 'c4', 'd4');
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'LBM', 'VC', 10000);
-- make sure the index is used
!set outputformat csv
explain plan for select * from vc where a = 'a1';
explain plan for select * from vc where b = 'b2';
explain plan for select * from vc where c = 'c3';
explain plan for select * from vc where d = 'd4';
!set outputformat table
select * from vc where a = 'a1';
select * from vc where b = 'b2';
select * from vc where c = 'c3';
select * from vc where d = 'd4';

create table vc2(a varchar(40));
insert into vc2 values
    (null),(null),(null),(null),(null),(null),(null),(null),(null),(null);
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
insert into vc2 select * from vc2;
create index i_vc2 on vc2(a);
delete from vc2 where lcs_rid(a) = 0;
!set outputformat csv
explain plan for select count(*) from vc2 where a is null;
!set outputformat table
select count(*) from vc2 where a is null;

-- LER-10217 -- Note these testcases may not cause a failure even w/o the fix
-- for LER-10217, as that bug requires the data to be sorted in a particular
-- order, which is non-deterministic.  However, they do exercise the codepaths
-- introduced by the fix.
create table t5(
    a varchar(4000), b varchar(4000), c varchar(4000), d varchar(4000), 
    e varchar(4000));
create index it5a on t5(a);
create index it5b on t5(b);
create index it5c on t5(c);
create index it5d on t5(d);
create index it5e on t5(e);
create table src(a varchar(4000));
insert into src values
    ('a'),('a'),('b'),('c'),('d'),('e'),('a'),('a');
insert into src select * from src;
insert into src select * from src;
insert into t5 select a, a, a, a, a from src;
select * from t5 where a = 'a';
-- make sure the index was used
!set outputformat csv
explain plan for select * from t5 where a = 'a';
!set outputformat table

create table t3(a varchar(4000), b varchar(4000), c varchar(4000));
create index it3a on t3(a);
create index it3b on t3(b);
create index it3c on t3(c);
truncate table src;
insert into src values
    ('a'),('b'),('c'),('d'),('e'),('f'),('g'),('h'),('a'),('i'),('a'),('b'),
    ('c'),('d'),('e'),('f');
insert into src select * from src;
insert into t3 select a, a, a from src;
select * from t3 where a = 'a';
-- make sure the index was used
!set outputformat csv
explain plan for select * from t3 where a = 'a';

-- cleanup
drop server test_data cascade;

-- End lbm.sql

