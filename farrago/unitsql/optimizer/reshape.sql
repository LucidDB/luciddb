-- $Id$

------------------------------------
-- SQL Tests for Reshape exec stream
------------------------------------

create schema reshape;
set schema 'reshape';

alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

create table t1(
    t1id int not null,
    t1a char(5) not null,
    t1b varchar(10),
    pk int primary key);
create table t2(
    t2id int,
    t2a varchar(15),
    t2b char(20),
    pk int primary key);
create table trunc(
    id int,
    a varchar(3),
    b varchar(2),
    pk int primary key);

insert into t1 values(1, 't1a1', 't1b1', 1);
insert into t1 values(2, 't1a2', 't1b2', 2);
insert into t1 values(3, 't1a3', 't1b3', 3);
insert into t1 values(4, 't1a4', 't1b4', 4);
insert into t1 values(5, 't1a5', 't1b5', 5);

insert into t2 values(1, 't2a1', 't2b1', 1);
insert into t2 values(2, 't2a2', 't2b2', 2);
insert into t2 values(3, 't2a3', 't2b3', 3);
insert into t2 values(4, 't2a4', 't2b4', 4);
insert into t2 values(5, 't2a5', 't2b5', 5);
insert into t2 values(5, null, null, 6);

!set outputformat csv
--------------------------------
-- the following are reshapeable
--------------------------------
-- simple project
explain plan for select t2a, t1b, t1a, t2b from t1, t2;

-- projections with simple casts
explain plan for select cast(t2id as integer) from t2;
explain plan for 
    select cast(t2b as varchar(10)), cast(t2a as varchar(5)) from t2;

-- casting in an insert-select that will truncate the data
explain plan for
    insert into trunc select * from t1;

-- filters that are supported
-- ='s
explain plan for
    select * from t1 where t1id = 1 and t1b = 't1b1';
explain plan for
    select * from t1 where t1b = 't1b1' and t1id = 1;
explain plan for
    select * from t2 where 't2a2' = t2a and 2 = t2id;
-- range operators
explain plan for
    select * from t2 where t2id >= 3;
explain plan for
    select * from t1 where t1a > 't1a2';
explain plan for
    select * from t1 where t1b <= 't1b3';
explain plan for
    select * from t2 where t2a < 't2a4';
explain plan for
    select * from t2 where 't2b3' >= t2b;
explain plan for
    select * from t1 where 't1a3' > t1a;
explain plan for
    select * from t1 where 't1b4' <= t1b;
explain plan for
    select * from t2 where 't2a4' < t2a;

-- range used with an equal
explain plan for select * from t1 where t1id = 3 and t1b >= 't1b1';
explain plan for select * from t2 where t2id = 5 and t2b < 't3';

explain plan for select * from t2 where t2a is null;

-- both project and filter
explain plan for
    select cast(t2b as varchar(10)), cast(t1b as varchar(5))
        from t1, t2 where t1.t1id = 1 and t2.t2id = 1;
explain plan for
    select cast(t1b as varchar(5)) from t1 where t1id = 3;

-----------------------------------
-- the following aren't reshapeable
-----------------------------------
-- non input refs/casts
explain plan for select 1 from t1;
explain plan for select t1id + 10 from t1;
-- cast on an expression
explain plan for select cast(upper(t1a) as varchar(10)) from t1;

-- casts that aren't supported
explain plan for select cast(t1id as integer) from t1;
explain plan for select cast(t1a as char(10)) from t1;
explain plan for select cast(t1a as char(5)) from t1;
explain plan for select cast(t1id as smallint) from t1;

-- non-AND expression
explain plan for select * from t1 where t1id = 1 or t1a = 't1a2';

-- invalid comparison operators
explain plan for select * from t1 where t1b = 't1b1' and t1id in (1, 2);
explain plan for select * from t2 where true;
create table b(a int primary key, b boolean, c int);
explain plan for select * from b where c = 1 and b;
explain plan for select * from t2 where t2a <> 't2a3';

-- not comparing to a literal
explain plan for select * from t1 where t1a = t1b;
explain plan for select * from t1 where t1a > t1b;

-- multiple range predicates
explain plan for select * from t1 where t1a > 't1a1' and t1a < 't1a4';

-- not comparing against an input reference
explain plan for select * from t1 where lower(t1b) = 't1b1';

-- comparing against the same column twice
explain plan for select * from t1 where t1id = 1 and t1id = 2;
explain plan for select * from t1 where t1id = 1 and t1id > 2;
explain plan for select * from t1 where t1id > 1 and t1id = 2;

------------------------------------
-- run the ones that are reshapeable
------------------------------------
!set outputformat table

select t2a, t1b, t1a, t2b from t1, t2 order by 1, 2, 3, 4;

select cast(t2id as integer) from t2 order by 1;
select cast(t2b as varchar(10)), cast(t2a as varchar(5)) from t2 order by 1, 2;

insert into trunc select * from t1;
select * from trunc order by 1, 2;

select * from t1 where t1id = 1 and t1b = 't1b1' order by t1id;
select * from t1 where t1b = 't1b1' and t1id = 1 order by t1id;
select * from t2 where 't2a2' = t2a and 2 = t2id order by t2id;

select * from t2 where t2id >= 3 order by t2id;
select * from t1 where t1a > 't1a2' order by t1id;
select * from t1 where t1b <= 't1b3' order by t1id;
select * from t2 where t2a < 't2a4' order by t2id;
select * from t2 where 't2b3' >= t2b order by t2id;
select * from t1 where 't1a3' > t1a order by t1id;
select * from t1 where 't1b4' <= t1b order by t1id;
select * from t2 where 't2a4' < t2a order by t2id;

select * from t1 where t1id = 3 and t1b >= 't1b1';
select * from t2 where t2id = 5 and t2b < 't3';

select * from t2 where t2a is null;

select cast(t2b as varchar(10)), cast(t1b as varchar(5))
    from t1, t2 where t1.t1id = 1 and t2.t2id = 1 order by 1, 2;
select cast(t1b as varchar(5)) from t1 where t1id = 3 order by 1;

-------------------------
-- LucidDb-specific tests
-------------------------
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table d1(a decimal(10, 1));
create table d2(a decimal(10, 1) not null);
create table d3(a decimal(10, 2) not null);

insert into d1 values(1.1);
insert into d2 values(1.1);

-- the two join columns are of two types because one allows nulls while the
-- other doesn't; hash join requires them to be the same so it will cast the
-- non-nullable one to allow nulls; reshape does handle this; but not the
-- second query because the types are different
!set outputformat csv
explain plan for select * from d1, d2 where d1.a = d2.a;
explain plan for select * from d1, d3 where d1.a = d3.a;

!set outputformat table
select * from d1, d2 where d1.a = d2.a;
