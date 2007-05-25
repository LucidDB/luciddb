-- $Id$
-- Test optimizer rules for pushing down filters

create schema pdf;
set schema 'pdf';

create table t1(
    k1 int primary key,
    t1a int not null,
    t1b int not null,
    t1c int not null,
    t1d int not null);
create table t2(k2 int primary key, t2a int, t2b int, t2c int, t2d int);
create table t3(k3 int primary key, t3a int, t3b int, t3c int, t3d int);

insert into t1 values(1, 11, 12, 13, 14);
insert into t1 values(2, 21, 22, 23, 24);
insert into t1 values(3, 31, 32, 33, 34);
insert into t1 values(4, 41, 42, 43, 44);
insert into t1 values(5, 51, 52, 53, 54);
insert into t2 select * from t1;
insert into t3 select * from t1;

!set outputformat csv

-------------------------------
-- pushdown table level filters
-------------------------------

explain plan for
    select * from t1, t2 where
        t1a  = 1 order by k1, k2;

explain plan for
    select * from t1, t2 where
        t1a >= 11 and 21 <= t2a and (t2b < 32 or t2c > 43) and t1d = t2d and
        t1b + 1 = t1c order by k1;

-- filters in the on clause
explain plan for
    select * from t1 inner join t2 on
        t1a >= 11 and 21 <= t2a and (t2b < 32 or t2c > 43) and t1d = t2d and
        t1b + 1 = t1c order by k1;

-- filters in both on clause and where
explain plan for
    select * from t1 inner join t2
        on t1d = t2d and t1b + 1 = t1c
        where t1a >= 11 and 21 <= t2a and (t2b < 32 or t2c > 43)
        order by k1;

-- outer joins
-- need to join on key columns as outer joins not supported otherwise

-- can push to the left since it doesn't generate nulls
explain plan for
    select * from t1 left outer join t2
        on k1 = k2
        where t1a = 11;

-- can't push to the right from where clause since right generates nulls
explain plan for
    select * from t1 left outer join t2
        on k1 = k2
        where t2a = 11;

-- FRG-158 -- filters that always evaluate to true aren't pushed
explain plan for
    select t1a from t1 left outer join (select *, true as x from t2) on true
        where x is true;

------------------------
-- pushdown join filters
------------------------

-- create indexes on the tables to make pushing down joins worthwhile

create index it2 on t2(t2a);
create index it3 on t3(t3b);

explain plan for
    select * from t1, t2, t3 where
        t1a = t2a and t2b = t3b
        order by k1, k2;

-- both table filters and joins
-- note that cartesian joins will end up getting used because the filters
-- reduce the size of the inputs into the joins; but you should still see
-- the filters being pushed to their respective tables and joins

explain plan for
    select * from t1, t2, t3 where
        t1a = t2a and t2b = t3b and t1b > 12 and t2c > 23 and t3d = 34;

-- join filter that references all 3 columns and therefore can only be pushed
-- to the topmost join
explain plan for
    select * from t1, t2, t3 where
        t1a /10 = t2b / 10 + t3c / 10
        order by k1, k2;

-------------------------------
-- push down filter past setops
-------------------------------
explain plan for
    select * from (select * from t1 union select * from t2) where t1a < 41;
explain plan for
    select * from (select * from t1 union all select * from t2) where t1a < 41;
explain plan for
    select * from
        (select t1a, t1b from t1 union
            select t2b, t2a from t2 union
            select t3c, t3b from t3)
        where t1b in (12, 21, 32);

-----------------------------------------------------------------
-- run queries just to make sure the plans created are executable
-----------------------------------------------------------------
!set outputformat table

select * from t1, t2 where
    t1a = 11 order by k1, k2;

select * from t1, t2 where
    t1a >= 11 and 21 <= t2a and (t2b < 32 or t2c > 43) and t1d = t2d and
    t1b + 1 = t1c order by k1;

select * from t1 inner join t2 on
    t1a >= 11 and 21 <= t2a and (t2b < 32 or t2c > 43) and t1d = t2d and
    t1b + 1 = t1c order by k1;

select * from t1 inner join t2
    on t1d = t2d and t1b + 1 = t1c
    where t1a >= 11 and 21 <= t2a and (t2b < 32 or t2c > 43)
    order by k1;

select * from t1 left outer join t2
    on k1 = k2
    where t1a = 11;

select * from t1 left outer join t2
    on k1 = k2
    where t2a = 11;

select * from t1, t2, t3 where
    t1a = t2a and t2b = t3b
    order by k1;

select * from t1, t2, t3 where
    t1a = t2a and t2b = t3b and t1b > 11 and t2c > 23 and t3d = 34;

select * from t1, t2, t3 where
    t1a /10 = t2b / 10 + t3c / 10
    order by k1, k2;

select * from (select * from t1 union select * from t2) where t1a < 41
    order by k1;

select * from (select * from t1 union all select * from t2) where t1a < 41
    order by k1;

select * from
    (select t1a, t1b from t1 union
        select t2b, t2a from t2 union
        select t3c, t3b from t3)
    where t1b in (12, 21, 32)
    order by t1a;

----------------------------------------------------------------
-- run a query through Volcano to exercise rules more thoroughly
----------------------------------------------------------------
!set outputformat csv
alter session implementation add jar sys_boot.sys_boot.volcano_plugin;
explain plan for
    select t1a from t1 left outer join (select *, true as x from t2) on true
        where x is true;

------------------------------
-- LucidDb-specific operations
------------------------------
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
create table lt1(
    t1a int unique not null,
    t1b int unique not null,
    t1c int unique not null,
    t1d int unique not null);
create table lt2(
    t2a int unique, t2b int unique, t2c int unique, t2d int unique);
create table lt3(
    t3a int unique, t3b int unique, t3c int unique, t3d int unique);
insert into lt1 select t1a, t1b, t1c, t1d from t1;
insert into lt2 select t2a, t2b, t2c, t2d from t2;
insert into lt3 select t3a, t3b, t3c, t3d from t3;

-- fake row count so that index access is considered
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'PDF', 'LT1', 100);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'PDF', 'LT2', 100);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'PDF', 'LT3', 100);

explain plan for
    select * from
        (select t1c, t1d from lt1 intersect select t2b+1, t2a+3 from lt2)
            where t1d > 30;

explain plan for
    select * from
        (select t1a, t1b, t1c from lt1 except
            select t2b - 1, t2c - 1, t2d - 1 from lt2 where t2a < 20)
    where t1c > 25;

-- the following exercises pushing the filter past the setop as well as
-- merging the pushed filter with the filter already on top of the scan;
-- if the merge is being done, then indexes should be used for all of the
-- filters
explain plan for
    select * from
        (select t1a, t1b from lt1 where t1a = 31 union
            select t2b, t2a from lt2 union
            select t3c, t3b from lt3 where t3c = 13)
        where t1b in (12, 21, 32);

!set outputformat table
select * from
    (select t1c, t1d from t1 intersect select t2b+1, t2a+3 from t2)
        where t1d > 30
    order by t1c;

select * from
    (select t1a, t1b, t1c from lt1 except
        select t2b - 1, t2c - 1, t2d - 1 from lt2 where t2a < 20)
where t1c > 25 order by t1a;

select * from
    (select t1a, t1b from lt1 where t1a = 31 union
        select t2b, t2a from lt2 union
        select t3c, t3b from lt3 where t3c = 13)
    where t1b in (12, 21, 32)
    order by t1a;
