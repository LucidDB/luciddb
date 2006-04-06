-- $Id$
-- Test optimizer rules for pushing down filters

create schema pdf;
set schema 'pdf';

create table t1(k1 int primary key, t1a int, t1b int, t1c int, t1d int);
create table t2(k2 int primary key, t2a int, t2b int, t2c int, t2d int);
create table t3(k3 int primary key, t3a int, t3b int, t3c int, t3d int);

insert into t1 values(1, 1, 1, 1, 1);
insert into t1 values(2, 2, 2, 2, 2);
insert into t1 values(3, 3, 3, 3, 3);
insert into t1 values(4, 4, 4, 4, 4);
insert into t1 values(5, 5, 5, 5, 5);
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
        t1a >= 1 and 2 <= t2a and (t2b < 3 or t2c > 4) and t1d = t2d and
        t1b = t1c order by k1;

-- filters in the on clause
explain plan for
    select * from t1 inner join t2 on
        t1a >= 1 and 2 <= t2a and (t2b < 3 or t2c > 4) and t1d = t2d and
        t1b = t1c order by k1;

-- filters in both on clause and where
explain plan for
    select * from t1 inner join t2
        on t1d = t2d and t1b = t1c
        where t1a >= 1 and 2 <= t2a and (t2b < 3 or t2c > 4)
        order by k1;

-- outer joins
-- need to join on key columns as outer joins not supported otherwise

-- can push to the left since it doesn't generate nulls
explain plan for
    select * from t1 left outer join t2
        on k1 = k2
        where t1a = 1;

-- can't push to the right from where clause since right generates nulls
explain plan for
    select * from t1 left outer join t2
        on k1 = k2
        where t2a = 1;

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
        t1a = t2a and t2b = t3b and t1b > 1 and t2c > 2 and t3d = 3;

-- join filter that references all 3 columns and therefore can only be pushed
-- to the topmost join
explain plan for
    select * from t1, t2, t3 where
        t1a = t2b + t3c
        order by k1, k2;

-----------------------------------------------------------------
-- run queries just to make sure the plans created are executable
-----------------------------------------------------------------
!set outputformat table

select * from t1, t2 where
    t1a  = 1 order by k1, k2;

select * from t1, t2 where
    t1a >= 1 and 2 <= t2a and (t2b < 3 or t2c > 4) and t1d = t2d and
    t1b = t1c order by k1;

select * from t1 inner join t2 on
    t1a >= 1 and 2 <= t2a and (t2b < 3 or t2c > 4) and t1d = t2d and
    t1b = t1c order by k1;

select * from t1 inner join t2
    on t1d = t2d and t1b = t1c
    where t1a >= 1 and 2 <= t2a and (t2b < 3 or t2c > 4)
    order by k1;

select * from t1 left outer join t2
    on k1 = k2
    where t1a = 1;

select * from t1 left outer join t2
    on k1 = k2
    where t2a = 1;

select * from t1, t2, t3 where
    t1a = t2a and t2b = t3b
    order by k1;

select * from t1, t2, t3 where
    t1a = t2a and t2b = t3b and t1b > 1 and t2c > 2 and t3d = 3;

select * from t1, t2, t3 where
    t1a = t2b + t3c
    order by k1, k2;
