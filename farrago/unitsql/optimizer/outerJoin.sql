-- $Id$

-- Test optimization of outer join queries

create schema oj;
set schema 'oj';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table A(a int not null);
create table B(b int not null);
create table C(c int not null);
create table D(d int not null);
insert into A values (1), (2), (3);
insert into B values (2), (3), (4);
insert into C values (2), (3), (4);
insert into D values (3), (4), (5);

---------------------------------------------------
-- Set 1 -- RHS of topmost join is (C inner join D)
---------------------------------------------------
select * from
    (select * from A inner join B on a = b)
    left outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    right outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    full outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    inner join
    (select * from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A right outer join B on a = b)
    left outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    right outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    full outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    inner join
    (select * from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A left outer join B on a = b)
    left outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    right outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    full outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    inner join
    (select * from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A full outer join B on a = b)
    left outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    right outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    full outer join
    (select * from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    inner join
    (select * from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

---------------------------------------------------------
-- Set 2 -- RHS of topmost join is (C right outer join D)
---------------------------------------------------------
select * from
    (select * from A inner join B on a = b)
    left outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    right outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    full outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    inner join
    (select * from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A right outer join B on a = b)
    left outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    right outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    full outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    inner join
    (select * from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A left outer join B on a = b)
    left outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    right outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    full outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    inner join
    (select * from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A full outer join B on a = b)
    left outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    right outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    full outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    inner join
    (select * from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

-------------------------------------------------------
-- Set 3 - RHS of topmost join is (C left outer join D)
-------------------------------------------------------
select * from
    (select * from A inner join B on a = b)
    left outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    right outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    full outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    inner join
    (select * from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A right outer join B on a = b)
    left outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    right outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    full outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    inner join
    (select * from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A left outer join B on a = b)
    left outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    right outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    full outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    inner join
    (select * from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A full outer join B on a = b)
    left outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    right outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    full outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    inner join
    (select * from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

-------------------------------------------------------
-- Set 4 - RHS of topmost join is (C full outer join D)
-------------------------------------------------------
select * from
    (select * from A inner join B on a = b)
    left outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    right outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    full outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A inner join B on a = b)
    inner join
    (select * from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A right outer join B on a = b)
    left outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    right outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    full outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A right outer join B on a = b)
    inner join
    (select * from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A left outer join B on a = b)
    left outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    right outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    full outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A left outer join B on a = b)
    inner join
    (select * from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select * from
    (select * from A full outer join B on a = b)
    left outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    right outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    full outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select * from
    (select * from A full outer join B on a = b)
    inner join
    (select * from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

-------------------------------------------
-- explain on a subset of the above queries
-------------------------------------------
!set outputformat csv
explain plan for
select * from
    (select * from A inner join B on a = b)
    left outer join
    (select * from C inner join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A right outer join B on a = b)
    right outer join
    (select * from C inner join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A left outer join B on a = b)
    full outer join
    (select * from C inner join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A full outer join B on a = b)
    inner join
    (select * from C inner join D on c = d)
    on a = c;

explain plan for
select * from
    (select * from A inner join B on a = b)
    right outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A right outer join B on a = b)
    full outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A left outer join B on a = b)
    inner join
    (select * from C right outer join D on c = d)
    on a = c;
explain plan for
select * from
    (select * from A full outer join B on a = b)
    left outer join
    (select * from C right outer join D on c = d)
    on a = c and b = d;

explain plan for
select * from
    (select * from A inner join B on a = b)
    full outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A right outer join B on a = b)
    inner join
    (select * from C left outer join D on c = d)
    on a = c;
explain plan for
select * from
    (select * from A left outer join B on a = b)
    left outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A full outer join B on a = b)
    right outer join
    (select * from C left outer join D on c = d)
    on a = c and b = d;

explain plan for
select * from
    (select * from A inner join B on a = b)
    inner join
    (select * from C full outer join D on c = d)
    on a = c;
explain plan for
select * from
    (select * from A right outer join B on a = b)
    left outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A left outer join B on a = b)
    right outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d;
explain plan for
select * from
    (select * from A full outer join B on a = b)
    full outer join
    (select * from C full outer join D on c = d)
    on a = c and b = d;

------------------------------------
-- inner selects contain projections
------------------------------------
!set outputformat table
-- can't pull because RHS generates nulls
select * from
    A left outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb
order by 1, 2, 3;
-- can't pull because join condition references expression
select * from
    A right outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb
order by 1, 2, 3;
-- can pull
select * from
    A right outer join
    (select b as bb, c*1 as cc from B inner join C on b = c)
    on a = bb
order by 1, 2, 3;
-- can pull RHS since only LHS generates nulls
select * from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    right outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc
order by 1, 2, 3, 4;

-- can't pull because LHS generates nulls
select * from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    right outer join C
    on aa = c
order by 1, 2, 3;
-- can't pull because join condition references expression
select * from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c
order by 1, 2, 3;
-- can pull
select * from
    (select a as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c
order by 1, 2, 3;
-- can pull LHS because only RHS generates nulls
select * from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    left outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc
order by 1, 2, 3, 4;

-- can't pull because both LHS and RHS generate nulls
select * from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    full outer join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd
order by 1, 2, 3, 4;
-- can't pull because join condition references expressions
select * from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    inner join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd
order by 1, 2, 3, 4;
-- can pull
select * from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    inner join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc 
order by 1, 2, 3, 4;

------------------------------------
-- explain on the projection queries
------------------------------------
!set outputformat csv
-- can't pull because RHS generates nulls
explain plan for
select * from
    A left outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb;
-- can't pull because join condition references expression
explain plan for
select * from
    A right outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb;
-- can pull
explain plan for
select * from
    A right outer join
    (select b as bb, c*1 as cc from B inner join C on b = c)
    on a = bb;
-- can pull RHS since only LHS generates nulls
explain plan for
select * from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    right outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc;

-- can't pull because LHS generates nulls
explain plan for
select * from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    right outer join C
    on aa = c;
-- can't pull because join condition references expression
explain plan for
select * from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c;
-- can pull
explain plan for
select * from
    (select a as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c;
-- can pull LHS because only RHS generates nulls
explain plan for
select * from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    left outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc;

-- can't pull because both LHS and RHS generate nulls
explain plan for
select * from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    full outer join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd;
-- can't pull because join condition references expressions
explain plan for
select * from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    inner join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd;
-- can pull
explain plan for
select * from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    inner join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc;

-- a simple example of why projects can't be pulled up if they're part of a
-- null-generating input in an outer join; the two selects below should return
-- different results
!set outputformat table
create table t1(a int not null);
insert into t1 values (1), (2);
create table t2(a int not null, b int);
insert into t2 values (1, null);
select * from t1 left outer join
    (select a, coalesce(b, -99) from t2) as t2
    on t1.a = t2.a;
select t1.a, t2.a, coalesce(t2.b, -99)
    from t1 left outer join t2 on t1.a = t2.a;

-- LER-2181
create table ta(a int, k int);
create table tb(b int, k int);
create table tc(c int, k int);
create table td(d int, k int);
insert into ta values (1, 1), (2, 2), (3, 3);
insert into tb values (2, 1), (3, 2), (4, 3);
insert into tc values (2, 1), (3, 2), (4, 3);
insert into td values (3, 1), (4, 2), (5, 3);

select a, c from
    (select a, b from ta inner join tb on a = b)
    left outer join
    (select c, d from tc inner join td on c = d)
    on a = c
order by 1, 2;
