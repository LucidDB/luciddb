-- $Id$

-- Test optimization of outer join queries

create schema oj;
set schema 'oj';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table A(k int, a int not null);
create table B(k int, b int not null);
create table C(k int, c int not null);
create table D(k int, d int not null);
insert into A values (1, 1), (2, 2), (3, 3);
insert into B values (1, 2), (2, 3), (3, 4);
insert into C values (1, 2), (2, 3), (3, 4);
insert into D values (1, 3), (2, 4), (3, 5);

---------------------------------------------------
-- Set 1 -- RHS of topmost join is (C inner join D)
---------------------------------------------------
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    left outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    right outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    full outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    inner join
    (select c, d from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    left outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    right outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    full outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    inner join
    (select c, d from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    left outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    right outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    full outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    inner join
    (select c, d from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    left outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    right outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    full outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    inner join
    (select c, d from C inner join D on c = d)
    on a = c
order by 1, 2, 3, 4;

---------------------------------------------------------
-- Set 2 -- RHS of topmost join is (C right outer join D)
---------------------------------------------------------
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    left outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    right outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    full outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    inner join
    (select c, d from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    left outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    right outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    full outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    inner join
    (select c, d from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    left outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    right outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    full outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    inner join
    (select c, d from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    left outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    right outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    full outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    inner join
    (select c, d from C right outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

-------------------------------------------------------
-- Set 3 - RHS of topmost join is (C left outer join D)
-------------------------------------------------------
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    left outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    right outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    full outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    inner join
    (select c, d from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    left outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    right outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    full outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    inner join
    (select c, d from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    left outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    right outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    full outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    inner join
    (select c, d from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    left outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    right outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    full outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    inner join
    (select c, d from C left outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

-------------------------------------------------------
-- Set 4 - RHS of topmost join is (C full outer join D)
-------------------------------------------------------
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    left outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    right outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    full outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    inner join
    (select c, d from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    left outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    right outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    full outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    inner join
    (select c, d from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    left outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    right outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    full outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    inner join
    (select c, d from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    left outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    right outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    full outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d
order by 1, 2, 3, 4;
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    inner join
    (select c, d from C full outer join D on c = d)
    on a = c
order by 1, 2, 3, 4;

-------------------------------------------
-- explain on a subset of the above queries
-------------------------------------------
!set outputformat csv
explain plan for
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    left outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    right outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    full outer join
    (select c, d from C inner join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    inner join
    (select c, d from C inner join D on c = d)
    on a = c;

explain plan for
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    right outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    full outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    inner join
    (select c, d from C right outer join D on c = d)
    on a = c;
explain plan for
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    left outer join
    (select c, d from C right outer join D on c = d)
    on a = c and b = d;

explain plan for
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    full outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    inner join
    (select c, d from C left outer join D on c = d)
    on a = c;
explain plan for
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    left outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    right outer join
    (select c, d from C left outer join D on c = d)
    on a = c and b = d;

explain plan for
select a, b, c, d from
    (select a, b from A inner join B on a = b)
    inner join
    (select c, d from C full outer join D on c = d)
    on a = c;
explain plan for
select a, b, c, d from
    (select a, b from A right outer join B on a = b)
    left outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A left outer join B on a = b)
    right outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d;
explain plan for
select a, b, c, d from
    (select a, b from A full outer join B on a = b)
    full outer join
    (select c, d from C full outer join D on c = d)
    on a = c and b = d;

------------------------------------
-- inner selects contain projections
------------------------------------
!set outputformat table
-- can't pull because RHS generates nulls
select a, bb, cc from
    A left outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb
order by 1, 2, 3;
-- can't pull because join condition references expression
select a, bb, cc from
    A right outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb
order by 1, 2, 3;
-- can pull
select a, bb, cc from
    A right outer join
    (select b as bb, c*1 as cc from B inner join C on b = c)
    on a = bb
order by 1, 2, 3;
-- can pull RHS since only LHS generates nulls
select aa, bb, cc, dd from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    right outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc
order by 1, 2, 3, 4;

-- can't pull because LHS generates nulls
select aa, bb, c from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    right outer join C
    on aa = c
order by 1, 2, 3;
-- can't pull because join condition references expression
select aa, bb, c from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c
order by 1, 2, 3;
-- can pull
select aa, bb, c from
    (select a as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c
order by 1, 2, 3;
-- can pull LHS because only RHS generates nulls
select aa, bb, cc, dd from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    left outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc
order by 1, 2, 3, 4;

-- can't pull because both LHS and RHS generate nulls
select aa, bb, cc, dd from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    full outer join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd
order by 1, 2, 3, 4;
-- can't pull because join condition references expressions
select aa, bb, cc, dd from
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
select a, bb, cc from
    A left outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb;
-- can't pull because join condition references expression
explain plan for
select a, bb, cc from
    A right outer join
    (select b*1 as bb, c*1 as cc from B inner join C on b = c)
    on a = bb;
-- can pull
explain plan for
select a, bb, cc from
    A right outer join
    (select b as bb, c*1 as cc from B inner join C on b = c)
    on a = bb;
-- can pull RHS since only LHS generates nulls
explain plan for
select aa, bb, cc, dd from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    right outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc;

-- can't pull because LHS generates nulls
explain plan for
select aa, bb, c from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    right outer join C
    on aa = c;
-- can't pull because join condition references expression
explain plan for
select aa, bb, c from
    (select a*1 as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c;
-- can pull
explain plan for
select aa, bb, c from
    (select a as aa, b*1 as bb from A inner join b on a = b)
    left outer join C
    on aa = c;
-- can pull LHS because only RHS generates nulls
explain plan for
select aa, bb, cc, dd from
    (select a as aa, b*1 as bb from A inner join B on a = b)
    left outer join
    (select c as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc;

-- can't pull because both LHS and RHS generate nulls
explain plan for
select aa, bb, cc, dd from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    full outer join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd;
-- can't pull because join condition references expressions
explain plan for
select aa, bb, cc, dd from
    (select a*1 as aa, b*1 as bb from A inner join B on a = b)
    inner join
    (select c*1 as cc, d*1 as dd from C inner join D on c = d)
    on aa = cc and bb = dd;
-- can pull
explain plan for
select aa, bb, cc, dd from
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

-- testcase to ensure that predicates are appropriately converted before being
-- passed into metadata queries; in this particular case, the left outer join
-- is converted into a right outer join because there's a filter on y; there's
-- also a filter on x, but because x is null generating, that filter is not
-- pushed down; as a result, the outer join becomes a right outer join and the
-- columns returned from that outer join are returned in reverse order;
-- therefore the filter on x needs to be adjusted accordingly before its
-- passed into metadata queries on the new outer join node; otherwise, the
-- metadata routines will try to manipulate a filter that compares an integer
-- against a varchar column
create table x(x0 int);
create table y(y0 varchar(10), y1 int);
create table z(z0 varchar(10), z1 int);
!set outputformat csv
explain plan for
select distinct x0, y0, z0
    from
        y left outer join x on y1 = x0,
        z
    where
        x0 = z1 and y1 = 1 and x0 > 0;

-- LER-2472 - cartesian product joins with outer joins -- ROJ needs to be
-- converted to a LOJ for the cartesian product join to be valid
explain plan for
    select a, b from A right outer join B on a = 1;
!set outputformat table
select a, b from A right outer join B on a = 1;

------------------------
-- removable outer joins
------------------------
create table BUniq(k int, b int not null unique);
insert into BUniq values (1, 2), (2, 3), (3, 4);

!set outputformat csv
explain plan for
    select A.* from A left outer join BUniq on a = b;
explain plan for
    select A.* from A left outer join BUniq on b = a;
explain plan for
    select A.* from BUniq right outer join A on a = b;
explain plan for
    select a from (select a, b from A left outer join BUniq on a = b), C
        where a = c;
-- two join keys
explain plan for 
    select A.* from A left outer join BUniq B on a = b and A.k = B.k;

!set outputformat table
select A.* from A left outer join BUniq on a = b order by a;
select A.* from A left outer join BUniq on b = a order by a;
select A.* from BUniq right outer join A on a = b order by a;
select a from (select a, b from A left outer join BUniq on a = b), C
    where a = c order by a;
select A.* from A left outer join BUniq B on a = b and A.k = B.k order by a;

----------------------------
-- non-removable outer joins
----------------------------
!set outputformat csv
-- full outer join
explain plan for
    select A.* from A full outer join BUniq on a = b;
-- join keys not unique
explain plan for
    select A.* from A left outer join B on a = b;
explain plan for
    select A.* from A left outer join B on a = b and A.k = B.k;
-- null generating factor projected in projection list
explain plan for
    select * from A left outer join BUniq on a = b;
explain plan for
    select A.*, BUniq.k from A left outer join BUniq on a = b;
-- null generating factor referenced in a join with another table
explain plan for
    select a from (select a, b from A left outer join BUniq on a = b), C
        where b = c;
explain plan for
    select a from
        (select a, Buniq.k from A left outer join BUniq on a = b) X, C
        where X.k = c;
-- non-equijoin filter
explain plan for
    select A.* from A left outer join BUniq on true;
explain plan for
    select A.* from A left outer join BUniq on a > 1;
-- non-input references
explain plan for
    select A.* from A left outer join B on a = 1;
explain plan for
    select A.* from A left outer join BUniq on a + 1 = b + 1;
-- no keys from null generating factor
explain plan for
    select A.* from A left outer join B on a = A.k;
