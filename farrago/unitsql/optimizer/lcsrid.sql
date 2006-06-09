-- $Id$
-- Test internal rid function

create schema rid;
set schema 'rid';

create table tencols(c0 int, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int,
            c7 int, c8 int, c9 int)
    server sys_column_store_data_server
    create clustered index i_c6_c7_c8_c9 on tencols(c6, c7, c8, c9)
    create clustered index i_c3_c4_c5 on tencols(c3, c4, c5)
    create clustered index i_c1_c2 on tencols(c1, c2)
    create clustered index i_c0 on tencols(c0);
create index itencols on tencols(c9);
insert into tencols values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
insert into tencols values(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
insert into tencols values(20, 21, 22, 23, 24, 25, 26, 27, 28, 29);
insert into tencols values(30, 31, 32, 33, 34, 35, 36, 37, 38, 39);
insert into tencols values(40, 41, 42, 43, 44, 45, 46, 47, 48, 49);
analyze table tencols compute statistics for all columns;

-- lcs_rid not available outside of LucidDb personality
select lcs_rid(c0) from tencols;

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- basic selects
select * from tencols order by c0;
select count(*) from tencols;
select lcs_rid(c0) from tencols order by 1;
select *, lcs_rid(c5) from tencols order by 10;
select c3, c2, lcs_rid(c8), c9, c0 from tencols order by 3;
select lcs_rid(c0 + c1) from tencols order by 1;
select 2 * lcs_rid(c0) from tencols;
select abs(c0 - c9), lcs_rid(c1) from tencols order by 2;

-- select rid multiple times
select lcs_rid(c0), lcs_rid(c1), lcs_rid(c2) from tencols order by 1;

-- select with where clause
select c9, c5, lcs_rid(c1), c2 from tencols where c0 = 0;
select *, lcs_rid(c0) from tencols where c1 = 11;

-- filter on rid
select c0 from tencols where lcs_rid(c1) > 2 order by c0;
select c0, c9, lcs_rid(c3) from tencols where c9 > 9 and lcs_rid(c1) = 3;
select c0, c6, lcs_rid(c7) from tencols where c6 = 26 and lcs_rid(c8) = 2;
select * from tencols where lcs_rid(c0) = 0;
select count(*) from tencols where lcs_rid(c0) < 3;
-- these return no rows
select c0, c6, lcs_rid(c7) from tencols where c6 = 26 and lcs_rid(c8) = 3;
select c0, c6, lcs_rid(c7) from tencols where c9 = 49 and lcs_rid(c8) = 3;

-- join
select t2.c8, t1.c5, lcs_rid(t1.c0), t1.c2, lcs_rid(t2.c4)
    from tencols t1, tencols t2 where t1.c0 = t2.c0
    order by 3;
select *, lcs_rid(t1.c0), lcs_rid(t2.c0)
    from tencols t1, tencols t2 where t1.c0 = t2.c0
    order by 1;
select t2.c8, t1.c5, lcs_rid(t1.c0), t1.c2, lcs_rid(t2.c4)
    from tencols t1, tencols t2 where lcs_rid(t1.c0) = lcs_rid(t2.c0)
    order by 3;

--------------
-- Error cases
--------------
-- no argument
select lcs_rid() from tencols;
-- non-column argument
select lcs_rid(0) from tencols;
-- non-existent column
select lcs_rid(c10) from tencols;
-- reference > 1 table
select lcs_rid(t1.c0 + t2.c1) from tencols t1, tencols t2;
-- non-column argument in where clause
select c0 from tencols where lcs_rid(0) = 0;

-----------------
-- explain output
-----------------
!set outputformat csv
explain plan for select count(*) from tencols;
explain plan for select lcs_rid(c0) from tencols order by 1;
explain plan for select *, lcs_rid(c5) from tencols order by 10;
explain plan for select c3, c2, lcs_rid(c8), c9, c0 from tencols order by 3;
explain plan for select lcs_rid(c0 + c1) from tencols order by 1;
explain plan for select 2 * lcs_rid(c0) from tencols;
explain plan for select abs(c0 - c9), lcs_rid(c1) from tencols order by 2;
explain plan for select lcs_rid(c0), lcs_rid(c1), lcs_rid(c2) from tencols
    order by 1;
explain plan for select c9, c5, lcs_rid(c1), c2 from tencols where c0 = 0;
explain plan for select *, lcs_rid(c0) from tencols where c1 = 11;
explain plan for select c0 from tencols where lcs_rid(c1) > 2 order by c0;
explain plan for select c0, c9, lcs_rid(c3) from tencols
    where c9 > 9 and lcs_rid(c1) = 3;
explain plan for select c0, c6, lcs_rid(c7) from tencols
    where c6 = 26 and lcs_rid(c8) = 2;
explain plan for select * from tencols where lcs_rid(c0) = 0;
explain plan for select count(*) from tencols where lcs_rid(c0) < 3;
explain plan for select t2.c8, t1.c5, lcs_rid(t1.c0), t1.c2, lcs_rid(t2.c4)
    from tencols t1, tencols t2 where t1.c0 = t2.c0
    order by 3;
explain plan for select t2.c8, t1.c5, lcs_rid(t1.c0), t1.c2, lcs_rid(t2.c4)
    from tencols t1, tencols t2 where lcs_rid(t1.c0) = lcs_rid(t2.c0)
    order by 3;
