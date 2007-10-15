-- $Id$

----------------------------------------
-- Sql level test for Residual Filtering
----------------------------------------

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create schema res;

set schema 'res';

create table t1 (a int, b double, c int);

create index t1_b on t1(b);

insert into t1 values (1, 2, 3);
insert into t1 values (2, 3, 4);
insert into t1 values (3, 4, 5);
insert into t1 values (4, 5, 6);
insert into t1 values (5, 6, 7);
insert into t1 values (6, 7, 8);
insert into t1 values (7, 8, 9);
insert into t1 values (8, 9, 10);
insert into t1 values (9, 10, 11);
insert into t1 values (10, 11, 12);
insert into t1 values (11, 12, 13);
insert into t1 values (12, 13, 14);
insert into t1 values (13, 14, 15);
insert into t1 values (14, 15, 16);
insert into t1 values (15, 16, 17);
insert into t1 values (16, 17, 18);
insert into t1 values (17, 18, 19);
insert into t1 values (18, 19, 20);
insert into t1 values (19, 20, 21);
insert into t1 values (20, 21, 22);
insert into t1 values (21, 22, 23);
insert into t1 values (22, 23, 24);
insert into t1 values (23, 24, 25);
insert into t1 values (24, 25, 26);
insert into t1 values (25, 26, 27);
insert into t1 values (26, 27, 28);
insert into t1 values (27, 28, 29);
insert into t1 values (28, 29, 30);
insert into t1 values (29, 30, 31);
insert into t1 values (30, 31, 32);
insert into t1 values (31, 32, 33);

!set outputformat csv

-- full scan 

explain plan for select a from t1 where c <= 12 order by 1;

explain plan for select a from t1 where c <= 12 and c > 7 or 
c in (3,4,5,6,7) order by 1;

explain plan for select a from t1 where (c <= 12 and c > 7 or 
c in (3,4,5,6,7)) and a > 5 order by 1;

explain plan for select a from t1 where a > 5 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6 or a in (7,8,9,10,11)) order by 1;


-- index scan

explain plan for select a from t1 where b >= 11 order by 1;

explain plan for select a from t1 where b >= 7 and c <= 12 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (7,8,9,10,11)) and b >= 7 order by 1;

explain plan for select a from t1 where 
(c <= 10 and c > 5 or c in (7,8,9,10,11)) and a > 5 and b <= 7 
order by 1;

explain plan for select a from t1 where a > 5 and b < 11 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6 or a in (7,8,9,10,11)) and 
(b in (6,7,8,9,10,11)) order by 1;


-- full scan and non-sargable predicates

explain plan for select a from t1 where 
c <= 12 and b + 1 >= 2 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (3,4,5,6,7)) 
and c + 1 >= 2 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (3,4,5,6,7) and c*2 < 44) 
and a > 5 order by 1;

explain plan for select a from t1 where a > 5 and c/2 > 2 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 and c - 1 < 102 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6  and a+1 > 0 or a in (7,8,9,10,11)) 
order by 1;


-- index scan and non-sargable predicates

explain plan for select a from t1 where b >= 11 and c+1 >= 11 order by 1;

explain plan for select a from t1 where b >= 7 and c <= 12 and 
a*2 >= 2 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 and c+1 in (4,5,6,7,8) or c in (3,4,5,6,7)) 
and b <= 7 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (3,4,5,6,7)) 
and a > 5 and b <= 7 and b+1 <= 8 order by 1;

explain plan for select a from t1 where a > 5 and b < 11 or 
c + 1 = 8 order by 1;

explain plan for select a from t1 where 
(c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6 or a in (7,8,9,10,11)) and 
(b in (6,7,8,9,10,11)) or c > 2 order by 1;

!set outputformat table

-- full scan 

select a from t1 where c <= 12 order by 1;

select a from t1 where c <= 12 and c > 7 or 
c in (3,4,5,6,7) order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) 
and a > 5 order by 1;

select a from t1 where a > 5 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) 
and (a > 5 and a <= 6 or a in (7,8,9,10,11)) order by 1;


-- index scan

select a from t1 where b >= 11 order by 1;

select a from t1 where b >= 7 and c <= 12 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (7,8,9,10,11)) 
and b >= 7 order by 1;

select a from t1 where (c <= 10 and c > 5 or c in (7,8,9,10,11)) and 
a > 5 and b <= 7 order by 1;

select a from t1 where a > 5 and b < 11 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6 or a in (7,8,9,10,11)) and 
(b in (6,7,8,9,10,11)) order by 1;


-- full scan and non-sargable predicates

select a from t1 where c <= 12 and b + 1 >= 2 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) 
and c + 1 >= 2 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7) 
and c*2 < 44) and a > 5 order by 1;

select a from t1 where a > 5 and c/2 > 2 order by 1;

select a from t1 where (c <= 12 and c > 7 and c - 1 < 102 
or c in (3,4,5,6,7)) and (a > 5 and a <= 6  and a+1 > 0 
or a in (7,8,9,10,11)) order by 1;


-- index scan and non-sargable predicates

select a from t1 where b >= 11 and c+1 >= 11 order by 1;

select a from t1 where b >= 7 and c <= 12 and a*2 >= 2 order by 1;

select a from t1 where (c <= 12 and c > 7 and c+1 in (4,5,6,7,8) 
or c in (3,4,5,6,7)) and b <= 7 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
a > 5 and b <= 7 and b+1 <= 8 order by 1;

select a from t1 where a > 5 and b < 11 or c + 1 = 8 order by 1;

select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6 or a in (7,8,9,10,11)) and (b in (6,7,8,9,10,11)) 
or c > 2 order by 1;

--
-- rerun the above query to test close
--
select a from t1 where (c <= 12 and c > 7 or c in (3,4,5,6,7)) and 
(a > 5 and a <= 6 or a in (7,8,9,10,11)) and (b in (6,7,8,9,10,11)) 
or c > 2 order by 1;

!set outputformat csv

--
-- Test VARCHAR with NULL
--

create table lcsemps(city varchar(20));

-- verify creation of system-defined clustered index
!indexes LCSEMPS

-- Plans with NULL in the populating stream
insert into lcsemps select city from sales.emps;
insert into lcsemps values(NULL);
insert into lcsemps values(NULL);
insert into lcsemps values('');
insert into lcsemps values('Pescadero');
-- insert duplicate values to force a compressed batch
insert into lcsemps select city from sales.emps;
insert into lcsemps select city from sales.emps;

explain plan for select * from lcsemps where city = 'Pescadero' 
or city = '' order by 1;

explain plan for select * from lcsemps where city = 'Pescadero' 
or city is null order by 1;

select * from lcsemps where city = 'Pescadero' or 
city = '' order by 1;

select * from lcsemps where city = 'Pescadero' or 
city is null order by 1;

-- Run analyze stats and make sure filters with lower selectivity are evaluated
-- first
analyze table t1 compute statistics for all columns;

explain plan for select * from t1 where a > 10 and c = 20;
explain plan for select * from t1 where c = 20 and a > 10;

explain plan for select * from t1 where (a > 1 and a < 30) and c in (5, 10);
explain plan for select * from t1 where (c > 1 and c < 30) and a in (5, 10);
