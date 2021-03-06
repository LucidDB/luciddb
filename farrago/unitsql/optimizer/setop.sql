-- $Id$
-- Test queries for set operations (union, intersect, except).

-----------
-- Setup --
-----------
create schema lhx;
set schema 'lhx';
set path 'lhx';

-- force usage of Java calc
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- lucidDB feature
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table emps1(
    ename1 varchar(20))
server sys_column_store_data_server;

create table emps2(
    ename2 varchar(20))
server sys_column_store_data_server;

insert into emps1 values(NULL);
insert into emps1 values('abc');
insert into emps1 values('abc');
insert into emps1 values('def');
insert into emps1 values('def');

insert into emps2 values(NULL);
insert into emps2 values(NULL);
insert into emps2 values('abc');

!set outputformat csv

------------------
-- set op tests --
------------------
explain plan for
select * from emps1 union select * from emps2 order by 1;

explain plan for
select * from emps1 union all select * from emps2 order by 1;

explain plan for
select * from emps1 intersect select * from emps2 order by 1;

explain plan for
select * from emps1 intersect all select * from emps2 order by 1;

explain plan for
select * from emps1 except select * from emps2 order by 1;

explain plan for
select * from emps1 except all select * from emps2 order by 1;

!set outputformat table
select * from emps1 union select * from emps2 order by 1;
select * from emps1 union all select * from emps2 order by 1;
select * from emps1 intersect select * from emps2 order by 1;
select * from emps1 except select * from emps2 order by 1;
!set outputformat csv

-- set op tree
explain plan for
select * from emps1 intersect select * from emps2 intersect select * from emps1
order by 1;

select * from emps1 intersect select * from emps2 intersect select * from emps1
order by 1;

explain plan for
select * from emps1 intersect select * from emps2 intersect select * from emps2
order by 1;

select * from emps1 intersect select * from emps2 intersect select * from emps2
order by 1;

explain plan for
select * from emps1 except select * from emps2 except select * from emps1
order by 1;

select * from emps1 except select * from emps2 except select * from emps1
order by 1;

explain plan for
select * from emps1 except select * from emps2 except select * from emps2
order by 1;

select * from emps1 except select * from emps2 except select * from emps2
order by 1;

-- some tests for set op precedence rules
explain plan for
select * from emps1 intersect select * from emps1 except select * from emps2
order by 1;

select * from emps1 intersect select * from emps1 except select * from emps2
order by 1;

explain plan for
select * from emps1 except select * from emps2 intersect select * from emps1
order by 1;

select * from emps1 except select * from emps2 intersect select * from emps1
order by 1;

explain plan for
select * from emps1 union select * from emps2 intersect select * from emps1
order by 1;

select * from emps1 union select * from emps2 intersect select * from emps1
order by 1;

explain plan for
select * from emps1 except select * from emps2 union select * from emps1
order by 1;

select * from emps1 except select * from emps2 union select * from emps1
order by 1;

explain plan for
select * from emps1 union select * from emps2 except select * from emps2
order by 1;

select * from emps1 union select * from emps2 except select * from emps2
order by 1;

-- multi-key
explain plan for
select ename1, ename1 from emps1 except select ename2, ename2 from emps2
intersect select ename1, ename1 from emps1 order by 1;

select ename1, ename1 from emps1 except select ename2, ename2 from emps2
intersect select ename1, ename1 from emps1 order by 1;

-------------------------------------------------------
-- http://jirahost.eigenbase.org:8081/browse/FRG-182 --
-------------------------------------------------------
create table s1 (a int, b int);
create table s2 (a int, bb int);

explain plan without implementation for
select * from s1 union all (select * from s2 where a = 1);

-- make sure this does not fail during physical plan generation
explain plan for select * from s1 union all (select * from s2 where a = 1);

-------------------------------------------
-- Tests for combining unions and distincts
-------------------------------------------
create table t1(a int);
create table t2(a int);
create table t3(a int);
create table t4(a int);
insert into t1 values(1), (1);
insert into t2 values(2), (2);
insert into t3 values(3), (3);
insert into t4 values(4), (4);

!set outputformat csv
explain plan for
    select * from t1 union all
    select * from t2 union all
    select * from t3 union all
    select * from t4;
explain plan for
    (select * from t1 union all select * from t2) union all
    (select * from t3 union all select * from t4);
explain plan for
    select * from t1 union all
    (select * from t2 union all select * from t3 union all select * from t4);
explain plan for
    select * from t1 union all
        (select * from t2 union all
            (select * from t3 union all select * from t4));

explain plan for
    select * from t1 union
    select * from t2 union
    select * from t3 union
    select * from t4;
explain plan for
    (select * from t1 union select * from t2) union
    (select * from t3 union select * from t4);
explain plan for
    select * from t1 union
    (select * from t2 union select * from t3 union select * from t4);
explain plan for
    select * from t1 union
        (select * from t2 union
            (select * from t3 union select * from t4));
explain plan for
    ((select * from t1 union select * from t2) union
        (select * from t3 union select * from t4))
    union
    ((select * from t1 union select * from t2) union
        (select * from t3 union select * from t4));

!set outputformat table
select * from t1 union all
    select * from t2 union all
    select * from t3 union all
    select * from t4
order by a;
((select * from t1 union select * from t2) union
    (select * from t3 union select * from t4))
union
    ((select * from t1 union select * from t2) union
        (select * from t3 union select * from t4))
order by a;

--------------
-- Clean up --
--------------
!set outputformat table
alter session implementation set default;
drop schema lhx cascade;

-- setops also work in default Farrago personality

select * from 
(
    select deptno from sales.emps
    intersect 
    select deptno from sales.depts
)
order by deptno;

select * from 
(
    select deptno from sales.emps
    except
    select deptno from sales.depts
)
order by deptno;

select * from 
(
    select deptno from sales.emps
    union 
    select deptno from sales.depts
)
order by deptno;

select * from 
(
    select deptno from sales.emps
    union all
    select deptno from sales.depts
)
order by deptno;

!set outputformat csv

explain plan for
select * from 
(
    select deptno from sales.emps
    intersect 
    select deptno from sales.depts
)
order by deptno;

explain plan for
select * from 
(
    select deptno from sales.emps
    except
    select deptno from sales.depts
)
order by deptno;

explain plan for
select * from 
(
    select deptno from sales.emps
    union 
    select deptno from sales.depts
)
order by deptno;

explain plan for
select * from 
(
    select deptno from sales.emps
    union all
    select deptno from sales.depts
)
order by deptno;

-- End setop.sql

