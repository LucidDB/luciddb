-- $Id$
-- Test aggregate queries

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

--------------------------
-- Test Sort Aggreagtes --
--------------------------
alter system set "codeCacheMaxBytes"=min;
alter session implementation set default;
!set outputformat table

select count(*) from depts;

select count(city) from emps;

select count(city) from emps where empno > 100000;

select sum(deptno) from depts;

select sum(deptno) from depts where deptno > 100000;

select max(deptno) from depts;

select min(deptno) from depts;

select avg(deptno) from depts;

------------
-- group bys
------------

select deptno, count(*) from emps group by deptno;

-- Issue the same statement again to make sure SortedAggStream
-- is in good state when reopened
select deptno, count(*) from emps group by deptno;

select d.name, count(*) from emps e, depts d
    where d.deptno = e.deptno group by d.name;

-- Test group by key where key value could be NULL
select deptno, gender, min(age), max(age) from emps
    group by deptno, gender;

select sum(age) from emps group by deptno;

-- Test where input stream is empty
select deptno, count(*) from emps where deptno < 0 group by deptno;


-- verify plans
!set outputformat csv

explain plan for
select count(*) from depts;

explain plan for
select count(city) from emps;

explain plan for
select sum(deptno) from depts;

explain plan for
select max(deptno) from depts;

explain plan for
select min(deptno) from depts;

explain plan for
select avg(deptno) from depts;

explain plan without implementation for
select deptno,max(name) from sales.emps group by deptno;

-----------------------------
-- verify plans for group bys
-----------------------------

explain plan for 
select deptno, count(*) from emps group by deptno;

explain plan for
select d.name, count(*) from emps e, depts d
    where d.deptno = e.deptno group by d.name;

explain plan for
select deptno, gender, min(age), max(age) from emps
    group by deptno, gender;

explain plan for
select sum(age) from emps group by deptno;

--------------------------
-- Test Hash Aggreagtes --
--------------------------
alter system set "codeCacheMaxBytes"=max;
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
!set outputformat table

------------
-- group bys
------------

select deptno, count(*) from emps group by deptno order by 1;

-- Issue the same statement again to make sure SortedAggStream
-- is in good state when reopened
select deptno, count(*) from emps group by deptno order by 1;

select d.name, count(*) from emps e, depts d
    where d.deptno = e.deptno group by d.name order by 1;

-- Test group by key where key value could be NULL
select deptno, gender, min(age), max(age) from emps
    group by deptno, gender order by 1,2;

select sum(age) from emps group by deptno order by 1;

-- Test where input stream is empty
select deptno, count(*) from emps where deptno < 0 group by deptno
order by 1;

-- LDB-135 - exercise the case where the buffer space for the aggregate 
-- result increases, requiring a new slot to be created for an existing tuple
create table test(num integer, name varchar(20));
insert into test values(0,'B');
insert into test values(1,'D');
insert into test values(0,'AAA');

create table test2(dname varchar(20), num integer);
insert into test2 values('dept1', 0);
insert into test2 values('dept2', 1);

select test2.dname, min(test.name) from test,test2
    where test.num = test2.num group by dname;

drop table test;
drop table test2;

-- verify plans
!set outputformat csv

explain plan without implementation for
select deptno,max(name) from sales.emps group by deptno;

-----------------------------
-- verify plans for group bys
-----------------------------

explain plan for 
select deptno, count(*) from emps group by deptno;

explain plan for
select d.name, count(*) from emps e, depts d
    where d.deptno = e.deptno group by d.name;

explain plan for
select deptno, gender, min(age), max(age) from emps
    group by deptno, gender;

explain plan for
select sum(age) from emps group by deptno;

-- End agg.sql
