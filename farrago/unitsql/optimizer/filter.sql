-- $Id$
-- Test queries which execute row-by-row filters

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- filter which returns one row
select name from emps where empno = 120;

-- filter which returns two rows
select name from emps where empno = 110 order by name;

-- IN filter implemented as OR
select name from emps where empno in (110, 120) order by name;

select name, empno, deptno from emps
where (empno, deptno) in ((110, 10), (120, 20)) order by name;

select name, empno, deptno from emps 
where (empno - 10, deptno + 10) in ((100, 20), (110, 30))
order by name;

-- IN filter implemented as join; have to go over
-- the default threshold of 20 list items for this to kick in;
-- throw in some duplicates just for fun
select name from emps where empno in 
(110, 110, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)
order by name;

-- verify plans
!set outputformat csv

explain plan for
select name from emps where empno = 120;

explain plan without implementation for
select name from emps where empno in (110, 120);

explain plan for
select name from emps where empno in (110, 120);

explain plan without implementation for
select name, empno, deptno from emps where (empno, deptno) in ((110, 10), (120, 20));

explain plan for
select name, empno, deptno from emps where (empno, deptno) in ((110, 10), (120, 20));

explain plan without implementation for
select name, empno, deptno from emps where (empno - 10, deptno + 10) in ((100, 20), (110, 30));

explain plan for
select name, empno, deptno from emps where (empno - 10, deptno + 10) in ((100, 20), (110, 30));

explain plan without implementation for
select name from emps where empno
in (110, 110, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

explain plan for
select name from emps where empno
in (110, 110, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- verify fix for LER-2691 (incorrect IN list padding)
-- requires Farrago personality because LucidDB aggregates 
-- raggedy CHAR to VARCHAR
create table test1(col varchar(2) not null primary key);
insert into test1 values 'B', 'A1';

explain plan for 
select col from test1
where col in (
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'B', 'A1', 'A1', 'A1');

select col from test1
where col in (
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'B', 'A1', 'A1', 'A1')
order by col;

create table test2(col char(2) not null primary key);
insert into test2 values 'B', 'A1';

explain plan for 
select col from test2
where col in (
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'B', 'A1', 'A1', 'A1');

select col from test2
where col in (
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'A1', 'A1', 'A1', 'A1',
'A1', 'B', 'A1', 'A1', 'A1')
order by col;

drop table test1;
drop table test2;

-- not in and null value
-- not in requires outer join
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

explain plan without implementation for 
select name from emps 
where empno not in (10, null);

explain plan for 
select name from emps 
where empno not in (10, null);

select name from emps 
where empno not in (10, null)
order by name;

explain plan without implementation for 
select name from emps
where empno not in (10+10, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10, null);

explain plan for 
select name from emps
where empno not in (10+10, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10, null);

select name from emps
where empno not in (10+10, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10*2, 10, null)
order by name;

explain plan without implementation for 
select name from emps
where empno not in (null, null, null, null, null, null, null, null,null, null, null, null,null, null, null, null,null, null, null, null);

explain plan for 
select name from emps
where empno not in (null, null, null, null, null, null, null, null,null, null, null, null,null, null, null, null,null, null, null, null);

select name from emps
where empno not in (null, null, null, null, null, null, null, null,null, null, null, null,null, null, null, null,null, null, null, null)
order by name;

explain plan without implementation for 
select name from emps
where empno not in (20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20);

explain plan for 
select name from emps
where empno not in (20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20);

select name from emps
where empno not in (20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20)
order by name;

-- Exercise cases where IS [NOT] NULL filters can be removed.
!set outputformat csv
explain plan for select * from emps where empno is null;
explain plan for select * from emps where empno is not null;
explain plan for select * from emps where not(empno is null);
explain plan for select * from emps where not(empno is not null);
explain plan for select * from emps where manager is unknown;
explain plan for select * from emps where not(manager is unknown);
explain plan for select min(empno) from emps
    having not(count(distinct city) is null);

-- Make sure the filter is NOT removed when it's applied on a nullable column
explain plan for select * from emps where city is null;
explain plan for select * from emps where city is not null;
explain plan for select * from emps where not(city is null);
explain plan for select * from emps where not(city is not null);
explain plan for select * from emps where slacker is unknown;
explain plan for select * from emps where not(slacker is unknown);
explain plan for
    select * from
        (select e.empno, d.name from emps e left outer join depts d
            on e.deptno = d.deptno)
    where name is null;
!set outputformat table
select * from
    (select e.empno, d.name from emps e left outer join depts d
        on e.deptno = d.deptno)
where name is null;
