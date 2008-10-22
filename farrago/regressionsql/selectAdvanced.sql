-- $Id$
-- Full vertical system testing of advanced select statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select empno*2 from sales.emps where empno/2>53-3 order by 1;
select empno*2 from sales.emps where empno+1>111 order by 1;
select empno+99900 as res from sales.emps where empno=100;
select age+empno from sales.emps where deptno*age>age-deptno order by 1;

select age+1 from sales.emps where age between 40 and 50;
select age+1 from sales.emps where age between 50 and 40;
select age+1 from sales.emps where age between symmetRic 50 and 40;
select age+1 from sales.emps where age not between 40 and 50 order by 1;
select name,name between 'WILMA' AND 'wilma' from sales.emps order by 1;


--These tests are failing but shouldnt
--select age from sales.emps having age>30;
--select * from sales.emps where deptno in (10, 20);

select name from sales.emps order by 1;
(select name from sales.emps) union all (select name from sales.emps) order by 1;

-- function in function
values power(power(2.0+1.0,power(2.0,2.0)-1.0)+3.0,2.0);

values -(1+2);

-- multiple line spanning using the neg operator
------------------------------------------------
select - -1,-      -2,
-
-
3
from (values(1));
-- This one is failing but shouldnt. Its basically the same query as above but with a comment in the middle
--select - -1,-      -2,
---- this is a comment in the middle of a statement
---
---
--3
--from (values(1));
------------------------------------------------

values (cast(null as boolean), cast(null as integer));

-- fails
values cast(null as boolean) + cast(null as integer);
-- fails
values cast(null as boolean) and 1;

-- OK - some of these test fail due to cast issues but shouldnt
--values cast(null as tinyint)+1;
--values cast(null as smallint)=1;
--values cast(null as bigint)<>1;
values cast(null as float)>1.0;
--values cast(null as float)>1;
values cast(null as integer)<=1;
--values cast(null as real)>=1;
--values cast(null as double)/1;
--values cast(null as tinyint)*1;
--values cast(null as tinyint)-1;
--values cast(null as char)='yo wasup?';

values 3*+-2;
values cast(1 as varbinary(1))+x'ff';
values x'ff'=x'ff';
--values x'ff'=cast(255 as varbinary(1));


--select * from sales.emps group by empno order by 1;
--select *from sales.emps group by empno having empno>22 order by 1;

select manager from sales.emps union select manager from sales.emps order by 1;
select manager from sales.emps union all select manager from sales.emps order by 1;

--select*from sales.emps where deptno in (select* from sales.depts);


--join tests
select emps.name,depts.name from sales.emps,sales.depts where depts.deptno=emps.deptno order by 1;
select emps.name,depts.name from sales.emps,sales.depts where depts.deptno=emps.deptno and age=80;
select emps.name,depts.name from sales.emps INNER JOIN sales.depts ON depts.deptno=emps.deptno order by 1;
--select emps.name,depts.name from sales.emps LEFT JOIN sales.depts ON depts.deptno=emps.deptno order by 1;
--select emps.name,depts.name from sales.emps RIGHT JOIN sales.depts ON depts.deptno=emps.deptno order by 1;

--select emps.name,depts.name from sales.emps RIGHT OUTER JOIN sales.depts ON depts.deptno=emps.deptno order by 1;
--select emps.name,depts.name from sales.emps LEFT OUTER JOIN sales.depts ON depts.deptno=emps.deptno order by 1;
--select emps.name,depts.name from sales.emps FULL OUTER JOIN sales.depts ON depts.deptno=emps.deptno order by 1;

values (1) union values (2) order by 1;

select * from sales.depts union all values (40,'Foodfights') order by 1;

-- from dtbug 263:  test with identical table+column name
create schema s;
create table s.b (b boolean, i int primary key);
insert into s.b values (true, 1);
select not b from s.b;
select b or true from s.b;
select b and true from s.b;

-- a few decimal tests
create table s.nulldecimal(c decimal(4,2) primary key, d decimal(6,0));
insert into s.nulldecimal values (19, null);
insert into s.nulldecimal values (20, 20);

-- should fail
insert into s.nulldecimal values (null, 20);

-- test whether nullability flags are read correctly
-- the first column should be detected as not nullable
-- while the second column should be detected as nullable
select d from s.nulldecimal;

-- the type of (d*1.0) is decimal(7,1)
-- casting to decimal(6,1) requires an overflow check
-- this tests whether the overflow check works with null values
select cast((d * 1.0) as decimal(6,1)) from s.nulldecimal;

-- this tests whether a non-nullable value can be inserted into 
-- a nullable column. the values are the exact same type except 
-- nullability to try to fool decimal cast reduction
create table s.nullabledecimal(i int primary key, coldec decimal(2,1));
insert into s.nullabledecimal values (1, 1.2);

drop schema s cascade;

-- restart required for distinct (FNL-19)
select sum(c) 
from sales.depts, 
(select count(distinct deptno) as c from sales.emps);

-- verify plan for previous query
!set outputformat csv

explain plan for
select sum(c) 
from sales.depts, 
(select count(distinct deptno) as c from sales.emps);

!set outputformat table
