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
select pow(pow(2.0+1.0,pow(2.0,2.0)-1.0)+3.0,2.0) from values(1);

select -(1+2) from values(1);

-- multiple line spanning using the neg operator
------------------------------------------------
select - -1,-      -2,
-
-
3
from values(1);
-- This one is failing but shouldnt. Its basically the same query as above but with a comment in the middle
--select - -1,-      -2,
---- this is a comment in the middle of a statement
---
---
--3
--from values(1);
------------------------------------------------

select cast(null as boolean), cast(null as integer) from values(1);

-- fails
select cast(null as boolean) + cast(null as integer) from values(1);
-- fails
select cast(null as boolean) and 1 from values(1);

-- OK - some of these test fail due to cast issues but shouldnt
--select cast(null as tinyint)+1 from values(1);
--select cast(null as smallint)=1 from values(1);
--select cast(null as bigint)<>1 from values(1);
select cast(null as float)>1.0 from values(1);
--select cast(null as float)>1 from values(1);
select cast(null as integer)<=1 from values(1);
--select cast(null as real)>=1 from values(1);
--select cast(null as double)/1 from values(1);
--select cast(null as tinyint)*1 from values(1);
--select cast(null as tinyint)-1 from values(1);
--select cast(null as char)='yo wasup?' from values(1);

select 3*+-2 from values(1);
select cast(1 as varbinary)+x'ff' from values(1);
select x'ff'=x'ff' from values(1);
select x'f'=x'f' from values(1);
--select x'ff'=cast(255 as varbinary) from values(1);
select b'1010'=X'A' from values(1);
-- not equal
--select b'01010'<>b'1010' from values(1);


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
