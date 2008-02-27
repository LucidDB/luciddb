-- $Id$
-- Test ORDER BY

-- ORDER BY column name instead of ordinal
select empno,name 
from sales.emps 
order by name;

-- confusingly reverse the aliases; end result should be same as above
select empno as name,name as empno 
from sales.emps 
order by empno;

-- ORDER BY an MDR table
select "name" from sys_cwm."Relational"."Table" order by 1;

-- make sure UNION takes precedence over ORDER BY
select name from sales.depts 
union all 
select name from sales.depts
order by name;

-- disallow internal ORDER BY
select * from (select name from sales.depts order by name);

-- ORDER BY on explicit TABLE
table sales.depts order by name;

-- ORDER BY DESC
select name from sales.depts order by name desc;

-- ORDER BY DESC, ASC
select deptno, name from sales.emps order by deptno desc, name asc;

-- ORDER BY DESC, DESC
select deptno, name from sales.emps order by deptno desc, name desc;

-- ORDER BY ASC, DESC
select deptno, name from sales.emps order by deptno asc, name desc;

-- ORDER BY a very long value produced in Java to see if it correctly
-- results in an error when we try to squeeze it into a too-small
-- Fennel page (FRG-305 was that we would instead silently produce EOS)
create schema udf;
create function udf.repeat(v varchar(128), i int)
returns varchar(65535)
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.repeat';

select udf.repeat('Z', 6000) as r
from (values (0))
order by r;
