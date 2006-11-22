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

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- LucidDB doesn't support ORDER BY DESC yet
select name from sales.depts order by name desc;
