-- $Id$
-- Test expressions on nullable data

-- computation
select age+1 from sales.emps order by 1;

-- comparison
select name from sales.emps where age > 30;
