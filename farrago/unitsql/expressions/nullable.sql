-- $Id$
-- Test expressions on nullable data

-- comparison
select name from sales.emps where age > 30;

-- use outputformat xmlattr so we can see nulls
!set outputformat xmlattr

-- computation
select age+1 from sales.emps order by 1;
