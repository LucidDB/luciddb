-- $Id$
-- Test expressions on nullable data

-- comparison
select name from sales.emps where age > 30;

-- use outputformat xmlattr so we can see nulls
!set outputformat xmlattr

-- computation
select age+1 from sales.emps order by 1;

-- test boolean expressions with one nullable, one nonnullable
select slacker, manager, 
       slacker and manager, not slacker and manager from sales.emps;

select slacker, manager, 
       slacker or manager, not slacker or manager from sales.emps;

-- test boolean expressions with both nullable
select slacker, age < 60,
       slacker and (age < 60), not slacker and (age < 60) from sales.emps;

select slacker, age < 60,
       slacker or (age < 60), not slacker or (age < 60) from sales.emps;