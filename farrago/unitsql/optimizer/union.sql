-- $Id$
-- Test UNION queries

set schema sales;

-- test UNION without duplicates
select * from 
(select name from emps union select name from depts)
 order by 1;

-- test UNION ALL without duplicates
select * from 
(select name from emps union all select name from depts)
 order by 1;

-- test UNION with duplicates
select * from 
(select name from depts union select name from depts)
 order by 1;

-- test UNION ALL with duplicates
select * from 
(select name from depts union all select name from depts)
 order by 1;

-- verify plans
!set outputformat csv

explain plan for
select * from 
(select name from emps union select name from depts)
 order by 1;

explain plan for
select * from 
(select name from emps union select name from depts);

explain plan for
select * from 
(select name from emps union all select name from depts)
 order by 1;

explain plan for
select * from 
(select name from emps union all select name from depts);
