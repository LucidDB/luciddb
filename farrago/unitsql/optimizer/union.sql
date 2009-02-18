-- $Id$
-- Test UNION queries

set schema 'sales';

-- force usage of Java implementation
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

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

-- make sure the time function returns the same value; note that this test
-- isn't repeated for Fennel calc below because Fennel calc doesn't guarantee
-- that time functions return the same value if invoked multiple times within a
-- statement
select count(*) from
    (select current_timestamp from emps union
        select current_timestamp from depts);

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

-- repeat everything, this time with Fennel implementation
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

!set outputformat table

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

explain plan for
insert into depts(name)
select name from emps union all select name from depts;
