-- $Id$
-- Test queries with DISTINCT

set schema sales;

-- simple key
select distinct gender from emps order by 1;


-- TODO:  if the select list is reversed, the next query can't be
-- entirely pushed down to Fennel; need to deal with reordering

-- compound key
select distinct empno,gender from emps order by 1;


-- verify plans
!set outputformat csv

explain plan for
select distinct gender from emps;

explain plan for
select distinct gender from emps order by 1;

explain plan for
select distinct empno,gender from emps;

explain plan for
select distinct empno,gender from emps order by 1;
