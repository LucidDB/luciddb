-- $Id$
-- Test queries against views

!autocommit off

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

set schema sales;

-- select from view on permanent table
select * from empsview order by name;

-- select from view on temporary table
insert into temps select * from emps;
select * from tempsview order by name;
rollback;

-- select from view of join
select * from joinview order by dname,ename;

-- verify plans
!set outputformat csv

explain plan for
select * from empsview order by name;

explain plan for
select * from joinview order by dname,ename;

rollback;
