-- $Id$
-- Test queries which execute row-by-row filters

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- test an ORDER BY for which a sort is required
select city from emps order by 1;

-- verify plans
!set outputformat csv

explain plan for
select city from emps order by 1;
