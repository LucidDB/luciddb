-- $Id$
-- Test queries which execute row-by-row filters

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- filter which returns one row
select name from emps where empno = 120;

-- filter which returns two rows
select name from emps where empno = 110;

-- verify plans
!set outputformat csv

explain plan for
select name from emps where empno = 120;
