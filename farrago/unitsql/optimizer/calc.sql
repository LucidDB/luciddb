-- $Id$
-- Test queries which instantiate the calculator (filter and/or project)

set schema sales;

-- enable fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- filter which returns one row
explain plan for
select name from emps where lower(name) = 'wilma';

-- project
explain plan for
select lower(name), empid + 1, empid / 1, empid - 1, empid from sales.emps;

-- revert to system default calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

