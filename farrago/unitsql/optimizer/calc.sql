-- $Id$
-- Test queries which instantiate the calculator (filter and/or project)

set schema 'sales';
!set outputformat csv

-- force usage of Fennel calculator for single expression
explain plan for
select lower(name), IN_FENNEL(empid + 1)
from sales.emps;

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

-- filter which returns one row
explain plan for
select name from emps where lower(name) = 'wilma';

-- project
explain plan for
select lower(name), empid + 1, empid / 1, empid - 1, empid from sales.emps;

-- dtbug#32
select 1 + 2 from (values (3));

-- End calc.sql

