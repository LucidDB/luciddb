-- $Id$
-- test constants in operands.

-- first test with Java calc
alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';

select deptno from sales.depts where false;

select deptno from sales.depts where 0 > 1;

select deptno from sales.depts where true;

select deptno from sales.depts where 1 > 0;

-- now test with Fennel calc
alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';

select deptno from sales.depts where false;

select deptno from sales.depts where 0 > 1;

select deptno from sales.depts where true;

select deptno from sales.depts where 1 > 0;

