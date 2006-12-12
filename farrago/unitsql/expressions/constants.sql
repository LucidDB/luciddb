-- $Id$
-- test constants in operands.

-- test a constant reduction bug (LER-3482, rewrite from nullable to 
-- not null used to cause trouble)
select 'xxx' from
sales.emps where manager or empid > coalesce(5000,null);

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

