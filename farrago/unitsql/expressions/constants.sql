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

select deptno from sales.depts where true order by deptno;

select deptno from sales.depts where 1 > 0 order by deptno;

-- now test with Fennel calc
alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';

select deptno from sales.depts where false;

select deptno from sales.depts where 0 > 1;

select deptno from sales.depts where true order by deptno;

select deptno from sales.depts where 1 > 0 order by deptno;

-- test a bug (LER-3372) with the combination of constant reduction and
-- LucidDB error recovery:  we don't want errors encountered when
-- evaluating the reentrant statement used by constant reduction
-- to be treated as recoverable
alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
alter session set "errorMax" = 25;
!set shownestederrs true

select cast('xyz' as int)+2 from (values(0));

!set shownestederrs false
