-- $Id$
-- Full vertical system testing of non advanced select statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select empno*2 from sales.emps where empno/2>53-3 order by 1;
select empno*2 from sales.emps where empno+1>111 order by 1;
select empno+99900 as res from sales.emps where empno=100;
select age+empno from sales.emps where deptno*age>age-deptno order by 1;

select age+1 from sales.emps where age between 40 and 50;
select age+1 from sales.emps where age between 50 and 40;
select age+1 from sales.emps where age between symmetRic 50 and 40;
select age+1 from sales.emps where age not between 40 and 50 order by 1;
