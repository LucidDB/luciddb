-- $Id$
-- Full vertical system testing of non advanced select statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select empno from sales.emps order by empno asc;
select empno+1 from sales.emps order by 1;
select empno-1 from sales.emps order by 1;
select empno*2 from sales.emps order by 1;
select empno/2 from sales.emps order by 1;

select empno from sales.emps where empno=110;
select empno from sales.emps where empno>=110 order by 1;
select empno from sales.emps where empno>110;
select empno from sales.emps where empno<=110 order by 1;
select empno from sales.emps where empno<110;
select empno from sales.emps where empno<>110 order by 1;

select empno from sales.emps where empno=99999;
select empno from sales.emps where empno>=99999;
select empno from sales.emps where empno<1;
select empno from sales.emps where empno<=1;
select empno from sales.emps where empno>99999;
select empno from sales.emps where empno<>99999 order by 1;

select empno+1 from sales.emps where empno=110;
select empno+1 from sales.emps where empno>=110 order by 1;
select empno+1 from sales.emps where empno>110;
select empno+1 from sales.emps where empno<110;

select empno+1, empno/2 from sales.emps;

--select empno, empno from sales.emps;

select empno*2 from sales.emps where empno+1>111 order by 1;

select empno+99900 as res from sales.emps where empno=100;
-- select 1 from values(2);
