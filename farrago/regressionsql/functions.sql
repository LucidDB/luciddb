-- $Id$
-- Full vertical system testing of non function statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select pow(2.0,2.0) as exp from sales.emps where empno=100 ;
select -pow(2.0,2.0) as exp from sales.emps where empno=100 ;
select abs(-pow(2.0,-2.0)) as res from sales.emps where empno=100 ;
select ln(2.71828) as res from sales.emps where empno=100;
select (-empno) as res from sales.emps;
select (-empno)*2 as res from sales.emps;
select slacker and true from sales.emps;
select slacker and false from sales.emps;
select slacker and unknown from sales.emps;
select slacker or true from sales.emps;
select slacker or false from sales.emps;
select slacker or unknown from sales.emps;
--select *  from sales.emps where age between 40 and 60;
--select abs((-empno)*2) as res from sales.emps;
--select abs(2) as res from sales.emps;
--select abs(-2) as res from sales.emps;
--select abs((-empno)*2.0) as res from sales.emps;
