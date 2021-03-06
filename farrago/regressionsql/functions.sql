-- $Id$
-- Full vertical system testing of non function statements

!set numberformat #.######

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select sqrt(12.96) as x from sales.emps where empno=100;
select power(2.0,2.0) as exponential from sales.emps where empno=100 ;
select -power(2.0,2.0) as exponential from sales.emps where empno=100 ;
select mod(age,9) from sales.emps order by 1;
--values abs(-5000000000);
select abs(-power(2.0,-2.0)) as res from sales.emps where empno=100 ;
select ln(2.71828) as res from sales.emps where empno=100;
select log10(10.0) from sales.emps;
select log10(10) from sales.emps;
values log10(10*10.0);
select (-empno) as res from sales.emps order by 1;
select (-empno)*2 as res from sales.emps order by 1;
select slacker and true from sales.emps order by 1;
select slacker and false from sales.emps order by 1;
select slacker and unknown from sales.emps order by 1;
select slacker or true from sales.emps order by 1;
select slacker or false from sales.emps order by 1;
select slacker or unknown from sales.emps order by 1;

select coalesce(age,-1) from sales.emps order by 1;
select case slacker when true then 'yes' when false then 'no' end from sales.emps order by 1;
select case slacker when true then 'yes' when false then 'no' else 'null' end from sales.emps order by 1;
values CASE WHEN TRUE THEN 9 ELSE 1 END;

values nullif(5.0, 5.0);
values nullif(4.2, 1);
values nullif(34e1, 3.4e1);
select nullif(name,'Wilma') from sales.emps order by 1;
select nullif(50,age) is null from sales.emps order by 1;
select nullif(age,50) is null from sales.emps order by 1;
select nullif(50,age) from sales.emps order by 1;
select nullif(age,50) from sales.emps order by 1;
select nullif(age,cast(null as integer)) from sales.emps order by 1;

select abs((-empno)*2) as res from sales.emps order by 1;
select abs(2) as res from sales.emps;
select abs(-2) as res from sales.emps;
select abs((-empno)*2.0) as res from sales.emps order by 1;

values 1+1.0;
values 1.0-1;
values 10<=10.0;
values 10<10.001;
values 10>9.999;
values 10.0>=10;
values 5000000000<>1e1;
values 5000000000<-1.3;
values 1e-2=1;
values 1.0=0.1;
values 1=1.0;
values 1.0=1;


values trim(' ' from '  abc');
values trim('a' from 'Aa');
values trim(leading 'a' from 'Aa');
values trim('a' from 'aAa');
values trim(trailing 'a' from 'Aa');
-- End functions.sql
