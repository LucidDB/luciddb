-- $Id$
-- Full vertical system testing of non function statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select pow(2.0,2.0) as exp from sales.emps where empno=100 ;
select -pow(2.0,2.0) as exp from sales.emps where empno=100 ;
select mod(age,9) from sales.emps order by 1;
--values abs(-5000000000);
select abs(-pow(2.0,-2.0)) as res from sales.emps where empno=100 ;
select ln(2.71828) as res from sales.emps where empno=100;
select log(10.0) from sales.emps;
select log(10) from sales.emps;
values log(10*10.0);
select (-empno) as res from sales.emps;
select (-empno)*2 as res from sales.emps;
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
select nullif(name,'Wilma') from sales.emps order by 1;
select nullif(50,age) is null from sales.emps order by 1;
select nullif(age,50) is null from sales.emps order by 1;
select nullif(50,age) from sales.emps order by 1;
select nullif(age,50) from sales.emps order by 1;
select nullif(age,cast(null as integer)) from sales.emps order by 1;

select abs((-empno)*2) as res from sales.emps;
select abs(2) as res from sales.emps;
select abs(-2) as res from sales.emps;
select abs((-empno)*2.0) as res from sales.emps;

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
