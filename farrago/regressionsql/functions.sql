-- $Id$
-- Full vertical system testing of non function statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select pow(2.0,2.0) as exp from sales.emps where empno=100 ;
select -pow(2.0,2.0) as exp from sales.emps where empno=100 ;
select mod(age,9) from sales.emps order by 1;
select abs(-pow(2.0,-2.0)) as res from sales.emps where empno=100 ;
select ln(2.71828) as res from sales.emps where empno=100;
select log(10.0) from sales.emps;
--select log(10) from sales.emps;
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
select nullif(name,'Wilma') from sales.emps order by 1;
select nullif(50,age) is null from sales.emps order by 1;
select nullif(age,50) is null from sales.emps order by 1;
select nullif(50,age) from sales.emps order by 1;
select nullif(age,50) from sales.emps order by 1;

select cast(null as tinyint) from values(1);
select cast(null as smallint) from values(1);
select cast(null as integer) from values(1);
select cast(null as bigint) from values(1);
select cast(null as real) from values(1);
select cast(null as double) from values(1);
--select cast(null as bit) from values(1);
select cast(null as boolean) from values(1);
select cast(null as char) from values(1);
select cast(null as varchar) from values(1);
select cast(null as binary) from values(1);
select cast(null as date) from values(1);
select cast(null as time) from values(1);
select cast(null as timestamp) from values(1);
select cast(null as varbinary) from values(1);
--select cast(null as decimal) from values(1);

--select abs((-empno)*2) as res from sales.emps;
--select abs(2) as res from sales.emps;
--select abs(-2) as res from sales.emps;
--select abs((-empno)*2.0) as res from sales.emps;
