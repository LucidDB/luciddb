-- $Id$
-- Full vertical system testing of non function statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select 'w'='w' from values(1);
select 'w'>'w' from values(1);
select 'W'>'w' from values(1);
select 'w'>'W' from values(1);
select 'w'>='w' from values(1);
select 'a'<'b' from values(1);
select 'a'<>'b' from values(1);
select 'a'<='b' from values(1);

select substring(name from 1 for 2) from sales.emps order by 1;
select substring(name from 2) from sales.emps order by 1;
select substring(name from 1) from sales.emps order by 1;

select lower(city) from sales.emps order by 1;
select upper(city) from sales.emps order by 1;

select overlay(name placing '??' from 1) from sales.emps order by 1;
select overlay(name placing '??' from 2 for 1) from sales.emps order by 1;
select overlay(name placing '??' from 2 for 2) from sales.emps order by 1;
select overlay(city placing '??' from 2 for 3) from sales.emps order by 1;
select overlay(name placing '??' from 2 for 4) from sales.emps order by 1;

select position('wi' in name) from sales.emps order by 1;
select position('Fran' in city) from sales.emps order by 1;

select name like '%ma' from sales.emps order by 1;
select name like 'ma' from sales.emps;
select name like '_ma' from sales.emps;
select name like '___ma' from sales.emps order by 1;
--select name like '' from sales.emps;
--select city like 'san%' from sales.emps order by 1;
--select city like 'San%' from sales.emps order by 1;

--select char_length(city) from sales.emps order by 1;
