-- $Id$
-- Full vertical system testing of non function statements

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

values 'w'='w';
values 'w'>'w';
values 'W'>'w';
values 'w'>'W';
values 'w'>='w';
values 'a'<'b';
values 'a'<>'b';
values 'a'<='b';

select substring(name from 1 for 2) from sales.emps order by 1;
select substring(name from 2) from sales.emps order by 1;
select substring(name from 1) from sales.emps order by 1;
select city, age, substring(city, 2, age/10) from sales.emps order by 1;
select city, age, substring(city,  age/20, empid) from sales.emps order by 1;
select city, substring(city, 3) from sales.emps order by 1;
select city, substring(city, -1, 4) from sales.emps order by 1;
select city, substring(city, 0, 4) from sales.emps order by 1;
select name, empid, substring(name, 2, empid) from sales.emps order by 1;

select lower(city) from sales.emps order by 1;
select upper(city) from sales.emps order by 1;

-- NOTE jvs 10-Oct-2005:  I added these trims to avoid dtbug 405 in the
-- Fennel calc codegen which causes varchar to get pre-cast to char
-- when overlay operand types are mixed.  Get rid of them once this bug
-- is fixed.

select overlay(name placing trim('??') from 1) from sales.emps order by 1;
select overlay(name placing trim('??') from 2 for 1) from sales.emps order by 1;
select overlay(name placing trim('??') from 2 for 2) from sales.emps order by 1;
select overlay(city placing trim('??') from 2 for 3) from sales.emps order by 1;
select overlay(name placing trim('??') from 2 for 4) from sales.emps order by 1;
values overlay('ABCdef' placing '12' from 2 for 1);
values overlay('ABCdef' placing '12' from 2 for 2);
values overlay('ABCdef' placing '12' from 2 for 3);
values overlay('ABCdef' placing '12' from 2 for 9);
values overlay('ABCdef' placing '12' from 999 for 1);

select position('wi' in name) from sales.emps order by 1;
select position('Fran' in city) from sales.emps order by 1;

select name like '%ma' from sales.emps order by 1;
select name like 'ma' from sales.emps;
select name like '_ma' from sales.emps;
select name like '___ma' from sales.emps order by 1;
select name like '' from sales.emps;
select city like 'san%' from sales.emps order by 1;
select city like 'San%' from sales.emps order by 1;
select name, name similar to 'Fr(ed|ank)+' from sales.emps order by 1;
select city, city similar to '(%an)+cr*isco' from sales.emps order by 1;
values '_a%' like 'a_aaa%' escape 'a';
values '_a%' similar to 'a_aaa%' escape 'a';


select char_length(city) from sales.emps order by 1;

values 'a'||'b';
values 'a '||'b';
values 'a'||'b'||'c';
values 'a'||cast(null as varchar(1));
values cast(null as varchar(1))||'b';
select name||' is from city '||city from sales.emps order by 1;
select name||' is from city '||city||'.' from sales.emps order by 1;
