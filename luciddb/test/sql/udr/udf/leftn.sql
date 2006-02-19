-- $ ID: //open/lu/dev/luciddb/test/sql/udr/udf/leftn.sql#1 $
-- Tests for leftN UDF
set schema 'udftest';
set path 'udftest';

-- define functions
create function leftN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.leftN.FunctionExecute';

values leftN('The test string - chop off this portion', 15);
values leftN('and this?', 0);
values leftN('', 3);

-- failures
values leftN('Here''s another one', -3);

-- create view with reference to leftN
create view cutcust as 
select leftN(fname, 5), leftN(lname, 3), leftN(phone, 7)
from customers
where sex = 'M';

select * from cutcust
order by 1;

select fname, leftN(sex, 20)
from customers
order by 1;

-- in expressions
select fname, leftN(phone, 7) || leftN(lname, 3) 
from customers
order by 1;

-- nested
values leftN('Here is the original string.', 20);
values leftN(leftN('Here is the original string.', 20), 5);

-- cleanup
-- should fail, cutcust dependent on it 
drop routine leftN;

-- succeeds
drop view cutcust;
drop routine leftN;