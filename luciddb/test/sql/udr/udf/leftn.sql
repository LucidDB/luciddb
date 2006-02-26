-- $ ID: //open/lu/dev/luciddb/test/sql/udr/udf/leftn.sql#2 $
-- Tests for leftN UDF
set schema 'udftest';
set path 'udftest';


values applib.leftN('The test string - chop off this portion', 15);
values applib.leftN('and this?', 0);
values applib.leftN('', 3);

-- failures
values applib.leftN('Here''s another one', -3);

-- create view with reference to applib.leftN
create view cutcust as 
select applib.leftN(fname, 5), applib.leftN(lname, 3), applib.leftN(phone, 7)
from customers
where sex = 'M';

select * from cutcust
order by 1;

select fname, applib.leftN(sex, 20)
from customers
order by 1;

-- in expressions
select fname, applib.leftN(phone, 7) || applib.leftN(lname, 3) 
from customers
order by 1;

-- nested
values applib.leftN('Here is the original string.', 20);
values applib.leftN(applib.leftN('Here is the original string.', 20), 5);

-- cleanup
-- should fail, cutcust dependent on it 
drop routine applib.leftN;

-- succeeds
drop view cutcust;
