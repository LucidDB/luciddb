-- $ ID: //open/lu/dev/luciddb/test/sql/udr/udf/rightn.sql#2 $
-- Tests for rightN UDF
set schema 'udftest';
set path 'udftest';


values applib.rightN('The test string - chop off this portion', 15);
values applib.rightN('and this?', 0);
values applib.rightN('', 3);

-- create view with reference to applib.rightN
create view cutcust as
select applib.rightN(fname, 5), applib.rightN(lname, 3), applib.rightN(phone, 7)
from customers
where sex = 'M';

select * from cutcust
order by 1;

select fname, applib.rightN(sex, 1)
from customers
order by 1;

-- in expressions
select fname, applib.rightN(phone, 7) || applib.rightN(fname, 3) 
from customers
order by 1;

-- nested
values applib.rightN('Here is the original string.', 20);
values applib.rightN(applib.rightN('Here is the original string.', 20), 5);

-- cleanup
drop view cutcust;
