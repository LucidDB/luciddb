-- $ ID: //open/lu/dev/luciddb/test/sql/udr/udf/rightn.sql#1 $
-- Tests for rightN UDF
set schema 'udftest';
set path 'udftest';

-- define functions
create function rightN(str varchar(128), len integer)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.rightN.FunctionExecute';

values rightN('The test string - chop off this portion', 15);
values rightN('and this?', 0);
values rightN('', 3);

-- create view with reference to rightN
create view cutcust as
select rightN(fname, 5), rightN(lname, 3), rightN(phone, 7)
from customers
where sex = 'M';

select * from cutcust
order by 1;

select fname, rightN(sex, 1)
from customers
order by 1;

-- in expressions
select fname, rightN(phone, 7) || rightN(fname, 3) 
from customers
order by 1;

-- nested
values rightN('Here is the original string.', 20);
values rightN(rightN('Here is the original string.', 20), 5);

-- cleanup
drop view cutcust;
drop routine rightN;