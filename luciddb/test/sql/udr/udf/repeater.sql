-- $Id$
-- Test queries for repeater UDF
set schema 'udftest';
set path 'udftest';

-- define function
create function repeater(str varchar(128), times integer)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.repeater.FunctionExecute';


values repeater('lola ', 3);
values repeater('2', 3);
values repeater('lola ', 1000000000);

-- these should fail
values repeater(3, 3);
values repeater('lola ', X'02');

-- create view with reference to repeater
create view repview as
select fname, lname, repeater(sex, 2) 
from customers;

select * from repview 
order by 1;

-- in expressions
values ('is who? ' || repeater('Voter', 2));
values(cast (repeater('25', 2) as integer) / 25 - 1);
values(cast (repeater('11', 3) as integer) + 10000000000);

-- nested
values (repeater( repeater('Voter ', 2) || 'is who? ', 2));