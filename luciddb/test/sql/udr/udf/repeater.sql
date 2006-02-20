-- $Id$
-- Test queries for repeater UDF
set schema 'udftest';
set path 'udftest';


values applib.repeater('lola ', 3);
values applib.repeater('2', 3);
values applib.repeater('lola ', 1000000000);

-- these should fail
values applib.repeater(3, 3);
values applib.repeater('lola ', X'02');

-- create view with reference to applib.repeater
create view repview as
select fname, lname, applib.repeater(sex, 2) 
from customers;

select * from repview 
order by 1;

-- in expressions
values ('is who? ' || applib.repeater('Voter', 2));
values(cast (applib.repeater('25', 2) as integer) / 25 - 1);
values(cast (applib.repeater('11', 3) as integer) + 10000000000);

-- nested
values (applib.repeater( applib.repeater('Voter ', 2) || 'is who? ', 2));

-- cleanup
drop view repview;