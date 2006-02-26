-- $Id$
-- Tests for DateBBInternal UDF
set schema 'udftest';
set path 'udftest';


values applib.date_internal(9223372036854775807);
values applib.date_internal(0);
values applib.date_internal(-63072000000);

-- these should fail
values applib.date_internal(9223372036854775808);
values applib.date_internal(4.55);
values applib.date_internal(9223372036854775807.01);
values applib.date_internal(9223372036854775807.99);

-- create view with reference to applib.date_internal
create view fromint as
select fname, applib.date_internal(age * 8640000) 
from customers;

select * from fromint
order by 1;

create view todate as
select applib.date_internal(coltiny), applib.date_internal(colsmall), applib.date_internal(colint), applib.date_internal(colbig) 
from inttable;

select * from todate
order by 1;

-- in expressions
values 'Here''s the date:' || applib.date_internal(2207520000000);


-- cleanup
drop view fromint;
drop view todate;