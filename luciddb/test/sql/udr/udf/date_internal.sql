-- $Id$
-- Tests for DateBBInternal UDF
set schema 'udftest';
set path 'udftest';


values applib.internal_date(9223372036854775807);
values applib.internal_date(0);
values applib.internal_date(-63072000000);

-- these should fail
values applib.internal_date(9223372036854775808);
values applib.internal_date(4.55);
values applib.internal_date(9223372036854775807.01);
values applib.internal_date(9223372036854775807.99);

-- create view with reference to applib.internal_date
create view fromint as
select fname, applib.internal_date(age * 8640000) 
from customers;

select * from fromint
order by 1;

create view todate as
select applib.internal_date(coltiny), applib.internal_date(colsmall), applib.internal_date(colint), applib.internal_date(colbig) 
from inttable;

select * from todate
order by 1;

-- in expressions
values 'Here''s the date:' || applib.internal_date(2207520000000);


-- cleanup
drop view fromint;
drop view todate;