-- $Id$
-- Tests for DateBBInternal UDF
set schema 'udftest';
set path 'udftest';

-- define function
create function date_internal(indate bigint)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.DateBBInternal.FunctionExecute';

values date_internal(9223372036854775807);
values date_internal(0);
values date_internal(-63072000000);

-- these should fail
values date_internal(9223372036854775808);
values date_internal(4.55);
values date_internal(9223372036854775807.01);
values date_internal(9223372036854775807.99);

-- create view with reference to date_internal
create view fromint as
select fname, date_internal(age * 8640000) 
from customers;

select * from fromint
order by 1;

create view todate as
select date_internal(coltiny), date_internal(colsmall), date_internal(colint), date_internal(colbig) 
from inttable;

select * from todate
order by 1;

-- in expressions
values 'Here''s the date:' || date_internal(2207520000000);

