create or replace schema udftest;
set schema 'udftest';
set path 'udftest';

-- The identity UDX copies its input to its output.
-- Its name is "self" because "identity" is a SQL reserved word.
create function self(c cursor)
returns table(c.*)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.self';

explain plan for
select * from table(self(cursor(select * from sales.depts)));

select * from table(self(cursor(select * from sales.depts)));
