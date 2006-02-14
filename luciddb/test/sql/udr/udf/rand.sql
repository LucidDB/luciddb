-- $ ID: //open/lu/dev/luciddb/test/sql/udr/udf/rand.sql#1 $
-- Tests for rand UDF
-- commented out tests because function is nondeterministic
set schema 'udftest';
set path 'udftest';

-- define functions
create function rand(minVal integer, maxVal integer)
returns integer
language java
not deterministic
no sql
external name 'class com.lucidera.luciddb.applib.rand.FunctionExecute';

--values rand(1, 100);
--values rand(0, 0);

-- failures
values rand (9, 2);
values rand(1.1, 3);

-- create view with reference to rand
create view randt as
select colname, rand(coltiny, coltiny+50) as rcoltiny, rand(colsmall, colsmall+100) as rcolsmall
from inttable;

--select * from randt
--order by 1;

select randt.colname, rcoltiny between coltiny and coltiny+50
from randt, inttable
where randt.colname = inttable.colname
order by 1;

select randt.colname, rcolsmall between colsmall and colsmall+100 
from randt, inttable
where randt.colname = inttable.colname
order by 1;
-- in expressions

create view v2 as
select colname, rand(coltiny, coltiny+5) + rand(colsmall, colsmall+10)
from inttable;

-- nested
values rand(1, rand(5, 10)) between 1 and 10;

-- cleanup
drop view v2;
drop view randt;
drop routine rand;