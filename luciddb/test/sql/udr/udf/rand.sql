-- $ ID: //open/lu/dev/luciddb/test/sql/udr/udf/rand.sql#2 $
-- Tests for rand UDF
-- commented out tests because function is nondeterministic
set schema 'udftest';
set path 'udftest';


--values applib.rand(1, 100);
--values applib.rand(0, 0);

-- failures
values applib.rand (9, 2);
values applib.rand(1.1, 3);

-- create view with reference to applib.rand
create view randt as
select colname, applib.rand(coltiny, coltiny+50) as rcoltiny, applib.rand(colsmall, colsmall+100) as rcolsmall
from inttable;

--select * from applib.randt
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
select colname, applib.rand(coltiny, coltiny+5) + applib.rand(colsmall, colsmall+10)
from inttable;

-- nested
values applib.rand(1, applib.rand(5, 10)) between 1 and 10;

-- cleanup
drop view v2;
drop view randt;
