-- tests from previously reported bugs

--{{{ Bug 2108 ("= ALL" operator did not work with character fields)

CREATE SCHEMA BUG2108
;

create table BUG2108.TEST(a char(3))
;

create table BUG2108.TEST2(a char(3))
;

insert into BUG2108.TEST values ('1')
;

insert into BUG2108.TEST values ('2')
;

insert into BUG2108.TEST values ('3')
;

insert into BUG2108.TEST2 values ('1')
;

-- this used to fail with internal error in calcconvert.cpp
-- select count(*) from BUG2108.TEST
-- where a = all (select a from BUG2108.TEST2)
-- ;

select count(*) from BUG2108.TEST
where a in (select a from BUG2108.TEST2)
;

--}}}

-- Was calc12.sql

set schema 's';

create table calcsmallstring (s1 char(8))
;
insert into calcsmallstring values ('abcdefghijklm')
;
insert into calcsmallstring values ('abcd')
;

-- test SQL 92 rule about padding for comparisons to CHAR strings
-- also prevent this from doing a range scan
select * from calcsmallstring where 'abcd' = s1 or s1 || ' ' = 'dummy'
;

select * from calcsmallstring order by 1
;

select ltrim(s1, 'a') from calcsmallstring
;
select ltrim(s1, 'c') from calcsmallstring
;
select rtrim(s1, ' ') from calcsmallstring
;
select rtrim(s1) from calcsmallstring
;
select substring(s1, 2, 3) from calcsmallstring order by 1
;
select substring(s1, -5, 3) from calcsmallstring
;
select substring(s1, -2, 1) from calcsmallstring
;
select num, case num
when 7 then 1
when 24 then NULL
when 54 then 2
else 666 end
from calcdec order by 1
;
select num, case num
when 7 then NULL
when 24 then NULL
when 54 then 2
else 666 end
from calcdec order by 1
;
select num, case num
when 7 then 1
when 24 then NULL
when 54 then 2
else NULL end
from calcdec order by 1
;
select num, case num
when 7 then NULL
when 24 then NULL
when 54 then NULL
else 666 end
from calcdec order by 1
;



drop table moo
;
create table moo (a integer, b integer)
;

insert into moo values (1, 2)
;



-- test small division bug
select sum(46) / sum(90), sum(44) / sum(90), 46 / 90, 44 / 90
from moo order by 1
;


-- test negation with int64 (bug 1897)
select -(a*b/a) from moo
;
