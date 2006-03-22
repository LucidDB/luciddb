-- test for precision (per Bill's request)
-- Was calc9.sql

-- Check precision on computed results
values (1.23 + 1.22222, 1.23 - 1.22222);
values (1000.21 * 1000000);
values (543.1234567 + .1 + .1 + .1 + .1);
values (100.2468 / 50.1234);

-- Check really big precision numeric types
set schema 's';

DROP TABLE foo
;
create table foo (x decimal(19,4))
;
insert into foo values (123456789012345)
;
insert into foo values (123456789012345.1234)
;
-- TODO: update once FRG-40 is fixed
insert into foo values (1234567890123456)
;
insert into foo values (123456789012345.12345)
;
SELECT * FROM FOO order by 1;
SELECT x * 10 FROM FOO order by 1;
SELECT x * 100 FROM FOO order by 1;
SELECT x * 1000 FROM FOO order by 1;
SELECT x * 10000 FROM FOO order by 1;
SELECT x * 100000 FROM FOO order by 1;
DROP TABLE FOO;

--  Check insertion into a decimal column of a really big
-- decimal
-- FRG-44
CREATE TABLE FOO (x DECIMAL);
INSERT INTO FOO VALUES (0.1234567890123456789);
SELECT * FROM FOO order by 1;
SELECT x * 10 FROM FOO order by 1;
SELECT x * 100 FROM FOO order by 1;
SELECT x / 10 FROM FOO order by 1;
SELECT X / 100 FROM FOO order by 1;
DROP TABLE FOO;


create table foo (x numeric(7,0), y numeric(9,0))
;
insert into foo values (1234567, 123456789)
;
insert into foo values (7654321, 987654321)
;
select x * 1000000 from foo order by 1
;
-- LDB-21 overflow
values (1234567 * 1000000)
;
values (123456789 + 1)
;
select y + 1 from foo order by 1
;
drop table foo
;

-- Check what happens when we multiply two numbers and the sum
-- of scales of operands is greater than then maximum allowable
-- scale

values( cast (.2 as decimal (11,9)) * cast (.3 as decimal (12,10)))
;
