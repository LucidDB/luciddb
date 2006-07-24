-- test varbinary type in calc conversions

set schema 's';

DROP TABLE foo;
CREATE TABLE foo (x int, y varbinary(6));
create table boo (x char(10))
;
insert into foo values (1, X'1bad3bad')
;
insert into foo values (1, X'1bad3aad')
;
insert into foo values (1, X'1bad3cad')
;
insert into foo values (2, X'1bad2bad3bad')
;
--bug FRG-147 values not truncated before inserting to table
--insert into foo values (3, X'1bad2bad3bad4bad')
--;
--end bug
-- FRG-148 (bad imput to varbinary passes parser)
insert into foo values(3, X'X');

select * from foo
;
-- not supported?
--insert into boo select y from foo
--;
--select * from boo
--;

-- test calc's compare operations, prevent range scan

select * from foo where y > X'1bad3bad' or x > 99999999
;
select * from foo where y >= X'1bad3bad' or x > 99999999
;
select * from foo where y < X'1bad3bad' or x > 99999999
;
select * from foo where y <= X'1bad3bad' or x > 99999999
;
select * from foo where y <> X'1bad3bad' or x > 99999999
;
select * from foo where y = X'1bad3bad' or x > 99999999
;

-- character/binary should be a type compatibility error
insert into boo select y from foo
;

DROP TABLE boo;
DROP TABLE foo;
