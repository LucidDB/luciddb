-- test binary type in calc conversions

set schema 's';

DROP TABLE foo;
CREATE TABLE foo (x int, y binary(6));
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
insert into foo values (3, X'1bad2bad3bad4bad')
;
select * from foo
;
insert into boo select y from foo
;
select * from boo
;
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
-- character/binary should be a type compatibility error
select * from foo where y = '1bad3bad' or x > 99999999;
DROP TABLE boo;
DROP TABLE foo;
