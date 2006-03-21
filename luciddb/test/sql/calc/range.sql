-- test more bugs  (range of numeric types)
-- Was calc13.sql

set schema 's';

create table numbers (a tinyint, b smallint, c numeric (7,3))
;
insert into numbers values (120, 32000, 1234.123)
;
insert into numbers values (-120, -32000, -1234.123)
;
insert into numbers values (130, 66000, 12345.123)
;
insert into numbers values (-130, -66000, -12345.123)
;
select * from numbers order by 1
;
