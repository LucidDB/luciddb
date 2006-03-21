-- test all types with implicit and explicit calc coercion

set schema 's';

create table calctypes
(
s1 char(30),
s2 varchar(30),
n1 integer,
n2 numeric(8,4),
n3 numeric(6,2),
f1 float,
f2 double,
t1 time,
d1 date,
ts1 timestamp
)
;

insert into calctypes values ('9:21:43', '1996-11-30', 10, 12.9876, 24.12, 6.5, 12.8, TIME '9:21:43', DATE '1996-11-30', TIMESTAMP '1996-11-30 9:21:43')
;

select n1 * f1, n1 / f1, n2 + f2, n2 - f2 from calctypes
;
-- TODO: UDF
-- select f1 + { fn convert(s1,SQL_NUMERIC) } from calctypes
-- ;

-- check out where clause
select d1 from calctypes where d1 < date '1996-02-14'
;
-- TODO: UDF
-- select d1 from calctypes where { fn convert(d1,SQL_TIMESTAMP) } > ts1
-- ;
select d1 from calctypes where t1 < time '9:21:55'
;
select d1 from calctypes where f1 != n2
;
select d1 from calctypes where f2 > f1
;
select d1 from calctypes where 10.523 > 10.5
;

-- Implicit int --> float conversion
drop table foo
;
create table foo (x real, y int)
;
insert into foo values (354.1234, 3)
;
-- FRG-49
select avg(b.x), sum(b.x) from foo a, foo b where a.y = b.y
;
drop table foo
;

-- date to timestamp implicit conversion
select d1 from calctypes where d1 < ts1
;

select d1 from calctypes where d1 < cast (ts1 as date)
;

-- errors
select d1 + 1, d1 - 10, t1 + 1000, t1 - 1000, ts1 + 1000, ts1 - 1000 from calctypes
;
select n1 * f1, n1 / f1, n2 + f2, n2 - f2, n1 + '10', n2 + '10.555' from calctypes
;
select f1 + s1 from calctypes
;
select d1 from calctypes where f1 > '4.2345'
;
select d1 from calctypes where f1 < '10.234552'
;
select ts1 * 2 from calctypes
;
select ts1 / 2 from calctypes
;
select d1 * 2 from calctypes
;
select d1 / 2 from calctypes
;
select t1 * 2 from calctypes
;
select t1 / 2 from calctypes
;
select d1 + t1 from calctypes
;
select d1 - t1 from calctypes
;
select f1 + d1 from calctypes
;
select f1 - d1 from calctypes
;
select * from calctypes where t1 > d1
;
select * from calctypes where f1 > ts1
;
