-- test explicit conversions in the calculator
-- loading is the best way to excercise this code
-- Was calc11.sql

set schema 's';

create table calcstring (s1 char(30))
;
create table calcdec (num integer)
;
create table calcnum (num numeric(8,4))
;
create table calcfloat (f1 float)
;
create table calcdouble (f2 double)
;
create table calcdate (d1 date)
;
create table calctime (t1 time)
;
create table calcts (ts timestamp)
;
insert into calcstring select s1 from calctypes where n1 + 100 > 0
;
insert into calcstring select s2 from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (n1 as varchar(30)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (n2 as char(30)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (n3 as char(50)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (f1 as char(30)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (f2 as char(30)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (t1 as char(30)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (d1 as char(30)) from calctypes where n1 + 100 > 0
;
insert into calcstring select cast (ts1 as char(30)) from calctypes where n1 + 100 > 0
;
select * from calcstring order by 1
;
insert into calcdec select cast ('54' as integer) from calctypes where n1 + 100 > 0
;
insert into calcdec select n2 from calctypes where n1 + 100 > 0
;
insert into calcdec select n3 from calctypes where n1 + 100 > 0
;
insert into calcdec select f1 from calctypes where n1 + 100 > 0
;
insert into calcdec select f2 from calctypes where n1 + 100 > 0
;
select * from calcdec order by 1
;
insert into calcnum select n1 from calctypes where n1 + 100 > 0
;
insert into calcnum select f1 from calctypes where n1 + 100 > 0
;
insert into calcnum select f2 from calctypes where n1 + 100 > 0
;
insert into calcnum select cast ('543.123' as numeric(10,3)) from calctypes where n1 + 100 > 0
;
select * from calcnum order by 1
;
insert into calcfloat select n1 from calctypes where n1 + 100 > 0
;
insert into calcfloat select n2 from calctypes where n1 + 100 > 0
;
insert into calcfloat select n3 from calctypes where n1 + 100 > 0
;
insert into calcfloat select f2 from calctypes where n1 + 100 > 0
;
insert into calcfloat select cast ('1234.987654' as numeric(10,6)) from calctypes where n1 + 100 > 0
;
select * from calcfloat order by 1
;
insert into calcdouble select cast ('1234.0987654' as numeric(11,7)) from calctypes where n1 + 100 > 0
;
insert into calcdouble select n1 from calctypes where n1 + 100 > 0
;
insert into calcdouble select n2 from calctypes where n1 + 100 > 0
;
insert into calcdouble select n3 from calctypes where n1 + 100 > 0
;
insert into calcdouble select f1 from calctypes where n1 + 100 > 0
;
select * from calcdouble order by 1
;
insert into calcdate select cast (s2 as date) from calctypes where n1 + 100 > 0
;
-- FRG-20
insert into calcdate select cast (ts1 as date) from calctypes where n1 + 100 > 0
;
select * from calcdate order by 1
;
-- FRG-20
insert into calctime select cast (s1 as time) from calctypes where n1 + 100 > 0
;
-- FRG-20
insert into calctime select cast (ts1 as time) from calctypes where n1 + 100 > 0
;
select * from calctime order by 1
;
-- FRG-20
insert into calcts select cast (cast (s1 as time) as timestamp) from calctypes where n1 + 100 > 0
;
select * from calcts order by 1
;
