create schema gcrc;
set schema 'gcrc';


create table types (
  c_smallint smallint,
  c_tinyint tinyint,
  c_int integer, 
  c_varchar varchar(10), 
  c_bigint bigint, 
  c_char char(10), 
  c_decimal decimal(10,4), 
  c_date date, 
  c_time time, 
  c_timestamp timestamp,
  c_boolean boolean
);

create table types_subset (
  c_smallint smallint,
  c_tinyint tinyint,
  c_varchar varchar(10), 
  c_decimal decimal(10,4),
  c_timestamp timestamp,
  c_boolean boolean
);

-- setup table
insert into types values
(32767,127,2147483647,'abcdefghij',9223372036854775807,'abcdefghij',999999.9999,date'2006-12-19', time'4:29:29',timestamp'2020-2-2 2:2:2',true),
(null,null,null,null,null,null,null,null,null,null,null),
(-32767,-127,-2147483647,'abcdefghij',-9223372036854775807,'abcdefghij',-999999.9999,date'2006-12-19', time'4:29:29',timestamp'2020-2-2 2:2:2',true),
(1,null,56,'lola',null,null,null,date'1979-2-20', time'8:17:3',null,false),
(null,1,null,null,56,'lola',null,date'1979-2-20', time'8:17:3',null,false),
(null,1,null,null,56,'lola',null,null,null,timestamp'1979-2-20 8:17:3',false);

insert into types_subset values
(32767,127,'abcdefghij',999999.9999,timestamp'2020-2-2 2:2:2',true),
(null,null,null,null,null,null),
(-32767,-127,'abcdefghij',-999999.9999,timestamp'2020-2-2 2:2:2',true),
(1,null,'lola',null,null,false),
(null,1,null,null,null,false),
(null,1,null,null,timestamp'1979-2-20 8:17:3',false);

-- check crc_values
select * 
from table(applib.generate_crc(
  cursor(select * from types)))
order by 1,2,3,4,5,6,7,8,9,10,11,12;

-- should get the same crc values for both queries below
select * 
from table(applib.generate_crc(
  cursor(select * from types_subset)))
order by 1,2,3,4,5,6;

select *
from table(applib.generate_crc(
  cursor(select * from types),
  row(c_smallint, c_tinyint, c_varchar, c_decimal, c_timestamp, c_boolean),
  false))
order by 1,2,4,7,10,11;

create table temptypes(c_decimal decimal(10,5),c_tinyint tinyint,c_smallint smallint,c_int integer,c_bigint bigint,c_char char(10),c_varchar varchar(10),c_varchar2 varchar(2),c_int2 char(3),c_timestamp timestamp,c_boolean boolean);

-- crc value not different for different column types, same data
insert into temptypes values
(null,1,null,null,56,'lola',null,null,null,timestamp'1979-2-20 8:17:3',false);

select * 
from table(applib.generate_crc(
  cursor(select * from temptypes)))
order by 1,2,3,4,5,6,7,8,9,10,11,12;

-- on joined table
select *
from table(applib.generate_crc(
  cursor(select * from types, temptypes 
    where types.c_tinyint = temptypes.c_tinyint)))
order by crc_value,1,2,3,4,5,6,7,8,9,10,11;

-- join with filter
select * 
from table(applib.generate_crc(
  cursor(select * from types,temptypes where types.c_decimal > 0)))
order by crc_value,1,2,3,4,5,6,7,8,9,10,11;

-- on view
create view typeview as
select c_decimal, c_tinyint, c_smallint, c_int, c_bigint, c_char, c_varchar, c_timestamp, c_boolean from types;

select * 
from table(applib.generate_crc(
  cursor(select * from typeview)))
order by crc_value,1,2,3,4,5,6,7,8,9;

-- functions
select * 
from table(applib.generate_crc(
  cursor(select c_smallint/2, coalesce(c_varchar, 'null') from types)))
order by crc_value,1,2;
 
-- recursive
select *
from table(applib.generate_crc(
  cursor(select * from table(applib.generate_crc(
    cursor(select * from types))))))
order by crc_value,1,2,3,4,5,6,7,8,9,10,11,12;

-- merge 
create table types2 (
  c_smallint smallint,
  c_tinyint tinyint,
  c_int integer, 
  c_varchar varchar(10), 
  c_bigint bigint, 
  c_char char(10), 
  c_decimal decimal(10,4), 
  c_date date, 
  c_time time, 
  c_timestamp timestamp,
  c_boolean boolean,
  crc bigint
);

delete from types_subset where c_tinyint is null;
insert into types2(crc) values (3509424027), (2591132920);
select crc_value from table(applib.generate_crc(
  cursor(select * from types_subset)))
order by c_smallint, c_tinyint, c_varchar, c_decimal, c_timestamp, c_boolean;

-- this should populate types2 with all the same rows as types_subset
merge into types2 as tgt
  using table(applib.generate_crc(cursor(select * from types_subset))) as src
  on tgt.crc = src.crc_value
  when matched then
    update set c_smallint = src.c_smallint, c_tinyint = src.c_tinyint,
      c_varchar = src.c_varchar, c_decimal = src.c_decimal,
      c_timestamp = src.c_timestamp, c_boolean = src.c_boolean
  when not matched then
    insert (c_smallint, c_tinyint, c_varchar, c_decimal, c_timestamp,
      c_boolean, crc)
      values (src.c_smallint, src.c_tinyint, src.c_varchar, src.c_decimal,
      src.c_timestamp, src.c_boolean, src.crc_value);

select * from types2 order by 1,2,3,4,5,6,7,8,9,10,11;

-- now update types2 so that it has all the fields in types
-- and new rows have crc value of null
merge into types2 as tgt 
  using table(applib.generate_crc(
    cursor(select * from types),
    row(c_smallint, c_tinyint, c_varchar, c_decimal, c_timestamp, c_boolean),
    false)) as src
  on tgt.crc = src.crc_value
  when matched then 
    update set c_int = src.c_int, c_bigint = src.c_bigint, c_char = src.c_char,
      c_date = src.c_date, c_time = src.c_time
  when not matched then
    insert values(src.c_smallint, src.c_tinyint, src.c_int, src.c_varchar,
      src.c_bigint, src.c_char, src.c_decimal, src.c_date, src.c_time,
      src.c_timestamp, src.c_boolean, null);

-- crc should be the same output as using types table since we're excluding
-- the crc row
select * from table(applib.generate_crc(
  cursor(select * from types2), 
  row(crc),
  true))
order by 1,2,3,4,5,6,7,8,9,10,11,12;


-- what happens if no rows selected?
select * from table(applib.generate_crc(
  cursor(select * from types_subset),
  row(c_smallint, c_tinyint, c_varchar, c_decimal, c_timestamp, c_boolean),
  true))
order by 1,2,3,4,5,6,7;

-- cleanup
drop view typeview cascade;
drop table types cascade;
drop schema gcrc cascade;
