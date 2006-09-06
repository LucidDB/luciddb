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

-- setup table
insert into types values
(32767,127,2147483647,'abcdefghij',9223372036854775807,'abcdefghij',999999.9999,date'2006-12-19', time'4:29:29',timestamp'2020-2-2 2:2:2',true),
(null,null,null,null,null,null,null,null,null,null,null),
(-32767,-127,-2147483647,'abcdefghij',-9223372036854775807,'abcdefghij',-999999.9999,date'2006-12-19', time'4:29:29',timestamp'2020-2-2 2:2:2',true),
(1,null,56,'lola',null,null,null,date'1979-2-20', time'8:17:3',null,false),
(null,1,null,null,56,'lola',null,date'1979-2-20', time'8:17:3',null,false),
(null,1,null,null,56,'lola',null,null,null,timestamp'1979-2-20 8:17:3',false);

-- check crc_values
select * 
from table(applib.generate_crc(
  cursor(select * from types)))
order by crc_value;

create table temptypes(c_decimal decimal(10,5),c_tinyint tinyint,c_smallint smallint,c_int integer,c_bigint bigint,c_char char(10),c_varchar varchar(10),c_varchar2 varchar(2),c_int2 char(3),c_timestamp timestamp,c_boolean boolean);

-- crc value not different for different column types, same data
insert into temptypes values
(null,1,null,null,56,'lola',null,null,null,timestamp'1979-2-20 8:17:3',false);

select * 
from table(applib.generate_crc(
  cursor(select * from temptypes)))
order by crc_value;

-- on joined table
select *
from table(applib.generate_crc(
  cursor(select * from types, temptypes 
    where type.c_tinyint = temptypes.c_tinyint)))
order by *;

-- join with filter
select * 
from table(applib.generate_crc(
  cursor(select * from types,temptypes where types.c_decimal > 0)))
order by *;

-- on view
create view typeview as
select c_decimal, c_tinyint, c_smallint, c_int, c_bigint, c_char, c_varchar, c_timestamp, c_boolean from types;

select * 
from table(applib.generate_crc(
  cursor(select * from typeview)))
order by *;

-- functions
select * 
from table(applib.generate_crc(
  cursor(select c_smallint/2, coalesce(c_varchar, 'null') from types)))
order by *;
 
-- recursive
select *
from table(applib.generate_crc(
  cursor(select * from table(applib.generate_crc(
    cursor(select * from types))))))
order by *;

-- cleanup
drop view typeview cascade;
drop table types cascade;
drop schema gcrc cascade;
