set schema 's'
;

-- Test will load all datatypes into a integer column.

drop table datatype_target
;
create table datatype_target(col integer)
;

-- tinyint to integer; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- smallint to integer; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- integer to integer; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- bigint to integer; min/max range for target datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- bigint to integer; min/max range for source datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- decimal to integer; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- decimal to integer; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- numeric to integer; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- numeric to integer; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;


drop table datatype_target
;
create table datatype_target(col integer)
;

-- double to integer; min/max range for target datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- double to integer; min/max range for source datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- float to integer; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- float to integer; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- real to integer; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- real to integer; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col integer)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target order by 1
;
-- PASS: if value = 123
