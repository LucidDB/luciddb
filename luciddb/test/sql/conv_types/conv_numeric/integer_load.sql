set schema 's'
;

-- Test will load all datatypes into a integer column.

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- tinyint to integer; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- smallint to integer; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- integer to integer; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- bigint to integer; min/max range for target datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- bigint to integer; min/max range for source datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- decimal to integer; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- decimal to integer; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- numeric to integer; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- numeric to integer; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;


drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- double to integer; min/max range for target datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- double to integer; min/max range for source datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- float to integer; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- float to integer; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- real to integer; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='integer'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- real to integer; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='integer'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col integer primary key)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target
;
-- PASS: if value = 123
