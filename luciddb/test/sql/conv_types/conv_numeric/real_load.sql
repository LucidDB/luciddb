set schema 's'
;

-- Test will load all datatypes into a real column.

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- tinyint to real; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- smallint to real; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- integer to real; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- bigint to real; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- decimal to real; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- decimal to real; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- numeric to real; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- numeric to real; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- double to real; min/max range for source datatype [same as target]

insert into datatype_target
 select coldouble from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- float to real; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- float to real; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- real to real; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- real to real; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col real primary key)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target
;
-- PASS: if value = 123.4567
