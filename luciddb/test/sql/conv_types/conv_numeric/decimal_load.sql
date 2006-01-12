set schema 's'
;

-- Test will load all datatypes into a decimal column.

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- tinyint to decimal; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- smallint to decimal; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- integer to decimal; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- bigint to decimal; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- decimal to decimal; min/max range for source datatype [same as target]

insert into datatype_target
 select coldec from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- numeric to decimal; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='decimal'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- numeric to decimal; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- double to decimal; min/max range for target datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='decimal'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- double to decimal; min/max range for source datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- float to decimal; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='decimal'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- float to decimal; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- real to decimal; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='decimal'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- real to decimal; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='decimal'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col decimal(15,4) primary key)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target
;
-- PASS: if value = 123.4567
