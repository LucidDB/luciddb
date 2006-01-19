set schema 's'
;

-- Test will load all datatypes into a real column.

drop table datatype_target
;
create table datatype_target(col real)
;

-- tinyint to real; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- smallint to real; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- integer to real; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- bigint to real; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- decimal to real; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- decimal to real; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- numeric to real; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- numeric to real; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- double to real; min/max range for source datatype [same as target]

insert into datatype_target
 select coldouble from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- float to real; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- float to real; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- real to real; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='real'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- real to real; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='real'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col real)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target order by 1
;
-- PASS: if value = 123.4567
