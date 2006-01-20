set schema 's'
;

-- Test will load all datatypes into a tinyint column.

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- tinyint to tinyint; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;


drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- smallint to tinyint; min/max range for target datatype

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- smallint to tinyint; min/max range for source datatype

insert into datatype_target
 select colsmall from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- integer to tinyint; min/max range for target datatype

insert into datatype_target
 select colint from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- integer to tinyint; min/max range for source datatype

insert into datatype_target
 select colint from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- bigint to tinyint; min/max range for target datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- bigint to tinyint; min/max range for source datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- decimal to tinyint; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- decimal to tinyint; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- numeric to tinyint; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- numeric to tinyint; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- double to tinyint; min/max range for target datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- double to tinyint; min/max range for source datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- float to tinyint; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- float to tinyint; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- real to tinyint; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- real to tinyint; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target
;


drop table datatype_target
;
create table datatype_target(col tinyint primary key)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target
;
-- PASS: if value = 123
