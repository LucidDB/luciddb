set schema 's'
;

-- Test will load all datatypes into a smallint column.

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- tinyint to smallint; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- smallint to smallint; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- integer to smallint; min/max range for target datatype

insert into datatype_target
 select colint from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- integer to smallint; min/max range for source datatype

insert into datatype_target
 select colint from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- bigint to smallint; min/max range for target datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- bigint to smallint; min/max range for source datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- decimal to smallint; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- decimal to smallint; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- numeric to smallint; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- numeric to smallint; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- double to smallint; min/max range for target datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- double to smallint; min/max range for source datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- float to smallint; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- float to smallint; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- real to smallint; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='smallint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- real to smallint; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='smallint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col smallint)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target order by 1
;
-- PASS: if value = 123
