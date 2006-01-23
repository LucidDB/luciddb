set schema 's'
;

-- Test will load all datatypes into a tinyint column.

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- tinyint to tinyint; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;


drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- smallint to tinyint; min/max range for target datatype

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- smallint to tinyint; min/max range for source datatype

insert into datatype_target
 select colsmall from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- integer to tinyint; min/max range for target datatype

insert into datatype_target
 select colint from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- integer to tinyint; min/max range for source datatype

insert into datatype_target
 select colint from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- bigint to tinyint; min/max range for target datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- bigint to tinyint; min/max range for source datatype

insert into datatype_target
 select colbig from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- decimal to tinyint; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- decimal to tinyint; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- numeric to tinyint; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- numeric to tinyint; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- double to tinyint; min/max range for target datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- double to tinyint; min/max range for source datatype

insert into datatype_target
 select coldouble from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- float to tinyint; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- float to tinyint; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- real to tinyint; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='tinyint'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- real to tinyint; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='tinyint'
    and range_for='source'
;
select * from datatype_target order by 1
;


drop table datatype_target
;
create table datatype_target(col tinyint)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target order by 1
;
-- PASS: if value = 123
