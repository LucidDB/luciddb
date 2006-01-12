set schema 's'
;

-- Test will load all datatypes into a float column.

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- tinyint to float; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- smallint to float; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- integer to float; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- bigint to float; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- decimal to float; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- decimal to float; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- numeric to float; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- numeric to float; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- double to float; min/max range for source datatype [same as target]

insert into datatype_target
 select coldouble from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- float to float; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- float to float; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- real to float; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- real to float; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target
;

drop table datatype_target
;
create table datatype_target(col float primary key)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target
;
-- PASS: if value = 123.456789
