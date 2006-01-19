set schema 's'
;

-- Test will load all datatypes into a float column.

drop table datatype_target
;
create table datatype_target(col float)
;

-- tinyint to float; min/max range for source datatype [same as target]

insert into datatype_target
 select coltiny from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- smallint to float; min/max range for source datatype [same as target]

insert into datatype_target 
 select colsmall from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- integer to float; min/max range for source datatype [same as target]

insert into datatype_target
 select colint from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- bigint to float; min/max range for source datatype [same as target]

insert into datatype_target
 select colbig from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- decimal to float; min/max range for target datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- decimal to float; min/max range for source datatype

insert into datatype_target
 select coldec from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- numeric to float; min/max range for target datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- numeric to float; min/max range for source datatype

insert into datatype_target
 select colnum from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- double to float; min/max range for source datatype [same as target]

insert into datatype_target
 select coldouble from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- float to float; min/max range for target datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- float to float; min/max range for source datatype

insert into datatype_target
 select colfloat from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- real to float; min/max range for target datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='float'
    and range_for='target'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- real to float; min/max range for source datatype

insert into datatype_target
 select colreal from datatype_source
  where target_type='float'
    and range_for='source'
;
select * from datatype_target order by 1
;

drop table datatype_target
;
create table datatype_target(col float)
;

-- test to drop scale

insert into datatype_target values(123.456789)
;
select * from datatype_target order by 1
;
-- PASS: if value = 123.456789
